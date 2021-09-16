/*
 *   Copyright (C) 2020 Nippon Telegraph and Telephone Corporation.

 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <rapidjson/document.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <cxxopts.hpp>
#include <experimental/filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <random>
#include <thread>
#include <variant>

#include "index/concurrent_table.h"
#include "lineairdb/config.h"
#include "spdlog/spdlog.h"
#include "util/epoch_framework.hpp"

const std::string CHARACTERS =
    "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";

template <typename T>
size_t benchmark(T& index, LineairDB::EpochFramework& epoch_f, size_t threads,
                 size_t proportion, size_t duration) {
  std::atomic<bool> end_flag(false);
  std::atomic<size_t> total_succeed(0);
  std::vector<std::future<void>> futures;
  for (size_t i = 0; i < threads; i++) {
    futures.push_back(std::async(std::launch::async, [&]() {
      size_t operation_succeed = 0;

      std::random_device seed_gen;
      std::mt19937 engine(seed_gen());
      std::uniform_int_distribution<> dist(0, 99);
      std::uniform_int_distribution<> random_string(0, CHARACTERS.size() - 1);

      for (;;) {
        if (end_flag.load()) {
          total_succeed.fetch_add(operation_succeed);
          break;
        };
        epoch_f.MakeMeOnline();

        const bool is_scan_operation =
            static_cast<size_t>(dist(engine)) < proportion;
        if (is_scan_operation) {
          std::string begin;
          std::string end;

          for (;;) {
            for (auto i = 0; i < 5; i++) {
              begin += CHARACTERS[random_string(engine)];
              end += CHARACTERS[random_string(engine)];
            }
            if (begin < end) break;
            begin.clear();
            end.clear();
          }

          auto result = index.Scan(begin, end, [](auto) { return false; });
          if (result.has_value()) operation_succeed++;

        } else {
          std::string key;
          for (auto i = 0; i < 5; i++) {
            key += CHARACTERS[random_string(engine)];
          }
          index.GetOrInsert(key);
          operation_succeed++;
        }
        epoch_f.MakeMeOffline();
      }
    }));
  }
  std::this_thread::sleep_for(std::chrono::milliseconds(duration));
  end_flag.store(true);
  for (auto& fut : futures) { fut.wait(); }
  return total_succeed.load();
}

int main(int argc, char** argv) {
  cxxopts::Options options("indexbench",
                           "Microbenchmark of various index structures");

  options.add_options()          //
      ("h,help", "Print usage")  //
      ("t,thread", "The number of worker threads",
       cxxopts::value<size_t>()->default_value(
           std::to_string(std::thread::hardware_concurrency())))  //
      ("s,structure", "Index data structure",
       cxxopts::value<std::string>()->default_value("LockBasedIndex"))  //
      ("p,proportion", "Proportion of 'scan' operation",
       cxxopts::value<size_t>()->default_value("10"))  //
      ("d,duration", "Measurement duration of this benchmark (milliseconds)",
       cxxopts::value<size_t>()->default_value("2000"))  //
      ("o,output", "Output JSON filename",
       cxxopts::value<std::string>()->default_value(
           "indexbench_result.json"))  //
      ;

  auto result = options.parse(argc, argv);
  if (result.count("help")) {
    std::cout << options.help() << std::endl;
    exit(0);
  }

  const uint64_t threads          = result["thread"].as<size_t>();
  const auto measurement_duration = result["duration"].as<size_t>();
  const auto proportion           = result["proportion"].as<size_t>();
  const auto structure            = result["structure"].as<std::string>();

  /** run benchmark **/
  auto ops = 0;

  {
    using namespace LineairDB::Index;

    LineairDB::EpochFramework epoch_framework;
    epoch_framework.Start();
    LineairDB::Config config;
    if (structure == "LockBasedIndex") {
      config.range_index = decltype(config)::RangeIndex::EpochROWEX;
    } else {
      std::cout << "invalid structure name." << std::endl
                << options.help() << std::endl;
      return EXIT_FAILURE;
    }

    ConcurrentTable index(epoch_framework, config);
    ops = benchmark<decltype(index)>(index, epoch_framework, threads,
                                     proportion, measurement_duration);
  }
  SPDLOG_INFO("Lockbench: measurement has finisihed.");
  SPDLOG_INFO(
      "structure: {0} Operations per "
      "seconds (ops): {1}",
      structure, ops);

  /** Output result as json format **/
  rapidjson::Document result_json(rapidjson::kObjectType);
  auto& allocator = result_json.GetAllocator();
  result_json.AddMember(
      "structure", rapidjson::Value(structure.c_str(), allocator), allocator);
  result_json.AddMember("threads", threads, allocator);
  result_json.AddMember("ops", ops, allocator);

  rapidjson::StringBuffer buffer;
  rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
  result_json.Accept(writer);
  writer.Flush();

  auto result_string   = buffer.GetString();
  auto output_filename = result["output"].as<std::string>();
  std::ofstream output_f(output_filename,
                         std::ofstream::out | std::ofstream::trunc);
  output_f << result_string;
  if (!output_f.good()) {
    std::cerr << "Unable to write output file" << output_filename << std::endl;
    exit(1);
  }
  std::cout << "This benchmark result is saved into " << output_filename
            << std::endl;
  return 0;
}