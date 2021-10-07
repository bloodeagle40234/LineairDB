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

#include "epoch_based_rcu_range_index.h"

#include <algorithm>
#include <atomic>
#include <cassert>
#include <functional>
#include <string_view>
#include <vector>

#include "types/data_item.hpp"
#include "types/definitions.h"

namespace LineairDB {
namespace Index {

EpochBasedRCURangeIndex::EpochBasedRCURangeIndex(LineairDB::EpochFramework& e)
    : RangeIndexBase(e),
      manager_state_(ManagerThreadState::Wait),
      atomic_triple_(nullptr),
      indexed_epoch_(0),
      manager_([&]() { EpochManagerJob(); }) {
  assert(manager_state_.is_always_lock_free());

  // TODO initialize atomic_triple_;

  manager_state_.store(ManagerThreadState::Running);
};
EpochBasedRCURangeIndex::~EpochBasedRCURangeIndex() {
  manager_state_.store(ManagerThreadState::IsRequestedToExit);
  manager_.join();
};

std::optional<size_t> EpochBasedRCURangeIndex::Scan(
    const std::string_view b, const std::string_view e,
    std::function<bool(std::string_view)> operation) {
  size_t hit       = 0;
  const auto begin = std::string(b);
  const auto end   = std::string(e);
  if (end < begin) return std::nullopt;

  auto* triple       = atomic_triple_.load();
  const auto& events = *triple->insert_or_delete_events;

  if (IsOverlapWithInsertOrDelete(b, e, events)) { return std::nullopt; }

  auto& container  = *triple->container;
  auto& predicates = *triple->predicates;

  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();

  // TODO create new record and CAS
  // 1. auto* new_predicates = new PredicateList(predicates); // copy
  // 2. new_predicates->emplace(this_predicate);
  // 3. atomic_triple_.cas(triple, {new_predicates, fetched, fetched})
  // 4. if the operation failed,
  //    4a: return std::nullopt, あきらめる
  //    4b: retry. goto beginning of this function.
  //        is it correct?

  // container:  {a, c, d}
  predicates.emplace_back(Predicate{begin, end, global_epoch});

  // container2: {a, b, c, d} has different address

  {
    auto it     = container.lower_bound(begin);
    auto it_end = container.upper_bound(end);
    for (; it != it_end; it++) {
      if (it->second.is_deleted) continue;
      hit++;
      auto cancel = operation(it->first);
      if (cancel) break;
    }
  }

  return hit;
};

bool EpochBasedRCURangeIndex::Insert(const std::string_view key) {
  return InsertOrDelete(key, true);
}

bool EpochBasedRCURangeIndex::Delete(const std::string_view key) {
  return InsertOrDelete(key, true);
};

bool EpochBasedRCURangeIndex::InsertOrDelete(const std::string_view key,
                                             bool is_insert) {
  auto* triple           = atomic_triple_.load();
  const auto& predicates = triple->predicates;

  if (IsInPredicateSet(key, predicates)) { return false; }
  // NOTE:
  // The global epoch read here may be larger than the epoch of the transaction
  // which invokes this method.
  // It may cause unnecessary aborts(there are false positives),
  // but it won't miss any phantom anomaly (there are no false negatives).
  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();

  const auto new_event =
      InsertOrDeleteEvent{std::string(key), is_insert, global_epoch};

  // TODO create new record and CAS
  // 1. auto* new_inserts = new ...(fetched); // copy
  // 2. new_inserts->emplace(key);
  // 3. atomic_triple_.cas(triple, {new, fetched, fetched})
  // 4. if the operation failed,
  //    4a: return std::nullopt, あきらめる
  //    4b: retry. goto beginning of this function.
  //        is it correct?

  return true;
};

bool EpochBasedRCURangeIndex::IsInPredicateSet(
    const std::string_view key, const PredicateList& predicates) const {
  for (auto it = predicates.begin(); it != predicates.end(); it++) {
    if (it->begin <= key && key <= it->end) return true;
  }
  return false;
}

bool EpochBasedRCURangeIndex::IsOverlapWithInsertOrDelete(
    const std::string_view begin, const std::string_view end,
    const InsertOrDeleteKeySet& insert_or_delete_key_set) const {
  for (auto it = insert_or_delete_key_set.begin();
       it != insert_or_delete_key_set.end(); it++) {
    if (begin <= it->key && it->key <= end) return true;
  }
  return false;
}

void EpochBasedRCURangeIndex::EpochManagerJob() {
  while (manager_state_.load() == ManagerThreadState::Wait) {
    std::this_thread::yield();
  }
  assert(manager_state_.load() == ManagerThreadState::Running);

  while (manager_state_.load() == ManagerThreadState::Running) {
    const auto global = epoch_manager_ref_.GetGlobalEpoch();
    if (indexed_epoch_ == global) {
      std::this_thread::yield();
      continue;
    }
    indexed_epoch_ = global;

    { /**
       * Begin Atomic Section
       */
      auto* triple = atomic_triple_.load();

      auto* new_predicates = new PredicateList(*triple->predicates);
      auto* new_events =
          new InsertOrDeleteKeySet(*triple->insert_or_delete_events);
      auto* new_container = new ROWEXRangeIndexContainer(*triple->container);
      auto* new_atomic_triple =
          new AtomicTriple{new_predicates, new_events, new_container};

      {
        // Clear predicate list
        auto it = remove_if(new_predicates->begin(), new_predicates->end(),
                            [&](const auto& pred) {
                              const auto del = 2 <= global - pred.epoch;
                              return del;
                            });

        // TODO allocate new predicates
        new_predicates->erase(it, new_predicates->end());
      }
      {
        // Clear insert_or_delete_keys
        auto it = remove_if(new_events->begin(), new_events->end(),
                            [&](const auto& pred) {
                              const auto del = 2 <= global - pred.epoch;
                              return del;
                            });

        // Before deleting the set of insert_or_delete_keys, we update the
        // index container to contain such outdated (already committed)
        // insertions and deletions.
        auto outdated_start = it;

        for (; it != new_events->end(); it++) {
          if (it->is_delete_event) {
            assert(0 < new_container.count(it->key));
            new_container->at(it->key).is_deleted = true;
          } else {  // insert
            if (0 < new_container->count(it->key)) {
              auto& entry      = new_container->at(it->key);
              entry.is_deleted = false;
            }
            new_container->emplace(it->key, IndexItem{false, it->epoch});
          }
        }

        new_events->erase(outdated_start, new_events->end());
      }

      // TODO: memory reclamation
    }
  }
}

}  // namespace Index
}  // namespace LineairDB
