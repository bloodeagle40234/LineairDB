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
#include "util/logger.hpp"

namespace LineairDB {
namespace Index {

EpochBasedRCURangeIndex::EpochBasedRCURangeIndex(LineairDB::EpochFramework& e)
    : RangeIndexBase(e),
      atomic_triple_(new AtomicTriple()),
      indexed_epoch_(0),
      manager_state_(ManagerThreadState::Wait),
      manager_([&]() { EpochManagerJob(); }) {
  assert(manager_state_.is_always_lock_free);
  manager_state_.store(ManagerThreadState::Running);
  Util::SetUpSPDLog();  // TODO REMOVE
};

EpochBasedRCURangeIndex::~EpochBasedRCURangeIndex() {
  manager_state_.store(ManagerThreadState::IsRequestedToExit);
  manager_.join();
  delete atomic_triple_;
};

std::optional<size_t> EpochBasedRCURangeIndex::Scan(
    const std::string_view b, const std::string_view e,
    std::function<bool(std::string_view)> operation) {
  size_t hit       = 0;
  const auto begin = std::string(b);
  const auto end   = std::string(e);
  if (end < begin) return std::nullopt;

  auto* triple            = atomic_triple_.load();
  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();

  for (;;) {
    // load
    triple = atomic_triple_.load();

    const auto* predicates = triple->predicates;
    const auto* events     = triple->insert_or_delete_events;
    const auto* container  = triple->container;

    // validate
    if (IsOverlapWithInsertOrDelete(b, e, *events)) { return std::nullopt; }

    // create new predicates
    auto* new_predicates = new PredicateList(*predicates);
    new_predicates->emplace_back(Predicate{begin, end, global_epoch});
    auto* new_atomic_triple =
        new AtomicTriple(new_predicates, events, container);

    // update atomically
    if (atomic_triple_.compare_exchange_weak(triple, new_atomic_triple)) break;
  }

  {
    auto* tls = gc_items_.Get();
    std::lock_guard<decltype(tls->lock)> lk(tls->lock);
    tls->items.emplace_back(GCItem{triple, global_epoch});
  }

  const auto& container = *triple->container;
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
  return InsertOrDelete(key, false);
}

bool EpochBasedRCURangeIndex::Delete(const std::string_view key) {
  return InsertOrDelete(key, true);
};

bool EpochBasedRCURangeIndex::InsertOrDelete(const std::string_view key,
                                             bool is_delete) {
  // NOTE:
  // The global epoch read here may be larger than the epoch of the transaction
  // which invokes this method.
  // It may cause unnecessary aborts(there are false positives),
  // but it won't miss any phantom anomaly (there are no false negatives).
  auto* triple            = atomic_triple_.load();
  const auto global_epoch = epoch_manager_ref_.GetGlobalEpoch();

  for (;;) {
    // load
    triple = atomic_triple_.load();

    const auto* predicates = triple->predicates;
    const auto* events     = triple->insert_or_delete_events;
    const auto* container  = triple->container;

    // validate
    if (IsInPredicateSet(key, *predicates)) { return false; }

    // create new events
    auto* new_events = new InsertOrDeleteKeySet(*events);
    new_events->emplace_back(
        InsertOrDeleteEvent{std::string(key), is_delete, global_epoch});

    auto* new_atomic_triple =
        new AtomicTriple(predicates, new_events, container);

    // update atomically
    if (atomic_triple_.compare_exchange_weak(triple, new_atomic_triple)) break;
  }

  {
    auto* tls = gc_items_.Get();
    std::lock_guard<decltype(tls->lock)> lk(tls->lock);
    tls->items.emplace_back(GCItem{triple, global_epoch});
  }

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

    /**
     * Begin Atomic Section
     */
    auto* triple         = atomic_triple_.load();
    auto* new_predicates = new PredicateList();
    auto* new_events     = new InsertOrDeleteKeySet();
    auto* new_container  = new ROWEXRangeIndexContainer();
    auto* new_atomic_triple =
        new AtomicTriple{new_predicates, new_events, new_container};

    for (;;) {
      auto* triple    = atomic_triple_.load();
      *new_predicates = *triple->predicates;
      *new_events     = *triple->insert_or_delete_events;
      *new_container  = *triple->container;

      {
        // Clear predicate list
        auto it = std::remove_if(new_predicates->begin(), new_predicates->end(),
                                 [&](const auto& pred) {
                                   const auto del = 2 <= global - pred.epoch;
                                   return del;
                                 });

        new_predicates->erase(it, new_predicates->end());
      }
      {
        // Clear insert_or_delete_keys
        auto it = std::remove_if(new_events->begin(), new_events->end(),
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
            assert(0 < new_container->count(it->key));
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

      {  // update atomically
        if (atomic_triple_.compare_exchange_weak(triple, new_atomic_triple))
          break;
      }
    }

    auto* tls   = gc_items_.Get();
    auto& items = tls->items;
    items.emplace_back(GCItem{triple, global});

    // Garbage Collection
    {
      items.erase(std::remove_if(items.begin(), items.end(),
                                 [&](GCItem& item) {
                                   const bool remove = item.epoch + 2 <= global;
                                   if (remove) delete item.entry;
                                   return remove;
                                 }),
                  items.end());
    }
  }
}

}  // namespace Index
}  // namespace LineairDB
