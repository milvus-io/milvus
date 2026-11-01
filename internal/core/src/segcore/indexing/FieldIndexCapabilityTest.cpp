// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "segcore/indexing/IndexInventory.h"

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <utility>
#include <vector>

#include <folly/executors/CPUThreadPoolExecutor.h>
#include <folly/synchronization/Latch.h>
#include <gtest/gtest.h>

#include "futures/Future.h"
#include "test_utils/cachinglayer_test_utils.h"

namespace milvus::segcore {
namespace {

class InventoryTestReader final : public index::IIndexReaderBase {
 public:
    explicit InventoryTestReader(index::ReaderCaps caps, int64_t count = 1)
        : caps_(caps), count_(count) {
    }

    index::ReaderCaps
    Caps() const override {
        return caps_;
    }

    index::Domain
    CoordDomain() const override {
        return index::Domain::Row;
    }

    int64_t
    Count() const override {
        return count_;
    }

    DataType
    ValueType() const override {
        return DataType::INT64;
    }

    int64_t
    MemoryUsage() const override {
        return 0;
    }

    cachinglayer::ResourceUsage
    CellByteSize() const override {
        return {};
    }

 private:
    index::ReaderCaps caps_;
    int64_t count_;
};

IndexCapabilityEntry
TestEntry(FieldId field_id = FieldId(10),
          int64_t index_id = 1,
          index::ReaderCaps caps = {.predicate = true}) {
    IndexCapabilityEntry entry{
        IndexKey{field_id, IndexIdentity::PreBuiltIndex(index_id)}};
    entry.family = "inventory-test";
    entry.value_type = DataType::INT64;
    entry.caps = caps;
    return entry;
}

IndexInventory::RootSlot
TestSlot(std::string key,
         index::ReaderCaps caps = {.predicate = true},
         int64_t count = 1) {
    return CreateTestCacheIndex(
        std::move(key), std::make_unique<InventoryTestReader>(caps, count));
}

std::shared_ptr<IndexInventory::Entry>
LazyEntry(IndexCapabilityEntry meta, IndexInventory::Factory factory) {
    return std::make_shared<IndexInventory::Entry>(
        RegistrationFromCapability(meta), std::move(factory));
}

}  // namespace

TEST(IndexIdentityTest, ClassifiesPreBuiltAndSegmentLocalIndexes) {
    EXPECT_EQ(IndexIdentity::PreBuiltIndex(-1).kind(),
              IndexIdentity::Kind::PreBuiltIndex);
    EXPECT_EQ(IndexIdentity::SegmentLocal(1).kind(),
              IndexIdentity::Kind::SegmentLocal);
}

TEST(IndexIdentityTest, IndexKeyIncludesKindAndField) {
    const auto prebuilt =
        IndexKey{FieldId(10), IndexIdentity::PreBuiltIndex(7)};
    const auto same_number_local =
        IndexKey{FieldId(10), IndexIdentity::SegmentLocal(7)};
    const auto same_identity_other_field =
        IndexKey{FieldId(11), IndexIdentity::PreBuiltIndex(7)};

    EXPECT_NE(prebuilt, same_number_local);
    EXPECT_NE(prebuilt, same_identity_other_field);

    std::unordered_set<IndexKey, IndexKeyHash> keys;
    keys.insert(prebuilt);
    keys.insert(same_number_local);
    keys.insert(same_identity_other_field);
    EXPECT_EQ(keys.size(), 3);
}

TEST(IndexInventoryTest, LifecycleReadsDoNotResolveLazyEntry) {
    IndexInventory inventory;
    const auto entry = TestEntry();
    std::atomic<int> calls{0};
    inventory.Register(LazyEntry(entry, [&](OpContext*) {
        ++calls;
        return IndexInventory::ResolvedData{entry.family,
                                            entry.value_type,
                                            entry.caps,
                                            TestSlot("inventory-metadata")};
    }));

    EXPECT_EQ(calls.load(), 0);
    EXPECT_EQ(inventory.Entries().size(), 1);
    EXPECT_TRUE(inventory.Slots().empty());
    EXPECT_EQ(calls.load(), 0);

    auto dropped_snapshot = inventory;
    EXPECT_EQ(dropped_snapshot.Drop(entry.key), nullptr);
    EXPECT_TRUE(dropped_snapshot.Entries().empty());
    EXPECT_EQ(calls.load(), 0);

    const auto capability =
        inventory.ResolveCapability(nullptr, entry.key.field_id);
    ASSERT_EQ(capability.entries().size(), 1);
    EXPECT_EQ(capability.entries().front().family, entry.family);
    EXPECT_EQ(calls.load(), 1);

    auto pin = inventory.PinIndex(nullptr, entry.key);
    EXPECT_TRUE(pin);
    EXPECT_EQ(calls.load(), 1);
    ASSERT_EQ(inventory.Slots().size(), 1);
}

TEST(IndexInventoryTest, FieldLookupResolvesOnlyRequestedField) {
    IndexInventory inventory;
    const std::vector<IndexCapabilityEntry> entries{TestEntry(FieldId(10), 1),
                                                    TestEntry(FieldId(10), 2),
                                                    TestEntry(FieldId(11), 1)};
    int calls[3] = {0, 0, 0};
    for (size_t i = 0; i < entries.size(); ++i) {
        inventory.Register(
            LazyEntry(entries[i], [&, meta = entries[i], i](OpContext*) {
                ++calls[i];
                return IndexInventory::ResolvedData{
                    meta.family,
                    meta.value_type,
                    meta.caps,
                    TestSlot("inventory-field-lookup-" + std::to_string(i),
                             meta.caps,
                             i + 1)};
            }));
    }

    EXPECT_EQ(inventory.Entries().size(), 3);
    EXPECT_EQ(inventory.Entries(FieldId(10)).size(), 2);
    EXPECT_EQ(inventory.Entries(FieldId(11)).size(), 1);
    EXPECT_TRUE(inventory.Entries(FieldId(12)).empty());
    EXPECT_TRUE(inventory.ResolveCapability(nullptr, FieldId(12)).empty());
    EXPECT_TRUE(inventory.Slots().empty());

    const auto capability = inventory.ResolveCapability(nullptr, FieldId(10));
    ASSERT_EQ(capability.entries().size(), 2);
    EXPECT_EQ(capability.entries()[0].key, entries[0].key);
    EXPECT_EQ(capability.entries()[1].key, entries[1].key);
    EXPECT_EQ(calls[0], 1);
    EXPECT_EQ(calls[1], 1);
    EXPECT_EQ(calls[2], 0);

    auto pin = inventory.PinIndex(nullptr, entries[2].key);
    ASSERT_TRUE(pin);
    EXPECT_EQ(pin->Count(), 3);
    EXPECT_EQ(calls[2], 1);
    EXPECT_EQ(inventory.Slots().size(), 3);
    EXPECT_TRUE(
        inventory.PinIndex(nullptr, TestEntry(FieldId(10), 3).key).empty());
    EXPECT_TRUE(
        inventory.PinIndex(nullptr, TestEntry(FieldId(12), 1).key).empty());
}

TEST(IndexInventoryTest, FieldMutationsPreserveOtherFieldsAndSnapshots) {
    IndexInventory inventory;
    const auto first = TestEntry(FieldId(10), 1);
    const auto second = TestEntry(FieldId(10), 2);
    const auto other = TestEntry(FieldId(11), 1);
    const auto first_slot = TestSlot("inventory-field-mutation-first");
    const auto second_slot = TestSlot("inventory-field-mutation-second");
    const auto other_slot = TestSlot("inventory-field-mutation-other");
    inventory.Register(
        std::make_shared<IndexInventory::Entry>(first, first_slot));
    inventory.Register(
        std::make_shared<IndexInventory::Entry>(second, second_slot));
    inventory.Register(
        std::make_shared<IndexInventory::Entry>(other, other_slot));
    auto snapshot = inventory;

    EXPECT_EQ(inventory.Drop(TestEntry(FieldId(10), 3).key), nullptr);
    const auto replacement = TestSlot("inventory-field-mutation-replacement");
    EXPECT_EQ(inventory.Register(
                  std::make_shared<IndexInventory::Entry>(first, replacement)),
              first_slot);
    EXPECT_EQ(inventory.Drop(second.key), second_slot);
    const auto remaining = inventory.Entries(first.key.field_id);
    ASSERT_EQ(remaining.size(), 1);
    EXPECT_EQ(remaining.front().key, first.key);

    EXPECT_EQ(inventory.Drop(first.key), replacement);
    EXPECT_TRUE(inventory.Entries(first.key.field_id).empty());
    EXPECT_TRUE(
        inventory.ResolveCapability(nullptr, first.key.field_id).empty());
    EXPECT_TRUE(inventory.PinIndex(nullptr, first.key).empty());
    EXPECT_EQ(inventory.Drop(first.key), nullptr);
    ASSERT_EQ(inventory.Entries().size(), 1);
    EXPECT_EQ(inventory.Entries().front().key, other.key);
    ASSERT_EQ(inventory.Slots().size(), 1);
    EXPECT_EQ(inventory.Slots().front(), other_slot);

    EXPECT_EQ(inventory.Register(
                  std::make_shared<IndexInventory::Entry>(first, replacement)),
              nullptr);
    EXPECT_EQ(inventory.Entries(first.key.field_id).size(), 1);
    EXPECT_EQ(inventory.Entries().size(), 2);
    EXPECT_EQ(snapshot.Entries(first.key.field_id).size(), 2);
    EXPECT_EQ(snapshot.Entries().size(), 3);
    EXPECT_EQ(snapshot.Slots().size(), 3);
    EXPECT_EQ(snapshot.Drop(first.key), first_slot);
    EXPECT_EQ(snapshot.Drop(second.key), second_slot);
    EXPECT_EQ(snapshot.Drop(other.key), other_slot);
    EXPECT_TRUE(snapshot.Entries().empty());
    EXPECT_TRUE(snapshot.Slots().empty());
    EXPECT_EQ(inventory.Entries().size(), 2);
}

TEST(IndexInventoryTest, LazyEntryResolvesOnceAcrossConcurrentSnapshots) {
    IndexInventory inventory;
    const auto entry = TestEntry();
    std::atomic<int> calls{0};
    inventory.Register(LazyEntry(entry, [&](OpContext*) {
        ++calls;
        return IndexInventory::ResolvedData{entry.family,
                                            entry.value_type,
                                            entry.caps,
                                            TestSlot("inventory-concurrent")};
    }));

    constexpr int kWorkers = 8;
    folly::Latch start(kWorkers);
    std::vector<std::future<FieldIndexCapability>> capabilities;
    for (int i = 0; i != kWorkers; ++i) {
        capabilities.emplace_back(
            std::async(std::launch::async, [&, snapshot = inventory] {
                start.arrive_and_wait();
                return snapshot.ResolveCapability(nullptr, entry.key.field_id);
            }));
    }
    for (auto& future : capabilities) {
        const auto capability = future.get();
        ASSERT_EQ(capability.entries().size(), 1);
        EXPECT_EQ(capability.entries().front().key, entry.key);
    }
    EXPECT_EQ(calls.load(), 1);

    EXPECT_TRUE(inventory.PinIndex(nullptr, entry.key));
    EXPECT_EQ(calls.load(), 1);
}

TEST(IndexInventoryTest, LazyEntryRetriesAfterFactoryFailure) {
    IndexInventory inventory;
    const auto entry = TestEntry();
    std::atomic<int> calls{0};
    inventory.Register(LazyEntry(entry, [&](OpContext*) {
        if (++calls == 1) {
            throw std::runtime_error("expected lazy load failure");
        }
        return IndexInventory::ResolvedData{entry.family,
                                            entry.value_type,
                                            entry.caps,
                                            TestSlot("inventory-retry")};
    }));

    EXPECT_THROW(inventory.ResolveCapability(nullptr, entry.key.field_id),
                 std::runtime_error);
    EXPECT_TRUE(inventory.Slots().empty());
    const auto capability =
        inventory.ResolveCapability(nullptr, entry.key.field_id);
    EXPECT_EQ(capability.entries().size(), 1);
    EXPECT_EQ(calls.load(), 2);
}

TEST(IndexInventoryTest, CancelledWaiterDoesNotCancelSharedInitialization) {
    using namespace std::chrono_literals;
    IndexInventory inventory;
    const auto entry = TestEntry();
    const auto slot = TestSlot("inventory-cancelled-waiter");
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> calls{0};
    inventory.Register(LazyEntry(entry, [&](OpContext*) {
        ++calls;
        entered.set_value();
        released.wait();
        return IndexInventory::ResolvedData{
            entry.family, entry.value_type, entry.caps, slot};
    }));
    auto initializing = std::async(std::launch::async, [&] {
        return inventory.ResolveCapability(nullptr, entry.key.field_id);
    });
    if (entered.get_future().wait_for(5s) != std::future_status::ready) {
        release.set_value();
        initializing.wait();
        FAIL() << "lazy factory did not start";
    }

    folly::CPUThreadPoolExecutor executor(1);
    std::promise<void> waiter_started;
    std::promise<void> waiter_done;
    auto done = waiter_done.get_future();
    auto waiter = futures::Future<FieldIndexCapability>::async(
        &executor,
        0,
        [&, snapshot = inventory](folly::CancellationToken token) {
            OpContext op_ctx(token);
            waiter_started.set_value();
            return new FieldIndexCapability(
                snapshot.ResolveCapability(&op_ctx, entry.key.field_id));
        });
    waiter->registerReadyCallback(
        [](CLockedGoMutex* state) {
            reinterpret_cast<std::promise<void>*>(state)->set_value();
        },
        reinterpret_cast<CLockedGoMutex*>(&waiter_done));
    EXPECT_EQ(waiter_started.get_future().wait_for(5s),
              std::future_status::ready);
    EXPECT_EQ(done.wait_for(30ms), std::future_status::timeout);
    waiter->cancel();

    // The CGO future must become ready while the other query still owns the factory.
    EXPECT_EQ(done.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(initializing.wait_for(0s), std::future_status::timeout);
    EXPECT_TRUE(inventory.Slots().empty());
    EXPECT_EQ(calls.load(), 1);
    release.set_value();
    EXPECT_EQ(initializing.get().entries().size(), 1);
    done.wait();
    auto [result, status] = waiter->leakyGet();
    EXPECT_EQ(result, nullptr);
    EXPECT_EQ(status.error_code, ErrorCode::FollyCancel);
    delete static_cast<FieldIndexCapability*>(result);
    free(const_cast<char*>(status.error_msg));

    auto snapshot = inventory;
    EXPECT_EQ(snapshot.ResolveCapability(nullptr, entry.key.field_id)
                  .entries()
                  .size(),
              1);
    EXPECT_EQ(snapshot.Slots().front(), slot);
    EXPECT_EQ(calls.load(), 1);
}

TEST(IndexInventoryTest, WaitingQueryRetriesAfterInitializerCancellation) {
    using namespace std::chrono_literals;
    IndexInventory inventory;
    const auto entry = TestEntry();
    const auto slot = TestSlot("inventory-cancelled-initializer");
    folly::CancellationSource source;
    OpContext op_ctx(source.getToken());
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> calls{0};
    inventory.Register(LazyEntry(entry, [&](OpContext* ctx) {
        if (++calls == 1) {
            entered.set_value();
            released.wait();
            if (ctx->cancellation_token.isCancellationRequested()) {
                throw SegcoreError(ErrorCode::FollyCancel,
                                   "expected initializer cancellation");
            }
        }
        return IndexInventory::ResolvedData{
            entry.family, entry.value_type, entry.caps, slot};
    }));
    auto initializing = std::async(std::launch::async, [&] {
        return inventory.ResolveCapability(&op_ctx, entry.key.field_id);
    });
    if (entered.get_future().wait_for(5s) != std::future_status::ready) {
        release.set_value();
        initializing.wait();
        FAIL() << "lazy factory did not start";
    }

    std::promise<void> waiter_started;
    auto waiter = std::async(std::launch::async, [&, snapshot = inventory] {
        OpContext waiter_ctx;
        waiter_started.set_value();
        return snapshot.ResolveCapability(&waiter_ctx, entry.key.field_id);
    });
    EXPECT_EQ(waiter_started.get_future().wait_for(5s),
              std::future_status::ready);
    EXPECT_EQ(waiter.wait_for(30ms), std::future_status::timeout);
    source.requestCancellation();
    release.set_value();

    EXPECT_THROW(initializing.get(), SegcoreError);
    EXPECT_EQ(waiter.wait_for(5s), std::future_status::ready);
    EXPECT_EQ(waiter.get().entries().size(), 1);
    EXPECT_EQ(calls.load(), 2);
    ASSERT_EQ(inventory.Slots().size(), 1);
    EXPECT_EQ(inventory.Slots().front(), slot);
}

TEST(IndexInventoryTest, MultiEntryCapabilityRetriesWithoutRepeatingSuccess) {
    IndexInventory inventory;
    const auto first = TestEntry(FieldId(10), 1);
    const auto second = TestEntry(FieldId(10), 2);
    int total_calls = 0;
    int calls[2] = {0, 0};
    int failed_factory = -1;
    auto make_factory = [&](IndexCapabilityEntry entry, int id) {
        return [&, entry, id](OpContext*) -> IndexInventory::ResolvedData {
            ++calls[id];
            if (++total_calls == 2) {
                failed_factory = id;
                throw std::runtime_error("expected second factory failure");
            }
            return {entry.family,
                    entry.value_type,
                    entry.caps,
                    TestSlot("inventory-multi-" + std::to_string(id))};
        };
    };
    inventory.Register(LazyEntry(first, make_factory(first, 0)));
    inventory.Register(LazyEntry(second, make_factory(second, 1)));

    EXPECT_THROW(inventory.ResolveCapability(nullptr, first.key.field_id),
                 std::runtime_error);
    ASSERT_GE(failed_factory, 0);
    EXPECT_EQ(total_calls, 2);
    EXPECT_EQ(inventory.Slots().size(), 1);

    const auto capability =
        inventory.ResolveCapability(nullptr, first.key.field_id);
    ASSERT_EQ(capability.entries().size(), 2);
    std::unordered_set<IndexKey, IndexKeyHash> keys;
    for (const auto& entry : capability.entries()) {
        keys.insert(entry.key);
    }
    EXPECT_EQ(keys.count(first.key), 1);
    EXPECT_EQ(keys.count(second.key), 1);
    EXPECT_EQ(calls[failed_factory], 2);
    EXPECT_EQ(calls[1 - failed_factory], 1);
    EXPECT_EQ(inventory.Slots().size(), 2);
    EXPECT_EQ(inventory.ResolveCapability(nullptr, first.key.field_id)
                  .entries()
                  .size(),
              2);
    EXPECT_EQ(total_calls, 3);
}

TEST(IndexInventoryTest, LazyEntryReleasesFactoryAfterResolution) {
    IndexInventory inventory;
    const auto entry = TestEntry();
    auto metadata = std::make_shared<int>(42);
    std::weak_ptr<int> retained_metadata = metadata;
    inventory.Register(LazyEntry(entry, [metadata, entry](OpContext*) {
        return IndexInventory::ResolvedData{entry.family,
                                            entry.value_type,
                                            entry.caps,
                                            TestSlot("inventory-release")};
    }));
    metadata.reset();
    ASSERT_FALSE(retained_metadata.expired());

    EXPECT_EQ(inventory.ResolveCapability(nullptr, entry.key.field_id)
                  .entries()
                  .size(),
              1);
    EXPECT_TRUE(retained_metadata.expired());
}

TEST(IndexInventoryTest, SnapshotsKeepEntriesAfterDropAndReplace) {
    IndexInventory old_snapshot;
    const auto entry = TestEntry();
    std::atomic<int> old_factory_calls{0};
    old_snapshot.Register(LazyEntry(entry, [&](OpContext*) {
        ++old_factory_calls;
        return IndexInventory::ResolvedData{
            entry.family,
            entry.value_type,
            entry.caps,
            TestSlot("inventory-old", {.predicate = true}, 7)};
    }));

    auto dropped_snapshot = old_snapshot;
    EXPECT_EQ(dropped_snapshot.Drop(entry.key), nullptr);
    EXPECT_TRUE(dropped_snapshot.PinIndex(nullptr, entry.key).empty());
    auto old_pin = old_snapshot.PinIndex(nullptr, entry.key);
    ASSERT_TRUE(old_pin);
    EXPECT_EQ(old_pin->Count(), 7);
    EXPECT_EQ(old_factory_calls.load(), 1);

    auto replaced_snapshot = old_snapshot;
    replaced_snapshot.Register(std::make_shared<IndexInventory::Entry>(
        entry, TestSlot("inventory-new", {.predicate = true}, 11)));
    auto new_pin = replaced_snapshot.PinIndex(nullptr, entry.key);
    ASSERT_TRUE(new_pin);
    EXPECT_EQ(new_pin->Count(), 11);
    EXPECT_EQ(old_pin->Count(), 7);
    EXPECT_EQ(old_factory_calls.load(), 1);
}

TEST(IndexInventoryTest, LifecycleOperationsDoNotWaitForLazyFactory) {
    using namespace std::chrono_literals;
    IndexInventory inventory;
    const auto entry = TestEntry();
    const auto slot = TestSlot("inventory-blocked-factory");
    std::promise<void> entered;
    std::promise<void> release;
    auto released = release.get_future().share();
    std::atomic<int> calls{0};
    inventory.Register(LazyEntry(entry, [&](OpContext*) {
        ++calls;
        entered.set_value();
        released.wait();
        return IndexInventory::ResolvedData{
            entry.family, entry.value_type, entry.caps, slot};
    }));

    auto resolving = std::async(std::launch::async, [&] {
        return inventory.ResolveCapability(nullptr, entry.key.field_id);
    });
    const auto started = entered.get_future().wait_for(5s);
    if (started != std::future_status::ready) {
        release.set_value();
        resolving.wait();
        FAIL() << "lazy factory did not start";
    }

    auto lifecycle = std::async(std::launch::async, [&] {
        const auto slots = inventory.Slots();
        auto dropped = inventory;
        const auto retired_drop = dropped.Drop(entry.key);
        auto replaced = inventory;
        const auto retired_replace =
            replaced.Register(std::make_shared<IndexInventory::Entry>(
                entry, TestSlot("inventory-blocked-replacement")));
        return slots.empty() && retired_drop == nullptr &&
               retired_replace == nullptr;
    });
    const auto status = lifecycle.wait_for(5s);
    release.set_value();
    EXPECT_EQ(status, std::future_status::ready);
    EXPECT_TRUE(lifecycle.get());
    EXPECT_EQ(resolving.get().entries().size(), 1);
    EXPECT_EQ(calls.load(), 1);
    ASSERT_EQ(inventory.Slots().size(), 1);
    EXPECT_EQ(inventory.Slots().front(), slot);
}

TEST(IndexInventoryTest, EagerAndLazyProjectionMetadataMatchesAcrossClones) {
    auto entry = TestEntry();
    entry.json_path = "/doc/value";
    entry.json_cast_type = JsonCastType::FromString("DOUBLE");
    IndexInventory eager;
    eager.Register(std::make_shared<IndexInventory::Entry>(
        entry, TestSlot("inventory-projection-eager")));
    IndexInventory lazy;
    int calls = 0;
    lazy.Register(LazyEntry(entry, [&](OpContext*) {
        ++calls;
        return IndexInventory::ResolvedData{
            entry.family,
            entry.value_type,
            entry.caps,
            TestSlot("inventory-projection-lazy")};
    }));
    auto clone = lazy;
    const auto eager_caps =
        eager.ResolveCapability(nullptr, entry.key.field_id);
    const auto lazy_caps = lazy.ResolveCapability(nullptr, entry.key.field_id);
    const auto clone_caps =
        clone.ResolveCapability(nullptr, entry.key.field_id);
    ASSERT_EQ(eager_caps.entries().size(), 1);
    ASSERT_EQ(lazy_caps.entries().size(), 1);
    ASSERT_EQ(clone_caps.entries().size(), 1);
    const auto& expected = eager_caps.entries().front();
    for (const auto* actual :
         {&lazy_caps.entries().front(), &clone_caps.entries().front()}) {
        EXPECT_EQ(actual->key, expected.key);
        EXPECT_EQ(actual->json_path, expected.json_path);
        EXPECT_EQ(actual->json_cast_type.ToString(),
                  expected.json_cast_type.ToString());
        EXPECT_EQ(actual->family, expected.family);
        EXPECT_EQ(actual->value_type, expected.value_type);
        EXPECT_TRUE(SameCaps(actual->caps, expected.caps));
    }
    EXPECT_EQ(calls, 1);
    ASSERT_EQ(lazy.Slots().size(), 1);
    ASSERT_EQ(clone.Slots().size(), 1);
    EXPECT_EQ(lazy.Slots().front(), clone.Slots().front());
}

}  // namespace milvus::segcore
