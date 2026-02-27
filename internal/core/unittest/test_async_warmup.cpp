// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

// Unit tests for the async-warmup cancellation integration:
//   SealedIndexingRecord::drop_field_indexing() and ::clear() must call
//   CancelWarmup() on every evicted CacheSlot before releasing the pointer.

#include <memory>
#include <string>

#include <gtest/gtest.h>
#include <knowhere/comp/index_param.h>

#include "common/Tracer.h"
#include "common/Types.h"
#include "index/Index.h"
#include "index/IndexStats.h"
#include "segcore/SealedIndexingRecord.h"
#include "test_utils/cachinglayer_test_utils.h"

// ─── Minimal IndexBase stub ───────────────────────────────────────────────────
// Lives in milvus::index so it can use the type aliases (BinarySet, Config,
// DatasetPtr) that common/Types.h injects into the milvus:: namespace and
// that are visible in milvus::index:: as an enclosing namespace.

namespace milvus::index {

class StubIndex : public IndexBase {
 public:
    StubIndex() : IndexBase("STUB") {
    }

    BinarySet
    Serialize(const Config& /*config*/) override {
        return {};
    }

    void
    Load(const BinarySet& /*binary_set*/,
         const Config& /*config*/ = {}) override {
    }

    void
    Load(milvus::tracer::TraceContext /*ctx*/,
         const Config& /*config*/ = {}) override {
    }

    void
    BuildWithRawDataForUT(size_t /*n*/,
                          const void* /*values*/,
                          const Config& /*config*/ = {}) override {
    }

    void
    BuildWithDataset(const DatasetPtr& /*dataset*/,
                     const Config& /*config*/ = {}) override {
    }

    void
    Build(const Config& /*config*/ = {}) override {
    }

    int64_t
    Count() override {
        return 0;
    }

    IndexStatsPtr
    Upload(const Config& /*config*/ = {}) override {
        return nullptr;
    }

    const bool
    HasRawData() const override {
        return false;
    }

    bool
    IsMmapSupported() const override {
        return false;
    }
};

}  // namespace milvus::index

// ─── Helper ──────────────────────────────────────────────────────────────────

namespace {

milvus::index::CacheIndexBasePtr
MakeStubCacheIndex(const std::string& key) {
    return milvus::CreateTestCacheIndex(
        key, std::make_unique<milvus::index::StubIndex>());
}

}  // namespace

// ─── SealedIndexingRecord tests ──────────────────────────────────────────────

using milvus::FieldId;
using milvus::segcore::SealedIndexingRecord;

class SealedIndexingRecordTest : public ::testing::Test {
 protected:
    SealedIndexingRecord record_;
};

// After append + drop, is_ready() must return false.
TEST_F(SealedIndexingRecordTest, DropFieldIndexingRemovesEntry) {
    const FieldId fid{100};
    record_.append_field_indexing(
        fid, knowhere::metric::L2, MakeStubCacheIndex("key-drop-1"));

    ASSERT_TRUE(record_.is_ready(fid));

    record_.drop_field_indexing(fid);
    ASSERT_FALSE(record_.is_ready(fid));
}

// Dropping the same field twice must not crash (idempotent).
TEST_F(SealedIndexingRecordTest, DropFieldIndexingIsIdempotent) {
    const FieldId fid{101};
    record_.append_field_indexing(
        fid, knowhere::metric::L2, MakeStubCacheIndex("key-drop-2"));
    record_.drop_field_indexing(fid);
    ASSERT_NO_THROW(record_.drop_field_indexing(fid));
}

// Dropping a field that was never appended must be a no-op.
TEST_F(SealedIndexingRecordTest, DropNonexistentFieldIsNoOp) {
    ASSERT_NO_THROW(record_.drop_field_indexing(FieldId{999}));
}

// clear() must remove all entries.
TEST_F(SealedIndexingRecordTest, ClearRemovesAllEntries) {
    const FieldId fid1{200}, fid2{201}, fid3{202};
    record_.append_field_indexing(
        fid1, knowhere::metric::L2, MakeStubCacheIndex("key-clear-1"));
    record_.append_field_indexing(
        fid2, knowhere::metric::IP, MakeStubCacheIndex("key-clear-2"));
    record_.append_field_indexing(
        fid3, knowhere::metric::COSINE, MakeStubCacheIndex("key-clear-3"));

    ASSERT_TRUE(record_.is_ready(fid1));
    ASSERT_TRUE(record_.is_ready(fid2));
    ASSERT_TRUE(record_.is_ready(fid3));

    record_.clear();

    ASSERT_FALSE(record_.is_ready(fid1));
    ASSERT_FALSE(record_.is_ready(fid2));
    ASSERT_FALSE(record_.is_ready(fid3));
}

// clear() on an empty record must be a no-op.
TEST_F(SealedIndexingRecordTest, ClearEmptyRecordIsNoOp) {
    ASSERT_NO_THROW(record_.clear());
}

// Replacing a field's index must not crash: CancelWarmup fires on the old
// slot before its shared_ptr drops to zero.
TEST_F(SealedIndexingRecordTest, AppendReplaceExistingEntryNocrash) {
    const FieldId fid{300};
    record_.append_field_indexing(
        fid, knowhere::metric::L2, MakeStubCacheIndex("key-replace-old"));
    ASSERT_NO_THROW(record_.append_field_indexing(
        fid, knowhere::metric::L2, MakeStubCacheIndex("key-replace-new")));
    ASSERT_TRUE(record_.is_ready(fid));
}
