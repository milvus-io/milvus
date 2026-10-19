// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <gtest/gtest.h>
#include <arrow/api.h>
#include <arrow/io/memory.h>
#include <boost/filesystem.hpp>
#include <folly/ScopeGuard.h>

#include <atomic>
#include <cstring>
#include <future>
#include <memory>
#include <string>
#include <vector>

#include "common/FieldData.h"
#include "common/Json.h"
#include "index/JsonInvertedIndex.h"
#include "index/Utils.h"
#include "storage/ChunkManager.h"
#include "storage/IndexEntryDirectStreamWriter.h"
#include "storage/IndexEntryReader.h"
#include "storage/RemoteInputStream.h"
#include "storage/RemoteOutputStream.h"
#include "storage/Util.h"

namespace milvus::index {
namespace {

std::atomic<size_t> comparable_hits{0};

void
CountComparableHits(void* bitset, const uint32_t* doc_id, uintptr_t n) {
    comparable_hits.fetch_add(n, std::memory_order_relaxed);
    SetBitsetSealed(bitset, doc_id, n);
}

void
ExpectJsonOffsets(const TargetBitmap& bitmap,
                  const std::vector<size_t>& offsets) {
    EXPECT_EQ(bitmap.count(), offsets.size());
    for (auto offset : offsets) {
        EXPECT_TRUE(bitmap[offset]) << offset;
    }
}

template <typename T>
void
CheckJsonValidityCache(const char* cast_name,
                       const std::vector<std::string>& raw,
                       const std::vector<size_t>& valid,
                       const std::vector<size_t>& exists,
                       bool nullable_last = false,
                       const char* cast_function = "unknown",
                       bool array = false) {
    const auto root =
        (boost::filesystem::temp_directory_path() /
         boost::filesystem::unique_path("json-validity-cache-%%%%-%%%%"))
            .string();
    storage::StorageConfig config;
    config.storage_type = "local";
    config.root_path = root;
    auto cm = storage::CreateChunkManager(config);
    auto fs = storage::InitArrowFileSystem(config);
    auto cleanup =
        folly::makeGuard([&] { boost::filesystem::remove_all(root); });
    storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(proto::schema::JSON);
    field_meta.field_schema.set_fieldid(101);
    field_meta.field_schema.set_nullable(nullable_last);
    storage::IndexMeta index_meta;
    index_meta.segment_id = 3;
    index_meta.field_id = 101;
    index_meta.build_id = 4000;
    index_meta.index_version = 1;
    storage::FileManagerContext context(field_meta, index_meta, cm, fs);
    auto make_index = [&] {
        return std::make_unique<JsonInvertedIndex<T>>(
            JsonCastType::FromString(cast_name),
            "/a",
            context,
            TANTIVY_INDEX_LATEST_VERSION,
            JsonCastFunction::FromString(cast_function));
    };
    auto index = make_index();
    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < raw.size(); ++i) {
        if (nullable_last && i + 1 == raw.size()) {
            ASSERT_TRUE(builder.AppendNull().ok());
        } else {
            ASSERT_TRUE(builder.Append(raw[i]).ok());
        }
    }
    std::shared_ptr<arrow::Array> data;
    ASSERT_TRUE(builder.Finish(&data).ok());
    auto field =
        std::make_shared<FieldData<Json>>(DataType::JSON, nullable_last);
    field->FillFieldData(data);
    index->BuildWithFieldData({field});
    index->finish();

    comparable_hits.store(0);
    index->create_reader(CountComparableHits);
    // Positive fixtures prove that finalization queried real Tantivy postings.
    const auto initial_hits = comparable_hits.load();
    EXPECT_EQ(initial_hits, array ? 0 : valid.size());

    auto check = [&](JsonInvertedIndex<T>& reader) {
        EXPECT_EQ(reader.Count(), raw.size());
        ExpectJsonOffsets(reader.IsNotNull(), valid);
        EXPECT_EQ(reader.IsNull().count(), raw.size() - valid.size());
        ExpectJsonOffsets(reader.Exists(), exists);
        EXPECT_EQ(reader.ValidityBitmapByteSize(),
                  valid.size() == raw.size()
                      ? 0
                      : TargetBitmap(raw.size()).size_in_bytes());
        if (!array) {
            T unused{};
            ExpectJsonOffsets(reader.NotIn(0, &unused), valid);
        }
    };
    check(*index);
    auto copy = index->IsNotNull();
    copy.flip();
    index->FinalizeSealed();
    std::vector<std::future<void>> readers;
    for (int i = 0; i < 4; ++i) {
        readers.push_back(std::async(std::launch::async, [&] {
            for (int repeat = 0; repeat < 8; ++repeat) {
                check(*index);
            }
        }));
    }
    for (auto& reader : readers) {
        reader.get();
    }
    // Repeated finalization and concurrent null predicates reuse the cache.
    EXPECT_EQ(comparable_hits.load(), initial_hits);

    // Both writers keep the historical whole-field-only null representation.
    const auto scattered = index->Serialize({});
    EXPECT_EQ(scattered.Contains(INDEX_NULL_OFFSET_FILE_NAME), nullable_last);
    if (nullable_last) {
        const auto offsets = scattered.GetByName(INDEX_NULL_OFFSET_FILE_NAME);
        ASSERT_EQ(offsets->size, sizeof(size_t));
        size_t offset;
        std::memcpy(&offset, offsets->data.get(), sizeof(offset));
        EXPECT_EQ(offset, raw.size() - 1);
    }
    auto buffer = arrow::io::BufferOutputStream::Create().ValueOrDie();
    auto output = std::make_shared<storage::RemoteOutputStream>(buffer);
    storage::IndexEntryDirectStreamWriter writer(output);
    index->WriteEntries(&writer);
    writer.Finish();
    auto input = std::make_shared<storage::RemoteInputStream>(
        std::make_shared<arrow::io::BufferReader>(
            buffer->Finish().ValueOrDie()));
    auto packed = storage::IndexEntryReader::Open(input, input->Size());
    EXPECT_EQ(packed->GetMeta<bool>("has_null"), nullable_last);
    if (nullable_last) {
        const auto offsets = packed->ReadEntry(INDEX_NULL_OFFSET_FILE_NAME);
        ASSERT_EQ(offsets.data.size(), sizeof(size_t));
        size_t offset;
        std::memcpy(&offset, offsets.data.data(), sizeof(offset));
        EXPECT_EQ(offset, raw.size() - 1);
    }
    auto stats = index->Upload();
    context.set_for_loading_index(true);
    for (const bool mmap : {false, true}) {
        SCOPED_TRACE(mmap);
        {
            auto loaded = make_index();
            loaded->LoadEntries(*packed, {{ENABLE_MMAP, mmap}});
            check(*loaded);
            loaded->FinalizeSealed(true);
            check(*loaded);
        }
        {
            auto loaded = make_index();
            loaded->Load(
                milvus::tracer::TraceContext{},
                {{INDEX_FILES, stats->GetIndexFiles()}, {ENABLE_MMAP, mmap}});
            check(*loaded);
            loaded->FinalizeSealed(true);
            check(*loaded);
        }
    }
}

TEST(JsonValidityCacheTest, DoubleMissingCastFailureAndFieldNull) {
    CheckJsonValidityCache<double>("DOUBLE",
                                   {R"({"a":1})",
                                    R"({"a":0})",
                                    R"({"a":"bad"})",
                                    "{}",
                                    R"({"a":null})",
                                    R"({"a":2})"},
                                   {0, 1},
                                   {0, 1, 2},
                                   true);
}

TEST(JsonValidityCacheTest, FalseAndEmptyStringRemainComparable) {
    CheckJsonValidityCache<bool>(
        "BOOL",
        {R"({"a":false})", R"({"a":true})", R"({"a":0})", "{}"},
        {0, 1},
        {0, 1, 2});
    CheckJsonValidityCache<std::string>(
        "VARCHAR",
        {R"({"a":""})", R"({"a":"x"})", R"({"a":1})", "{}"},
        {0, 1},
        {0, 1, 2});
}

TEST(JsonValidityCacheTest, AllValidAndAllInvalid) {
    CheckJsonValidityCache<double>(
        "DOUBLE", {R"({"a":0})", R"({"a":1})"}, {0, 1}, {0, 1});
    CheckJsonValidityCache<double>(
        "DOUBLE", {R"({"a":"bad"})", "{}", R"({"a":null})"}, {}, {0});
}

TEST(JsonValidityCacheTest, CastFiniteNumericDomain) {
    CheckJsonValidityCache<double>("DOUBLE",
                                   {R"({"a":"1.5"})",
                                    R"({"a":"nan"})",
                                    R"({"a":"inf"})",
                                    R"({"a":"-inf"})",
                                    R"({"a":"bad"})",
                                    R"({"a":"1.7976931348623157e308"})",
                                    R"({"a":"-1.7976931348623157e308"})"},
                                   {0, 5, 6},
                                   {0, 1, 2, 3, 4, 5, 6},
                                   false,
                                   "STRING_TO_DOUBLE");
}

TEST(JsonValidityCacheTest, AllInvalidWithExistingPaths) {
    CheckJsonValidityCache<double>(
        "DOUBLE", {R"({"a":"bad"})", R"({"a":false})"}, {}, {0, 1});
}

TEST(JsonValidityCacheTest, ArrayValidityIsUnchanged) {
    CheckJsonValidityCache<double>(
        "ARRAY_DOUBLE",
        {R"({"a":[]})", R"({"a":[1]})", "{}", R"({"a":null})"},
        {0, 1, 2, 3},
        // Json::exist treats an empty array as absent.
        {1},
        false,
        "unknown",
        true);
}

}  // namespace
}  // namespace milvus::index
