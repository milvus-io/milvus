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

// Three-valued audit harness for leaf filter expressions. Evaluates a fixed
// data matrix end to end (CompileExpressions + Eval) on growing / sealed /
// indexed / JSON-stats segments with full-scan, offset and bitmap_input
// inputs, and compares per-candidate TRUE/FALSE/UNKNOWN results with a
// checked-in snapshot.

#include <arrow/array.h>
#include <arrow/builder.h>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <roaring/roaring64map.hh>
#include <simdjson.h>

#include <array>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <memory>
#include <numeric>
#include <optional>
#include <set>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/FieldData.h"
#include "common/Json.h"
#include "common/JsonCastType.h"
#include "common/RoaringMembership.h"
#include "common/Schema.h"
#include "common/Tracer.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/ExprBatchTestUtils.h"
#include "expr/ITypeExpr.h"
#include "index/BitmapIndex.h"
#include "index/IndexFactory.h"
#include "index/JsonScalarIndexWrapper.h"
#include "index/Meta.h"
#include "index/ScalarIndexSort.h"
#include "index/Utils.h"
#include "index/json_stats/JsonKeyStats.h"
#include "knowhere/comp/index_param.h"
#include "pb/common.pb.h"
#include "pb/plan.pb.h"
#include "pb/schema.pb.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/SegcoreConfigUtils.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::segcore;

namespace {

namespace stdfs = std::filesystem;

constexpr const char* kGoldenFile = "tristate_audit_golden.txt";
constexpr const char* kAllowlistFile = "tristate_audit_allowlist.txt";
constexpr int64_t kGrowingChunkRows = 5;
constexpr int64_t kBaseUs = 1735689600LL * 1000000;  // 2025-01-01T00:00:00Z
constexpr int64_t kDayUs = 86400LL * 1000000;

// ---------------------------------------------------------------------------
// Data matrix
// ---------------------------------------------------------------------------

// What json["a"] or json["arr"] holds on a row. Drives the rule oracle.
enum class JKind {
    Number,
    Missing,
    JsonNull,
    String,
    Object,
    Array,
    EmptyArray,
    MixedArray,
    ColumnNull,
};

struct AuditRow {
    std::string tag;
    bool valid;  // shared by i64, ts, json, arr and structA[elem]
    int64_t i64;
    int64_t ts_days;
    std::string json;  // "{}" payload for NULL rows
    std::vector<int64_t> arr;
    std::vector<int64_t> elem;
    JKind a_kind;
    JKind arr_kind;
};

const std::vector<AuditRow>&
AuditRows() {
    static const std::vector<AuditRow> rows = {
        {"normal", true, 1, 0, R"({"a":1,"arr":[1,2]})", {1, 2}, {1, 5},
         JKind::Number, JKind::Array},
        {"normal", true, 5, 40, R"({"a":5,"arr":[3]})", {3}, {5},
         JKind::Number, JKind::Array},
        {"column_null", false, 0, 0, "{}", {}, {},
         JKind::ColumnNull, JKind::ColumnNull},
        {"json_missing_path", true, 10, 10, R"({"b":1})", {1, 3}, {2, 3},
         JKind::Missing, JKind::Missing},
        {"json_type_mismatch", true, -1, 20, R"({"a":"x","arr":"x"})", {2},
         {7}, JKind::String, JKind::String},
        {"json_null_value", true, 3, 30, R"({"a":null,"arr":null})", {1}, {},
         JKind::JsonNull, JKind::JsonNull},
        {"empty_array", true, 2, 50, R"({"a":2,"arr":[]})", {}, {},
         JKind::Number, JKind::EmptyArray},
        {"mixed_type_array", true, 7, 60, R"({"a":1.5,"arr":[1,"x",true]})",
         {1, 2, 3}, {9, 1}, JKind::Number, JKind::MixedArray},
        {"column_null", false, 0, 0, "{}", {}, {},
         JKind::ColumnNull, JKind::ColumnNull},
        {"normal", true, 6, 5, R"({"a":6,"arr":[2,3]})", {2, 3}, {4, 6},
         JKind::Number, JKind::Array},
        {"json_object_at_path", true, 4, 15, R"({"a":{"c":1}})", {5}, {3, 4},
         JKind::Object, JKind::Missing},
        {"normal", true, 8, 70, R"({"a":8,"arr":[1,3]})", {1, 3}, {8},
         JKind::Number, JKind::Array},
    };
    return rows;
}

int64_t
RowCount() {
    return static_cast<int64_t>(AuditRows().size());
}

int64_t
ElementCount() {
    int64_t total = 0;
    for (const auto& row : AuditRows()) {
        if (row.valid) {
            total += static_cast<int64_t>(row.elem.size());
        }
    }
    return total;
}

int64_t
TsValue(const AuditRow& row) {
    return kBaseUs + row.ts_days * kDayUs;
}

std::vector<std::string>
JsonPayloads() {
    std::vector<std::string> payloads;
    for (const auto& row : AuditRows()) {
        payloads.push_back(row.json);
    }
    return payloads;
}

std::vector<uint8_t>
PackedValidity() {
    const auto& rows = AuditRows();
    std::vector<uint8_t> bits((rows.size() + 7) / 8, 0);
    for (size_t i = 0; i < rows.size(); ++i) {
        if (rows[i].valid) {
            bits[i >> 3] |= static_cast<uint8_t>(1 << (i & 0x07));
        }
    }
    return bits;
}

struct AuditFields {
    FieldId pk, i64, ts, json, arr, elem;
};

SchemaPtr
MakeAuditSchema(AuditFields& f) {
    auto schema = std::make_shared<Schema>();
    f.pk = schema->AddDebugField("pk", DataType::INT64);
    f.i64 = schema->AddDebugField("i64", DataType::INT64, true);
    f.ts = schema->AddDebugField("ts", DataType::TIMESTAMPTZ, true);
    f.json = schema->AddDebugField("json", DataType::JSON, true);
    f.arr =
        schema->AddDebugField("arr", DataType::ARRAY, DataType::INT64, true);
    f.elem = schema->AddDebugArrayField("structA[elem]", DataType::INT64, true);
    schema->set_primary_field_id(f.pk);
    return schema;
}

// A fresh dataset per segment: growing Insert may consume proto payloads.
GeneratedData
MakeAuditDataset(const SchemaPtr& schema,
                 const AuditFields& f,
                 int64_t begin = 0,
                 int64_t end = -1) {
    const auto& rows = AuditRows();
    if (end < 0) {
        end = RowCount();
    }
    const int64_t n = end - begin;
    std::vector<int64_t> pk(n), i64(n), ts(n);
    std::vector<std::string> json(n);
    std::vector<ScalarFieldProto> arr(n), elem(n);
    FixedVector<bool> valid(n);
    for (int64_t k = 0; k < n; ++k) {
        const auto& row = rows[begin + k];
        pk[k] = begin + k;
        i64[k] = row.i64;
        ts[k] = TsValue(row);
        json[k] = row.json;
        auto* arr_longs = arr[k].mutable_long_data();  // always set data_case
        for (auto v : row.arr) {
            arr_longs->add_data(v);
        }
        auto* elem_longs = elem[k].mutable_long_data();
        for (auto v : row.elem) {
            elem_longs->add_data(v);
        }
        valid[k] = row.valid;
    }

    auto record = std::make_unique<InsertRecordProto>();
    record->set_num_rows(n);
    auto add = [&](FieldId field_id, const void* data, const bool* validity) {
        auto array =
            CreateDataArrayFrom(data, validity, n, (*schema)[field_id]);
        record->mutable_fields_data()->AddAllocated(array.release());
    };
    add(f.pk, pk.data(), nullptr);
    add(f.i64, i64.data(), valid.data());
    add(f.ts, ts.data(), valid.data());
    add(f.json, json.data(), valid.data());
    add(f.arr, arr.data(), valid.data());
    add(f.elem, elem.data(), valid.data());

    GeneratedData dataset;
    dataset.schema_ = schema;
    dataset.raw_ = record.release();
    dataset.row_ids_.assign(pk.begin(), pk.end());
    dataset.timestamps_.assign(n, 1);
    return dataset;
}

constexpr int64_t kSealedSplitRow = 6;

// Rows [0, 6) and [6, 12) land in separate binlogs, so every field has two
// data chunks with their own SkipIndex statistics.
std::unique_ptr<SegmentSealed>
CreateTwoChunkSealedAudit(const SchemaPtr& schema, const AuditFields& f) {
    const auto first = MakeAuditDataset(schema, f, 0, kSealedSplitRow);
    const auto second =
        MakeAuditDataset(schema, f, kSealedSplitRow, RowCount());
    std::unordered_map<int64_t, std::vector<FieldDataPtr>> field_chunks;
    auto append_dataset = [&](const GeneratedData& dataset) {
        const auto row_count = dataset.row_ids_.size();
        auto row_ids =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        row_ids->FillFieldData(dataset.row_ids_.data(), row_count);
        field_chunks[RowFieldID.get()].push_back(std::move(row_ids));
        auto timestamps =
            std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
        timestamps->FillFieldData(dataset.timestamps_.data(), row_count);
        field_chunks[TimestampFieldID.get()].push_back(std::move(timestamps));
        const auto fields = schema->get_fields();
        for (const auto& data : dataset.raw_->fields_data()) {
            const auto field_id = data.field_id();
            field_chunks[field_id].push_back(CreateFieldDataFromDataArray(
                row_count, &data, fields.at(FieldId(field_id))));
        }
    };
    append_dataset(first);
    append_dataset(second);

    LoadFieldDataInfo combined_load_info;
    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    for (auto& [field_id, chunks] : field_chunks) {
        auto field_load_info = PrepareSingleFieldInsertBinlog(kCollectionID,
                                                              kPartitionID,
                                                              kSegmentID,
                                                              field_id,
                                                              std::move(chunks),
                                                              cm);
        combined_load_info.field_infos.merge(field_load_info.field_infos);
    }
    auto segment = CreateSealedSegment(schema, empty_index_meta);
    const auto status = LoadFieldData(segment.get(), &combined_load_info);
    AssertInfo(status.error_code == Success,
               "failed to load two-chunk sealed audit data: {}",
               status.error_msg);
    return segment;
}

// Deterministic, non-monotonic candidate order (iterative filtering hands
// offsets over in iterator order, not sorted). The last generated id is
// dropped, so offset input is a strict subset of the rows or elements.
std::vector<int32_t>
CandidatePermutation(int64_t n) {
    std::vector<int32_t> ids(n);
    if (n == 0) {
        return ids;
    }
    int64_t step = 5;
    while (std::gcd(step, n) != 1) {
        ++step;
    }
    for (int64_t i = 0; i < n; ++i) {
        ids[i] = static_cast<int32_t>((i * step + 3) % n);
    }
    ids.pop_back();
    return ids;
}

constexpr bool
IsCandidate(int64_t position) {
    return position % 3 != 1;
}

// ---------------------------------------------------------------------------
// Copied helpers (anonymous-namespace originals live in other TUs)
// ---------------------------------------------------------------------------

// Same MRB1 encoding as BuildMrb1 in the RoaringFilterExpr tests.
void
PutU16(std::string& out, size_t offset, uint16_t value) {
    out[offset] = static_cast<char>(value & 0xff);
    out[offset + 1] = static_cast<char>((value >> 8) & 0xff);
}

void
PutU64(std::string& out, size_t offset, uint64_t value) {
    for (int i = 0; i < 8; ++i) {
        out[offset + i] = static_cast<char>((value >> (8 * i)) & 0xff);
    }
}

std::string
BuildMrb1(const std::vector<int64_t>& values) {
    roaring::Roaring64Map bitmap;
    for (auto value : values) {
        bitmap.add(static_cast<uint64_t>(value));
    }
    bitmap.runOptimize();
    std::string body(bitmap.getSizeInBytes(true), '\0');
    const auto written = bitmap.write(body.data(), true);
    AssertInfo(written == body.size(), "roaring body size mismatch");
    std::string blob(RoaringMembership::kHeaderSize + body.size(), '\0');
    std::memcpy(blob.data(),
                RoaringMembership::kMagic.data(),
                RoaringMembership::kMagic.size());
    PutU16(blob, 4, RoaringMembership::kVersion);
    PutU16(blob, 6, RoaringMembership::kFormatPortableRoaring64);
    PutU64(blob, 8, bitmap.cardinality());
    PutU64(blob, 16, body.size());
    PutU64(blob, 24, 0);
    std::memcpy(blob.data() + RoaringMembership::kHeaderSize,
                body.data(),
                body.size());
    return blob;
}

// Same as MakeNullableJsonArray in the JsonContainsByStats tests.
std::shared_ptr<arrow::BinaryArray>
MakeNullableJsonArray(const std::vector<std::string>& json_strings,
                      const std::vector<uint8_t>& valid_data) {
    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < json_strings.size(); ++i) {
        const bool valid = ((valid_data[i >> 3] >> (i & 0x07)) & 1) != 0;
        auto status = valid ? builder.Append(json_strings[i])
                            : builder.AppendNull();
        AssertInfo(status.ok(), "append JSON: {}", status.ToString());
    }
    std::shared_ptr<arrow::Array> array;
    auto status = builder.Finish(&array);
    AssertInfo(status.ok(), "finish JSON array: {}", status.ToString());
    return std::static_pointer_cast<arrow::BinaryArray>(array);
}

int64_t
NextBuildId() {
    static std::atomic<int64_t> next{910000};
    return next++;
}

// Nullable variant of BuildJsonStatsIndex in the JsonContainsByStats tests.
std::shared_ptr<milvus::index::JsonKeyStats>
BuildJsonStats(FieldId json_fid) {
    const int64_t collection_id = 9101;
    const int64_t partition_id = 9201;
    const int64_t segment_id = 9301;
    const int64_t build_id = NextBuildId();
    const int64_t version_id = 1;

    auto field_data =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    field_data->FillFieldData(
        MakeNullableJsonArray(JsonPayloads(), PackedValidity()));
    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);

    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::JSON);
    field_schema.set_fieldid(json_fid.get());
    field_schema.set_nullable(true);
    storage::FieldDataMeta field_meta{collection_id,
                                      partition_id,
                                      segment_id,
                                      json_fid.get(),
                                      field_schema};
    storage::IndexMeta index_meta{
        segment_id, json_fid.get(), build_id, version_id};
    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);
    auto serialized = insert_data.Serialize(storage::Remote);

    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = TestLocalPath;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto arrow_fs = storage::InitArrowFileSystem(storage_config);
    auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                TestLocalPath,
                                collection_id,
                                partition_id,
                                segment_id,
                                json_fid.get(),
                                build_id);
    chunk_manager->Write(log_path, serialized.data(), serialized.size());

    storage::FileManagerContext ctx(
        field_meta, index_meta, chunk_manager, arrow_fs);
    Config build_config;
    build_config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
    auto builder = std::make_shared<milvus::index::JsonKeyStats>(ctx, false);
    builder->Build(build_config);
    auto created = builder->Upload(build_config);

    Config load_config;
    load_config["index_files"] = created->GetIndexFiles();
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;
    load_config[STATS_BASE_PATH_KEY] =
        storage::GenRemoteJsonStatsPathPrefix(chunk_manager,
                                              build_id,
                                              version_id,
                                              collection_id,
                                              partition_id,
                                              segment_id,
                                              json_fid.get());
    auto reader = std::make_shared<milvus::index::JsonKeyStats>(ctx, true);
    reader->Load(milvus::tracer::TraceContext{}, load_config);
    return reader;
}

// ---------------------------------------------------------------------------
// Index loading
// ---------------------------------------------------------------------------

void
LoadSortIndex(SegmentSealed* segment, FieldId field_id, DataType field_type) {
    const auto& rows = AuditRows();
    const size_t n = rows.size();
    std::vector<int64_t> values(n);
    auto valid = std::make_unique<bool[]>(n);
    for (size_t i = 0; i < n; ++i) {
        values[i] = field_type == DataType::TIMESTAMPTZ ? TsValue(rows[i])
                                                        : rows[i].i64;
        valid[i] = rows[i].valid;
    }
    auto scalar_index = milvus::index::CreateScalarIndexSort<int64_t>();
    scalar_index->Build(n, values.data(), valid.get());
    LoadIndexInfo info;
    info.field_id = field_id.get();
    info.field_type = field_type;
    info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    info.index_params = GenIndexParams(scalar_index.get());
    info.cache_index = CreateTestCacheIndex(
        fmt::format("tristate-sort-{}", NextBuildId()),
        std::move(scalar_index));
    segment->LoadIndex(info);
}

void
LoadJsonPathIndex(SegmentSealed* segment, FieldId json_fid) {
    storage::FileManagerContext ctx;
    ctx.fieldDataMeta.field_schema.set_data_type(proto::schema::JSON);
    ctx.fieldDataMeta.field_schema.set_fieldid(json_fid.get());
    ctx.fieldDataMeta.field_schema.set_nullable(true);
    ctx.fieldDataMeta.field_id = json_fid.get();
    const auto cast_type = JsonCastType::FromString("DOUBLE");
    auto created = milvus::index::IndexFactory::GetInstance().CreateJsonIndex(
        milvus::index::CreateIndexInfo{
            .index_type = milvus::index::INVERTED_INDEX_TYPE,
            .json_cast_type = cast_type,
            .json_path = "/a",
        },
        ctx);
    using JsonIndex = milvus::index::JsonInvertedIndex<double>;
    auto json_index = std::unique_ptr<JsonIndex>(
        static_cast<JsonIndex*>(created.release()));

    auto field_data =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    field_data->FillFieldData(
        MakeNullableJsonArray(JsonPayloads(), PackedValidity()));
    json_index->BuildWithFieldData({field_data});
    json_index->finish();
    json_index->create_reader(milvus::index::SetBitsetSealed);

    LoadIndexInfo info;
    info.field_id = json_fid.get();
    info.field_type = DataType::JSON;
    info.index_params = {{JSON_PATH, "/a"},
                         {JSON_CAST_TYPE, cast_type.ToString()}};
    info.cache_index = CreateTestCacheIndex(
        fmt::format("tristate-json-{}", NextBuildId()), std::move(json_index));
    segment->LoadIndex(info);
}

void
LoadArrayBitmapIndex(SegmentSealed* segment, FieldId arr_fid) {
    const auto& rows = AuditRows();
    const int64_t n = RowCount();
    FixedVector<Array> arrays;
    arrays.reserve(n);
    for (const auto& row : rows) {
        ScalarFieldProto proto_row;
        auto* longs = proto_row.mutable_long_data();
        for (auto v : row.arr) {
            longs->add_data(v);
        }
        arrays.emplace_back(proto_row);
    }
    auto valid = PackedValidity();
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::INT64, true);
    field_data->FillFieldData(arrays.data(), valid.data(), n, 0);

    proto::schema::FieldSchema field_schema;
    field_schema.set_name("arr");
    field_schema.set_fieldid(arr_fid.get());
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int64);
    field_schema.set_nullable(true);
    const auto build_id = NextBuildId();
    storage::FileManagerContext ctx;
    ctx.fieldDataMeta = storage::FieldDataMeta{
        kCollectionID, kPartitionID, kSegmentID, arr_fid.get(), field_schema};
    ctx.indexMeta =
        storage::IndexMeta{kSegmentID, arr_fid.get(), build_id, build_id};
    // Load() logs through file_manager_, which exists only for a valid ctx.
    ctx.chunkManagerPtr = storage::RemoteChunkManagerSingleton::GetInstance()
                              .GetRemoteChunkManager();
    auto built_index =
        std::make_unique<milvus::index::BitmapIndex<int64_t>>(ctx, false);
    built_index->BuildWithFieldData({field_data});
    // BuildWithFieldData leaves build_mode_ unset, so In() is only reliable
    // after a Serialize -> Load round trip.
    auto binary_set = built_index->Serialize({});
    auto bitmap_index =
        std::make_unique<milvus::index::BitmapIndex<int64_t>>(ctx, false);
    bitmap_index->Load(binary_set, {});

    LoadIndexInfo info;
    info.field_id = arr_fid.get();
    info.field_type = DataType::ARRAY;
    info.element_type = DataType::INT64;
    info.index_params = GenIndexParams(bitmap_index.get());
    info.cache_index = CreateTestCacheIndex(
        fmt::format("tristate-array-{}", build_id), std::move(bitmap_index));
    segment->LoadIndex(info);
}

// ---------------------------------------------------------------------------
// Cases
// ---------------------------------------------------------------------------

enum class Target { ScalarI64, Tstz, JsonA, JsonArr, ArrayCol, Element };

enum class Rule {
    ColumnNullUnknown,
    ArrayContains,
    ArrayIndexZero,
    JsonScalarPath,
    JsonExists,
    JsonContainsPath,
    ElementLevel,
    AnyKnown,
};

using ExprFactory = std::function<expr::TypedExprPtr(const AuditFields&)>;

struct AuditCase {
    std::string name;
    Target target;
    Rule rule;
    ExprFactory make;
};

proto::plan::GenericValue
I64Val(int64_t v) {
    proto::plan::GenericValue value;
    value.set_int64_val(v);
    return value;
}

std::vector<proto::plan::GenericValue>
I64Vals(const std::vector<int64_t>& values) {
    std::vector<proto::plan::GenericValue> out;
    for (auto v : values) {
        out.push_back(I64Val(v));
    }
    return out;
}

expr::TypedExprPtr
Not(expr::TypedExprPtr inner) {
    return std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, inner);
}

expr::ColumnInfo
ElementColumn(FieldId field_id) {
    proto::plan::ColumnInfo info;
    info.set_field_id(field_id.get());
    info.set_data_type(proto::schema::DataType::Array);
    info.set_element_type(proto::schema::DataType::Int64);
    info.set_nullable(true);
    info.set_is_element_level(true);
    return expr::ColumnInfo(info);
}

std::vector<AuditCase>
AuditCases() {
    using Op = proto::plan::OpType;
    using Arith = proto::plan::ArithOpType;
    using ColumnFn = std::function<expr::ColumnInfo(const AuditFields&)>;
    std::vector<AuditCase> cases;

    struct ScalarColumn {
        std::string suffix;
        Target target;
        Rule rule;
        ColumnFn column;
    };
    const std::vector<ScalarColumn> scalar_columns = {
        {"i64",
         Target::ScalarI64,
         Rule::ColumnNullUnknown,
         [](const AuditFields& f) {
             return expr::ColumnInfo(f.i64, DataType::INT64, {}, true);
         }},
        {"json_a",
         Target::JsonA,
         Rule::JsonScalarPath,
         [](const AuditFields& f) {
             return expr::ColumnInfo(f.json, DataType::JSON, {"a"}, true);
         }},
    };
    for (const auto& sc : scalar_columns) {
        const ColumnFn column = sc.column;
        auto add = [&](const std::string& op, ExprFactory make) {
            cases.push_back(
                {op + "." + sc.suffix, sc.target, sc.rule, std::move(make)});
        };
        auto unary = [column](Op op) {
            return [column, op](const AuditFields& f) -> expr::TypedExprPtr {
                return std::make_shared<expr::UnaryRangeFilterExpr>(
                    column(f), op, I64Val(5));
            };
        };
        add("unary_eq", unary(Op::Equal));
        add("unary_ne", unary(Op::NotEqual));
        add("unary_lt", unary(Op::LessThan));
        add("unary_ge", unary(Op::GreaterEqual));
        add("binary_range",
            [column](const AuditFields& f) -> expr::TypedExprPtr {
                return std::make_shared<expr::BinaryRangeFilterExpr>(
                    column(f), I64Val(1), I64Val(7), true, false);
            });
        add("term_in", [column](const AuditFields& f) -> expr::TypedExprPtr {
            return std::make_shared<expr::TermFilterExpr>(column(f),
                                                          I64Vals({1, 5, 10}));
        });
        add("term_not_in",
            [column](const AuditFields& f) -> expr::TypedExprPtr {
                return Not(std::make_shared<expr::TermFilterExpr>(
                    column(f), I64Vals({1, 5, 10})));
            });
        add("arith_add_eq",
            [column](const AuditFields& f) -> expr::TypedExprPtr {
                return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                    column(f), Op::Equal, Arith::Add, I64Val(7), I64Val(2));
            });
    }

    cases.push_back({"exists.json_a",
                     Target::JsonA,
                     Rule::JsonExists,
                     [](const AuditFields& f) -> expr::TypedExprPtr {
                         return std::make_shared<expr::ExistsExpr>(
                             expr::ColumnInfo(
                                 f.json, DataType::JSON, {"a"}, true));
                     }});
    cases.push_back(
        {"membership_roaring.i64",
         Target::ScalarI64,
         Rule::ColumnNullUnknown,
         [](const AuditFields& f) -> expr::TypedExprPtr {
             return std::make_shared<expr::RoaringFilterExpr>(
                 expr::ColumnInfo(f.i64, DataType::INT64, {}, true),
                 RoaringMembership::Parse(BuildMrb1({1, 5, 10})));
         }});
    cases.push_back(
        {"tstz_add_month_lt.ts",
         Target::Tstz,
         Rule::ColumnNullUnknown,
         [](const AuditFields& f) -> expr::TypedExprPtr {
             proto::plan::Interval interval;
             interval.set_months(1);
             return std::make_shared<expr::TimestamptzArithCompareExpr>(
                 expr::ColumnInfo(f.ts, DataType::TIMESTAMPTZ, {}, true),
                 Arith::Add,
                 interval,
                 Op::LessThan,
                 I64Val(kBaseUs + 60 * kDayUs));
         }});

    struct ContainsOp {
        std::string name;
        proto::plan::JSONContainsExpr_JSONOp op;
        std::vector<int64_t> values;
    };
    const std::vector<ContainsOp> contains_ops = {
        {"contains", proto::plan::JSONContainsExpr_JSONOp_Contains, {1}},
        {"contains_all",
         proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
         {1, 2}},
        {"contains_any",
         proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
         {1, 3}},
    };
    for (const auto& contains : contains_ops) {
        const auto op = contains.op;
        const auto vals = I64Vals(contains.values);
        cases.push_back({"json_" + contains.name + ".json_arr",
                         Target::JsonArr,
                         Rule::JsonContainsPath,
                         [op, vals](const AuditFields& f) -> expr::TypedExprPtr {
                             return std::make_shared<expr::JsonContainsExpr>(
                                 expr::ColumnInfo(
                                     f.json, DataType::JSON, {"arr"}, true),
                                 op,
                                 true,
                                 vals);
                         }});
        cases.push_back({"array_" + contains.name + ".arr",
                         Target::ArrayCol,
                         Rule::ArrayContains,
                         [op, vals](const AuditFields& f) -> expr::TypedExprPtr {
                             return std::make_shared<expr::JsonContainsExpr>(
                                 expr::ColumnInfo(f.arr,
                                                  DataType::ARRAY,
                                                  DataType::INT64,
                                                  {},
                                                  true),
                                 op,
                                 true,
                                 vals);
                         }});
    }
    cases.push_back(
        {"unary_eq_index0.arr",
         Target::ArrayCol,
         Rule::ArrayIndexZero,
         [](const AuditFields& f) -> expr::TypedExprPtr {
             return std::make_shared<expr::UnaryRangeFilterExpr>(
                 expr::ColumnInfo(
                     f.arr, DataType::ARRAY, DataType::INT64, {"0"}, true),
                 Op::Equal,
                 I64Val(1));
         }});

    cases.push_back({"elem_unary_gt.elem",
                     Target::Element,
                     Rule::ElementLevel,
                     [](const AuditFields& f) -> expr::TypedExprPtr {
                         return std::make_shared<expr::UnaryRangeFilterExpr>(
                             ElementColumn(f.elem), Op::GreaterThan, I64Val(2));
                     }});
    cases.push_back({"elem_binary_range.elem",
                     Target::Element,
                     Rule::ElementLevel,
                     [](const AuditFields& f) -> expr::TypedExprPtr {
                         return std::make_shared<expr::BinaryRangeFilterExpr>(
                             ElementColumn(f.elem), I64Val(1), I64Val(5), true, true);
                     }});
    cases.push_back({"elem_term_in.elem",
                     Target::Element,
                     Rule::ElementLevel,
                     [](const AuditFields& f) -> expr::TypedExprPtr {
                         return std::make_shared<expr::TermFilterExpr>(
                             ElementColumn(f.elem), I64Vals({1, 9}));
                     }});
    cases.push_back(
        {"elem_arith_mod_eq.elem",
         Target::Element,
         Rule::ElementLevel,
         [](const AuditFields& f) -> expr::TypedExprPtr {
             return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                 ElementColumn(f.elem), Op::Equal, Arith::Mod, I64Val(1), I64Val(2));
         }});
    // Prunes the second sealed_raw_2chunk chunk by SkipIndex (max 8 < 9).
    cases.push_back({"unary_gt.i64",
                     Target::ScalarI64,
                     Rule::ColumnNullUnknown,
                     [](const AuditFields& f) -> expr::TypedExprPtr {
                         return std::make_shared<expr::UnaryRangeFilterExpr>(
                             expr::ColumnInfo(f.i64, DataType::INT64, {}, true),
                             Op::GreaterThan,
                             I64Val(9));
                     }});
    // `x IN []`: the audit only requires a known result; scalar and JSON paths
    // currently differ on NULL rows.
    for (const bool json_target : {false, true}) {
        const std::string suffix = json_target ? ".json_a" : ".i64";
        const Target target = json_target ? Target::JsonA : Target::ScalarI64;
        auto column = [json_target](const AuditFields& f) {
            return json_target
                       ? expr::ColumnInfo(f.json, DataType::JSON, {"a"}, true)
                       : expr::ColumnInfo(f.i64, DataType::INT64, {}, true);
        };
        cases.push_back(
            {"term_in_empty" + suffix,
             target,
             Rule::AnyKnown,
             [column](const AuditFields& f) -> expr::TypedExprPtr {
                 return std::make_shared<expr::TermFilterExpr>(
                     column(f), std::vector<proto::plan::GenericValue>{});
             }});
    }
    return cases;
}

// Values the expected three-valued rules allow; nullopt when they do not
// constrain the row.
std::optional<std::string>
AllowedBySpec(Rule rule, const AuditRow* row) {
    if (rule == Rule::ElementLevel || rule == Rule::AnyKnown) {
        return "TF";
    }
    AssertInfo(row != nullptr, "row-level rule needs a row");
    switch (rule) {
        case Rule::ColumnNullUnknown:
            return row->valid ? "TF" : "U";
        case Rule::ArrayContains:
            if (!row->valid) {
                return "U";
            }
            return row->arr.empty() ? "F" : "TF";
        case Rule::ArrayIndexZero:
            return !row->valid || row->arr.empty() ? "U" : "TF";
        case Rule::JsonScalarPath:
            return row->a_kind == JKind::Number ? "TF" : "U";
        case Rule::JsonExists:
            switch (row->a_kind) {
                case JKind::ColumnNull:
                case JKind::Missing:
                    return "F";
                case JKind::JsonNull:
                    return std::nullopt;  // left unconstrained
                default:
                    return "T";
            }
        case Rule::JsonContainsPath:
            switch (row->arr_kind) {
                case JKind::Array:
                case JKind::MixedArray:
                    return "TF";
                case JKind::EmptyArray:
                    return "F";
                default:
                    return "U";
            }
        default:
            return std::nullopt;
    }
}

// ---------------------------------------------------------------------------
// Segments and evaluation
// ---------------------------------------------------------------------------

enum class Variant {
    GrowingRaw,
    SealedRaw,
    SealedRaw2Chunk,
    SealedIndexWithRaw,
    SealedIndexOnly,
    SealedJsonStats,
};

enum class InputMode { Full, Offsets, FullBitmap, OffsetsBitmap };

constexpr std::array<InputMode, 4> kInputModes = {InputMode::Full,
                                                   InputMode::Offsets,
                                                   InputMode::FullBitmap,
                                                   InputMode::OffsetsBitmap};

const char*
VariantName(Variant variant) {
    switch (variant) {
        case Variant::GrowingRaw:
            return "growing_raw";
        case Variant::SealedRaw:
            return "sealed_raw";
        case Variant::SealedRaw2Chunk:
            return "sealed_raw_2chunk";
        case Variant::SealedIndexWithRaw:
            return "sealed_index_with_raw";
        case Variant::SealedIndexOnly:
            return "sealed_index_only";
        case Variant::SealedJsonStats:
            return "sealed_json_stats";
    }
    return "unknown";
}

const char*
ModeName(InputMode mode) {
    switch (mode) {
        case InputMode::Full:
            return "full";
        case InputMode::Offsets:
            return "offsets";
        case InputMode::FullBitmap:
            return "full_bitmap";
        case InputMode::OffsetsBitmap:
            return "offsets_bitmap";
    }
    return "unknown";
}

bool
Applies(Variant variant, Target target) {
    switch (variant) {
        case Variant::GrowingRaw:
        case Variant::SealedRaw:
        case Variant::SealedRaw2Chunk:
            return true;
        case Variant::SealedIndexWithRaw:
            return target == Target::ScalarI64 || target == Target::Tstz ||
                   target == Target::JsonA || target == Target::ArrayCol;
        case Variant::SealedIndexOnly:
            return target == Target::ScalarI64 || target == Target::Tstz;
        case Variant::SealedJsonStats:
            return target == Target::JsonA || target == Target::JsonArr;
    }
    return false;
}

struct SegmentUnderTest {
    Variant variant;
    std::shared_ptr<SegmentInternalInterface> segment;
};

std::vector<SegmentUnderTest>
BuildSegments(const SchemaPtr& schema, const AuditFields& f) {
    const int64_t n = RowCount();
    std::vector<SegmentUnderTest> out;
    {
        auto dataset = MakeAuditDataset(schema, f);
        SegcoreConfig config = SegcoreConfig::default_config();
        config.set_chunk_rows(kGrowingChunkRows);
        SegmentGrowingPtr growing =
            CreateGrowingSegment(schema, empty_index_meta, 1, config);
        growing->PreInsert(n);
        growing->Insert(0,
                        n,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        AssertInfo(growing->GetArrayOffsets(f.elem) != nullptr,
                   "growing segment has no ArrayOffsets for structA[elem]");
        out.push_back({Variant::GrowingRaw, std::move(growing)});
    }
    {
        auto sealed = CreateSealedWithFieldDataLoaded(
            schema, MakeAuditDataset(schema, f));
        AssertInfo(sealed->GetArrayOffsets(f.elem) != nullptr,
                   "sealed segment has no ArrayOffsets for structA[elem]");
        out.push_back({Variant::SealedRaw, std::move(sealed)});
    }
    {
        auto sealed = CreateTwoChunkSealedAudit(schema, f);
        AssertInfo(sealed->GetArrayOffsets(f.elem) != nullptr,
                   "two-chunk sealed segment has no ArrayOffsets");
        out.push_back({Variant::SealedRaw2Chunk, std::move(sealed)});
    }
    {
        auto sealed = CreateSealedWithFieldDataLoaded(
            schema, MakeAuditDataset(schema, f));
        LoadSortIndex(sealed.get(), f.i64, DataType::INT64);
        LoadSortIndex(sealed.get(), f.ts, DataType::TIMESTAMPTZ);
        LoadJsonPathIndex(sealed.get(), f.json);
        LoadArrayBitmapIndex(sealed.get(), f.arr);
        out.push_back({Variant::SealedIndexWithRaw, std::move(sealed)});
    }
    {
        auto sealed = CreateSealedWithFieldDataLoaded(
            schema, MakeAuditDataset(schema, f));
        LoadSortIndex(sealed.get(), f.i64, DataType::INT64);
        LoadSortIndex(sealed.get(), f.ts, DataType::TIMESTAMPTZ);
        sealed->DropFieldData(f.i64);
        sealed->DropFieldData(f.ts);
        AssertInfo(!sealed->HasFieldData(f.i64) && !sealed->HasFieldData(f.ts),
                   "index-only variant still has raw data");
        out.push_back({Variant::SealedIndexOnly, std::move(sealed)});
    }
    {
        auto sealed = CreateSealedSegment(schema);
        auto* impl = dynamic_cast<ChunkedSegmentSealedImpl*>(sealed.get());
        AssertInfo(impl != nullptr, "expected ChunkedSegmentSealedImpl");
        impl->SetJsonStatsForTesting(f.json, BuildJsonStats(f.json));
        LoadGeneratedDataIntoSegment(MakeAuditDataset(schema, f), sealed.get());
        out.push_back({Variant::SealedJsonStats, std::move(sealed)});
    }
    return out;
}

struct EvalOutcome {
    bool ok = false;
    std::string error;
    std::vector<std::pair<int64_t, char>> rows;  // (row or element id, T/F/U/X)
};

EvalOutcome
EvalCase(const expr::TypedExprPtr& logical,
         const SegmentInternalInterface* segment,
         int64_t row_count,
         int64_t full_size,
         InputMode mode) {
    EvalOutcome outcome;
    const bool use_offsets =
        mode == InputMode::Offsets || mode == InputMode::OffsetsBitmap;
    const bool use_bitmap =
        mode == InputMode::FullBitmap || mode == InputMode::OffsetsBitmap;
    exec::OffsetVector offsets;
    if (use_offsets) {
        for (auto id : CandidatePermutation(full_size)) {
            offsets.emplace_back(id);
        }
    }
    const int64_t expected =
        use_offsets ? static_cast<int64_t>(offsets.size()) : full_size;
    try {
        auto query_context = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, segment, row_count, MAX_TIMESTAMP);
        exec::ExecContext exec_context(query_context.get());
        auto compiled =
            exec::CompileExpressions({logical}, &exec_context, {}, false);
        AssertInfo(compiled.size() == 1, "expected one compiled expression");
        exec::EvalCtx eval_ctx(&exec_context,
                               use_offsets ? &offsets : nullptr);
        if (use_bitmap) {
            TargetBitmap bitmap_input(expected, false);
            for (int64_t i = 0; i < expected; ++i) {
                if (IsCandidate(i)) {
                    bitmap_input.set(i);
                }
            }
            eval_ctx.set_bitmap_input(std::move(bitmap_input));
        }
        VectorPtr result;
        compiled[0]->Eval(eval_ctx, result);
        AssertInfo(result != nullptr, "expression returned no batch");
        auto column = milvus::test::GetColumnVectorForTest(result);
        AssertInfo(column != nullptr && column->IsBitmap(),
                   "expected a bitmap ColumnVector");
        AssertInfo(static_cast<int64_t>(column->size()) == expected,
                   "expected a single batch of {} results, got {}",
                   expected,
                   column->size());
        TargetBitmapView match(column->GetRawData(), column->size());
        TargetBitmapView known(column->GetValidRawData(), column->size());
        for (int64_t pos = 0; pos < expected; ++pos) {
            if (use_bitmap && !IsCandidate(pos)) {
                continue;  // pre-filtered rows are not compared
            }
            const int64_t row = use_offsets ? offsets[pos] : pos;
            const char value = known[pos] ? (match[pos] ? 'T' : 'F')
                                          : (match[pos] ? 'X' : 'U');
            outcome.rows.emplace_back(row, value);
        }
        outcome.ok = true;
    } catch (const std::exception& e) {
        outcome.error = e.what();
    }
    return outcome;
}

std::string
PathOf(const expr::TypedExprPtr& logical,
       const SegmentInternalInterface* segment,
       int64_t row_count) {
    try {
        return milvus::test::CanExprExecuteAllAtOnce(
                   logical, segment, row_count)
                   ? "non_raw"
                   : "raw";
    } catch (const std::exception&) {
        return "E";
    }
}

struct AuditRecord {
    std::string op;
    Target target;
    Variant variant;
    InputMode mode;
    std::string row;  // decimal id, "path" or "error"
    std::string value;

    std::string
    CaseId() const {
        return op + "|" + VariantName(variant) + "|" + ModeName(mode);
    }
};

std::vector<AuditRecord>
ComputeAudit() {
    // SegcoreConfig members are process-global (inline static); keep the
    // growing chunk size only while the segments are alive. Declared first so
    // it is destroyed last.
    ScopedSegcoreConfigRestore config_restore;
    // Every case must evaluate as a single batch (asserted in EvalCase).
    milvus::test::ExprBatchSizeGuard batch_size_guard(
        DEFAULT_EXEC_EVAL_EXPR_BATCH_SIZE);
    AuditFields fields;
    auto schema = MakeAuditSchema(fields);
    const int64_t rows = RowCount();
    const int64_t elements = ElementCount();
    auto segments = BuildSegments(schema, fields);

    std::vector<AuditRecord> records;
    for (const auto& audit_case : AuditCases()) {
        const auto logical = audit_case.make(fields);
        const int64_t full_size =
            audit_case.target == Target::Element ? elements : rows;
        for (const auto& sut : segments) {
            if (!Applies(sut.variant, audit_case.target)) {
                continue;
            }
            records.push_back({audit_case.name,
                               audit_case.target,
                               sut.variant,
                               InputMode::Full,
                               "path",
                               PathOf(logical, sut.segment.get(), rows)});
            for (auto mode : kInputModes) {
                auto outcome = EvalCase(
                    logical, sut.segment.get(), rows, full_size, mode);
                AuditRecord base{audit_case.name,
                                 audit_case.target,
                                 sut.variant,
                                 mode,
                                 "",
                                 ""};
                if (!outcome.ok) {
                    std::cerr << "[tristate-audit] " << base.CaseId()
                              << " threw: " << outcome.error << std::endl;
                    base.row = "error";
                    base.value = "E";
                    records.push_back(base);
                    continue;
                }
                for (const auto& [row, value] : outcome.rows) {
                    base.row = std::to_string(row);
                    base.value = std::string(1, value);
                    records.push_back(base);
                }
            }
        }
    }
    segments.clear();
    return records;
}

// ---------------------------------------------------------------------------
// Snapshot / allowlist IO
// ---------------------------------------------------------------------------

using SnapshotKey = std::pair<std::string, std::string>;
using Snapshot = std::map<SnapshotKey, std::string>;
using DiffEntry = std::array<std::string, 4>;  // case_id, row, old, new

stdfs::path
AuditDir() {
    if (const char* env = std::getenv("MILVUS_TRISTATE_AUDIT_DIR");
        env != nullptr && *env != '\0') {
        return env;
    }
    return stdfs::path(__FILE__).parent_path() / "test_utils";
}

bool
EnvIsTrue(const char* name) {
    const char* value = std::getenv(name);
    return value != nullptr &&
           (std::string(value) == "1" || std::string(value) == "true");
}

std::vector<std::string>
SplitTabs(const std::string& line) {
    std::vector<std::string> fields;
    std::string field;
    std::istringstream in(line);
    while (std::getline(in, field, '\t')) {
        fields.push_back(field);
    }
    return fields;
}

Snapshot
ToSnapshot(const std::vector<AuditRecord>& records) {
    Snapshot snapshot;
    for (const auto& record : records) {
        auto [it, inserted] = snapshot.emplace(
            SnapshotKey{record.CaseId(), record.row}, record.value);
        AssertInfo(inserted,
                   "duplicate audit key {} {}",
                   record.CaseId(),
                   record.row);
    }
    return snapshot;
}

Snapshot
ReadSnapshot(const stdfs::path& path) {
    Snapshot snapshot;
    std::ifstream in(path);
    AssertInfo(in.good(), "cannot open {}", path.string());
    std::string line;
    int64_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        if (line.empty() || line[0] == '#') {
            continue;
        }
        auto fields = SplitTabs(line);
        AssertInfo(fields.size() == 3,
                   "malformed line {} in {}: {}",
                   line_no,
                   path.string(),
                   line);
        const auto inserted =
            snapshot.emplace(SnapshotKey{fields[0], fields[1]}, fields[2])
                .second;
        AssertInfo(inserted,
                   "duplicate key {} {} at line {} in {}",
                   fields[0],
                   fields[1],
                   line_no,
                   path.string());
    }
    return snapshot;
}

void
WriteSnapshot(const stdfs::path& path, const std::vector<AuditRecord>& records) {
    std::ofstream out(path, std::ios::trunc);
    AssertInfo(out.good(), "cannot write {}", path.string());
    out << "# Tri-state audit golden snapshot, generated by "
           "unittest/test_expr_tristate_audit.cpp on master.\n"
        << "# <op>|<variant>|<input>\\t<row|element id|path|error>\\t"
           "<T|F|U|X|raw|non_raw|E>\n"
        << "# T=(match1,known1) F=(0,1) U=(0,0) X=(1,0). Only bitmap_input "
           "candidates are listed.\n"
        << "# Regenerate only when a result change is intended and reviewed "
           "(MILVUS_TRISTATE_AUDIT_UPDATE=1)\n";
    for (const auto& record : records) {
        out << record.CaseId() << '\t' << record.row << '\t' << record.value
            << '\n';
    }
    out.flush();
    AssertInfo(out.good(), "failed writing {}", path.string());
}

std::set<DiffEntry>
ReadAllowlist(const stdfs::path& path) {
    std::set<DiffEntry> entries;
    std::ifstream in(path);
    if (!in.good()) {
        return entries;
    }
    std::string line;
    int64_t line_no = 0;
    while (std::getline(in, line)) {
        ++line_no;
        if (line.empty() || line[0] == '#') {
            continue;
        }
        auto fields = SplitTabs(line);
        AssertInfo(fields.size() == 4,
                   "malformed allowlist line {} in {}: {}",
                   line_no,
                   path.string(),
                   line);
        entries.insert({fields[0], fields[1], fields[2], fields[3]});
    }
    return entries;
}

std::vector<DiffEntry>
DiffSnapshots(const Snapshot& golden, const Snapshot& current) {
    std::vector<DiffEntry> diffs;
    for (const auto& [key, old_value] : golden) {
        auto it = current.find(key);
        const std::string new_value = it == current.end() ? "-" : it->second;
        if (new_value != old_value) {
            diffs.push_back({key.first, key.second, old_value, new_value});
        }
    }
    for (const auto& [key, new_value] : current) {
        if (golden.find(key) == golden.end()) {
            diffs.push_back({key.first, key.second, "-", new_value});
        }
    }
    return diffs;
}

}  // namespace

TEST(TriStateAudit, MatchesGoldenSnapshot) {
    const auto dir = AuditDir();
    const auto golden_path = dir / kGoldenFile;
    const auto records = ComputeAudit();
    ASSERT_FALSE(records.empty());

    if (EnvIsTrue("MILVUS_TRISTATE_AUDIT_UPDATE")) {
        WriteSnapshot(golden_path, records);
        std::cout << "[tristate-audit] wrote " << records.size()
                  << " lines to " << golden_path << std::endl;
        return;
    }
    ASSERT_TRUE(stdfs::exists(golden_path))
        << golden_path << " is missing; generate it on master with "
        << "MILVUS_TRISTATE_AUDIT_UPDATE=1";

    const auto allowlist_path = dir / kAllowlistFile;
    const auto allowlist = ReadAllowlist(allowlist_path);
    const auto diffs = DiffSnapshots(ReadSnapshot(golden_path),
                                     ToSnapshot(records));

    std::set<DiffEntry> seen_allowed;
    size_t not_allowed = 0;
    for (const auto& diff : diffs) {
        const bool allowed = allowlist.count(diff) != 0;
        if (allowed) {
            seen_allowed.insert(diff);
        } else {
            ++not_allowed;
        }
        std::cout << "TRISTATE_DIFF\t" << diff[0] << '\t' << diff[1] << '\t'
                  << diff[2] << '\t' << diff[3] << '\t'
                  << (allowed ? "allowed" : "NOT_ALLOWED") << '\n';
    }
    if (const char* out_path = std::getenv("MILVUS_TRISTATE_AUDIT_DIFF_OUT");
        out_path != nullptr && *out_path != '\0') {
        std::ofstream out(out_path, std::ios::trunc);
        AssertInfo(out.good(), "cannot write {}", out_path);
        for (const auto& diff : diffs) {
            out << diff[0] << '\t' << diff[1] << '\t' << diff[2] << '\t'
                << diff[3] << '\n';
        }
        out.flush();
        AssertInfo(out.good(), "failed writing {}", out_path);
    }
    EXPECT_EQ(not_allowed, 0u)
        << not_allowed << " tri-state differences are not listed in "
        << allowlist_path;

    if (EnvIsTrue("MILVUS_TRISTATE_AUDIT_EXACT")) {
        for (const auto& entry : allowlist) {
            if (seen_allowed.count(entry) == 0) {
                std::cout << "TRISTATE_STALE_ALLOW\t" << entry[0] << '\t'
                          << entry[1] << '\t' << entry[2] << '\t' << entry[3]
                          << '\n';
            }
        }
        EXPECT_EQ(seen_allowed.size(), allowlist.size())
            << "allowlist entries that no longer occur (diff must equal the "
               "inconsistency list)";
    }
}

TEST(TriStateAudit, SnapshotIsDeterministic) {
    const auto diffs =
        DiffSnapshots(ToSnapshot(ComputeAudit()), ToSnapshot(ComputeAudit()));
    for (size_t i = 0; i < diffs.size() && i < 20; ++i) {
        std::cout << "NONDETERMINISTIC\t" << diffs[i][0] << '\t' << diffs[i][1]
                  << '\t' << diffs[i][2] << '\t' << diffs[i][3] << '\n';
    }
    EXPECT_TRUE(diffs.empty()) << diffs.size() << " unstable audit lines";
}

// Review aid, skipped unless MILVUS_TRISTATE_AUDIT_REPORT=<file> is set. Never
// fails on findings: writes cross-path mismatches and rule violations to the
// file as input for the reviewed inconsistency list.
TEST(TriStateAudit, CrossPathAndRuleReport) {
    const char* report_path = std::getenv("MILVUS_TRISTATE_AUDIT_REPORT");
    if (report_path == nullptr || *report_path == '\0') {
        GTEST_SKIP() << "set MILVUS_TRISTATE_AUDIT_REPORT=<file> to write the "
                        "report";
    }
    const auto records = ComputeAudit();
    std::map<std::string, Rule> rule_of;
    for (const auto& audit_case : AuditCases()) {
        rule_of.emplace(audit_case.name, audit_case.rule);
    }

    std::ostringstream report;
    std::map<SnapshotKey, std::map<std::string, std::string>> by_row;
    size_t violations = 0;
    size_t errors = 0;
    for (const auto& record : records) {
        if (record.row == "path") {
            continue;
        }
        if (record.row == "error") {
            ++errors;
            report << "ERROR\t" << record.CaseId() << '\n';
            continue;
        }
        by_row[{record.op, record.row}]
              [std::string(VariantName(record.variant)) + "/" +
               ModeName(record.mode)] = record.value;

        const auto row_index = std::stoll(record.row);
        const AuditRow* row = record.target == Target::Element
                                  ? nullptr
                                  : &AuditRows().at(row_index);
        const auto allowed = AllowedBySpec(rule_of.at(record.op), row);
        if (allowed && allowed->find(record.value) == std::string::npos) {
            ++violations;
            report << "RULE\t" << record.CaseId() << '\t' << record.row
                   << "\ttag=" << (row ? row->tag : "element")
                   << "\tgot=" << record.value << "\tallowed=" << *allowed
                   << '\n';
            if (allowed->size() == 1) {
                report << "ALLOW_SUGGEST\t" << record.CaseId() << '\t'
                       << record.row << '\t' << record.value << '\t'
                       << *allowed << '\n';
            }
        }
    }
    size_t mismatches = 0;
    for (const auto& [key, values] : by_row) {
        std::set<std::string> distinct;
        for (const auto& [where, value] : values) {
            distinct.insert(value);
        }
        if (distinct.size() <= 1) {
            continue;
        }
        ++mismatches;
        report << "MISMATCH\t" << key.first << '\t' << key.second;
        for (const auto& [where, value] : values) {
            report << '\t' << where << '=' << value;
        }
        report << '\n';
    }
    std::cout << report.str() << "[tristate-audit] " << mismatches
              << " cross-path mismatches, " << violations
              << " rule violations, " << errors << " errors" << std::endl;
    std::ofstream out(report_path, std::ios::trunc);
    AssertInfo(out.good(), "cannot write {}", report_path);
    out << report.str();
    out.flush();
    AssertInfo(out.good(), "failed writing {}", report_path);
}
