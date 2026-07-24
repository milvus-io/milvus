// Copyright (C) 2019-2025 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#include <arrow/builder.h>
#include <fmt/core.h>
#include <gtest/gtest.h>
#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "common/Consts.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "exec/QueryContext.h"
#include "exec/expression/BinaryRangeExpr.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/UnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "index/json_stats/JsonKeyStats.h"
#include "cachinglayer/Manager.h"
#include "segcore/storagev2translator/JsonStatsTranslator.h"
#include "pb/plan.pb.h"
#include "plan/PlanNode.h"
#include "query/ExecPlanNodeVisitor.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/Types.h"
#include "storage/InsertData.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::index;

namespace {

bool
IsValidAt(const std::vector<uint8_t>& valid_data, size_t i) {
    return ((valid_data[i >> 3] >> (i & 0x07)) & 1) != 0;
}

std::shared_ptr<arrow::BinaryArray>
MakeNullableJsonArray(const std::vector<std::string>& json_strings,
                      const std::vector<uint8_t>& valid_data) {
    arrow::BinaryBuilder builder;
    for (size_t i = 0; i < json_strings.size(); ++i) {
        auto status = IsValidAt(valid_data, i) ? builder.Append(json_strings[i])
                                               : builder.AppendNull();
        AssertInfo(status.ok(),
                   "failed to build nullable JSON Arrow array: {}",
                   status.ToString());
    }

    std::shared_ptr<arrow::Array> array;
    auto status = builder.Finish(&array);
    AssertInfo(status.ok(),
               "failed to finish nullable JSON Arrow array: {}",
               status.ToString());
    return std::static_pointer_cast<arrow::BinaryArray>(array);
}

milvus::index::CacheJsonKeyStatsPtr
BuildAndLoadJsonKeyStats(const std::vector<std::string>& json_strings,
                         const milvus::FieldId json_fid,
                         const std::string& root_path,
                         int64_t collection_id,
                         int64_t partition_id,
                         int64_t segment_id,
                         int64_t field_id,
                         int64_t build_id,
                         int64_t version_id,
                         const std::vector<uint8_t>* valid_data = nullptr) {
    std::vector<milvus::Json> data;
    data.reserve(json_strings.size());
    for (const auto& s : json_strings) {
        data.emplace_back(simdjson::padded_string(s));
    }

    auto nullable = valid_data != nullptr;
    auto field_data =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, nullable);
    if (valid_data != nullptr) {
        field_data->FillFieldData(
            MakeNullableJsonArray(json_strings, *valid_data));
    } else {
        field_data->add_json_data(data);
    }

    auto payload_reader =
        std::make_shared<milvus::storage::PayloadReader>(field_data);
    storage::InsertData insert_data(payload_reader);

    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::JSON);
    field_schema.set_fieldid(json_fid.get());
    field_schema.set_nullable(nullable);

    storage::FieldDataMeta field_meta{
        collection_id, partition_id, segment_id, field_id, field_schema};
    storage::IndexMeta index_meta{segment_id, field_id, build_id, version_id};

    insert_data.SetFieldDataMeta(field_meta);
    insert_data.SetTimestamps(0, 100);

    auto serialized_bytes = insert_data.Serialize(storage::Remote);

    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);

    auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                root_path,
                                collection_id,
                                partition_id,
                                segment_id,
                                field_id,
                                0);
    chunk_manager->Write(
        log_path, serialized_bytes.data(), serialized_bytes.size());

    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    Config build_config;
    build_config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};

    auto builder = std::make_shared<JsonKeyStats>(ctx, false);
    builder->Build(build_config);

    auto create_index_result = builder->Upload(build_config);
    auto index_files = create_index_result->GetIndexFiles();

    Config load_config;
    load_config["index_files"] = index_files;
    load_config[milvus::LOAD_PRIORITY] =
        milvus::proto::common::LoadPriority::HIGH;

    milvus::segcore::storagev2translator::JsonStatsLoadInfo load_info{
        /* enable_mmap */ false,
        /* mmap_dir_path */ "",
        /* segment_id */ segment_id,
        /* field_id */ field_id,
        /* stats_size */ 0,
        /* warmup_policy */ ""};

    std::unique_ptr<
        milvus::cachinglayer::Translator<milvus::index::JsonKeyStats>>
        base_translator = std::make_unique<
            milvus::segcore::storagev2translator::JsonStatsTranslator>(
            load_info, milvus::tracer::TraceContext{}, ctx, load_config);

    auto slot = milvus::cachinglayer::Manager::GetInstance().CreateCacheSlot(
        std::move(base_translator));
    return slot;
}

struct ExprBatchEvalResult {
    std::vector<int64_t> batch_sizes;
    ColumnVectorPtr result;
};

class ExprBatchSizeGuard {
 public:
    explicit ExprBatchSizeGuard(int64_t batch_size)
        : old_batch_size_(EXEC_EVAL_EXPR_BATCH_SIZE.exchange(batch_size)) {
    }

    ~ExprBatchSizeGuard() {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(old_batch_size_);
    }

 private:
    int64_t old_batch_size_;
};

ExprBatchEvalResult
EvalExprInBatches(const expr::TypedExprPtr& logical_expr,
                  const segcore::SegmentInternalInterface* segment,
                  int64_t active_count) {
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical_expr}, &exec_context, {}, false);
    AssertInfo(compiled.size() == 1,
               "expected one compiled expression, got {}",
               compiled.size());

    exec::EvalCtx eval_context(&exec_context);
    ExprBatchEvalResult evaluation;
    TargetBitmap combined_result;
    TargetBitmap combined_validity;
    int64_t processed_rows = 0;
    while (processed_rows < active_count) {
        VectorPtr result;
        compiled[0]->Eval(eval_context, result);
        AssertInfo(result != nullptr,
                   "expression evaluation stopped after {} of {} rows",
                   processed_rows,
                   active_count);
        auto column = std::dynamic_pointer_cast<ColumnVector>(result);
        AssertInfo(column != nullptr && column->IsBitmap(),
                   "expected bitmap column result");
        const auto batch_size = column->size();
        AssertInfo(
            batch_size > 0 && processed_rows + batch_size <= active_count,
            "invalid expression batch size {} after {} of {} rows",
            batch_size,
            processed_rows,
            active_count);
        evaluation.batch_sizes.push_back(batch_size);
        combined_result.append(
            TargetBitmapView(column->GetRawData(), batch_size));
        combined_validity.append(
            TargetBitmapView(column->GetValidRawData(), batch_size));
        processed_rows += batch_size;
    }
    evaluation.result = std::make_shared<ColumnVector>(
        std::move(combined_result), std::move(combined_validity));
    return evaluation;
}

ColumnVectorPtr
EvalExprOnce(const expr::TypedExprPtr& logical_expr,
             const segcore::SegmentInternalInterface* segment,
             int64_t active_count,
             exec::OffsetVector* offsets = nullptr) {
    auto query_context = std::make_shared<exec::QueryContext>(
        DEAFULT_QUERY_ID, segment, active_count, MAX_TIMESTAMP);
    exec::ExecContext exec_context(query_context.get());
    auto compiled =
        exec::CompileExpressions({logical_expr}, &exec_context, {}, false);
    AssertInfo(compiled.size() == 1,
               "expected one compiled expression, got {}",
               compiled.size());

    exec::EvalCtx eval_context(&exec_context);
    eval_context.set_offset_input(offsets);
    VectorPtr result;
    compiled[0]->Eval(eval_context, result);
    auto column = std::dynamic_pointer_cast<ColumnVector>(result);
    AssertInfo(column != nullptr && column->IsBitmap(),
               "expected bitmap column result");
    return column;
}

}  // namespace

TEST(JsonContainsByStatsTest, BasicContainsAnyOnArray) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);

    auto segment = segcore::CreateSealedSegment(schema);

    const int N = 10000;
    std::vector<std::string> json_raw_data;
    json_raw_data.reserve(N);
    for (int i = 0; i < N; ++i) {
        switch (i % 7) {
            case 0:
                json_raw_data.emplace_back(R"({"a": [1, 2, 3]})");
                break;
            case 1:
                json_raw_data.emplace_back(R"({"a": [4, 5]})");
                break;
            case 2:
                json_raw_data.emplace_back(R"({"a": [1]})");
                break;
            case 3:
                json_raw_data.emplace_back(R"({"a": []})");
                break;
            case 4:
                json_raw_data.emplace_back(R"({"b": [1, 2]})");
                break;
            case 5:
                json_raw_data.emplace_back(R"({"a": [10, 1, 20]})");
                break;
            case 6:
                json_raw_data.emplace_back(R"({"a": ["x", "y"]})");
                break;
        }
    }

    // Build and attach JsonKeyStats for the json field
    const int64_t collection_id = 1001;
    const int64_t partition_id = 2001;
    const int64_t segment_id = 3001;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5001;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          root_path,
                                          collection_id,
                                          partition_id,
                                          segment_id,
                                          field_id,
                                          build_id,
                                          version_id);
    segment->LoadJsonStats(json_fid, stats);

    // Load raw field data into sealed segment for execution
    std::vector<milvus::Json> jsons;
    for (auto& s : json_raw_data) {
        jsons.emplace_back(simdjson::padded_string(s));
    }
    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, false);
    json_field->add_json_data(jsons);

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    // Build json_contains expr: json['a'] contains any 1
    proto::plan::GenericValue value;
    value.set_int64_val(1);
    auto expr = std::make_shared<expr::JsonContainsExpr>(
        expr::ColumnInfo(
            json_fid, DataType::JSON, std::vector<std::string>{"a"}, true),
        proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
        true,
        std::vector<proto::plan::GenericValue>{value});

    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, expr);
    auto result = query::ExecuteQueryExpr(
        plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

    // Expected matches: positions where (i % 7) in {0, 2, 5}
    int64_t expected_count = (N / 7) * 3;
    int rem = N % 7;
    for (int i = 0; i < rem; ++i) {
        if (i == 0 || i == 2 || i == 5) {
            expected_count++;
        }
    }
    EXPECT_EQ(result.count(), expected_count);
    for (int i = 0; i < N; ++i) {
        bool should_match = ((i % 7) == 0) || ((i % 7) == 2) || ((i % 7) == 5);
        EXPECT_EQ(bool(result[i]), should_match);
    }
}

TEST(JsonContainsByStatsTest, EmptyElementsAdvanceWithoutRawJson) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON);
    const std::vector<std::string> json_raw_data(8, R"({"a": [1, 2]})");

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1002,
                                          2002,
                                          3002,
                                          json_fid.get(),
                                          5002,
                                          1);
    auto segment = segcore::CreateSealedSegment(schema);
    segment->LoadJsonStats(json_fid, stats);
    ASSERT_FALSE(segment->HasFieldData(json_fid));

    for (const auto op : {
             proto::plan::JSONContainsExpr_JSONOp_ContainsAny,
             proto::plan::JSONContainsExpr_JSONOp_ContainsAll,
         }) {
        auto contains_expr = std::make_shared<expr::JsonContainsExpr>(
            expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
            op,
            true,
            std::vector<proto::plan::GenericValue>{});

        ExprBatchSizeGuard batch_size_guard(3);
        auto batches = EvalExprInBatches(
            contains_expr, segment.get(), json_raw_data.size());
        EXPECT_EQ(batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));

        TargetBitmapView values(batches.result->GetRawData(),
                                batches.result->size());
        TargetBitmapView validity(batches.result->GetValidRawData(),
                                  batches.result->size());
        for (size_t i = 0; i < batches.result->size(); ++i) {
            EXPECT_EQ(values[i],
                      op == proto::plan::JSONContainsExpr_JSONOp_ContainsAll)
                << "row " << i;
            EXPECT_TRUE(validity[i]) << "row " << i;
        }
    }
}

TEST(JsonStatsUnaryRangeTest, NotEqualKeepsJsonPathUnknownsAndMasksFieldNull) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);

    auto segment = segcore::CreateSealedSegment(schema);

    std::vector<std::string> json_raw_data = {
        R"({"a": "1"})",    // equal, filtered out
        R"({"a": "123"})",  // string mismatch, kept
        R"({"a": 1})",      // type mismatch for string compare, UNKNOWN
        R"({"b": 1})",      // path missing, UNKNOWN
        R"({"a": null})",   // JSON path null, UNKNOWN
        R"({})",            // path missing, UNKNOWN
        R"({"a": "321"})",  // string mismatch, kept
        R"({"a": "123"})",  // field-level null, filtered out by valid data
    };
    std::vector<uint8_t> valid_data{0b01111111};

    const int64_t collection_id = 1101;
    const int64_t partition_id = 2101;
    const int64_t segment_id = 3101;
    const int64_t field_id = json_fid.get();
    const int64_t build_id = 5101;
    const int64_t version_id = 1;
    const std::string root_path = TestLocalPath;

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          root_path,
                                          collection_id,
                                          partition_id,
                                          segment_id,
                                          field_id,
                                          build_id,
                                          version_id,
                                          &valid_data);
    segment->LoadJsonStats(json_fid, stats);

    auto json_field =
        std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
    json_field->FillFieldData(MakeNullableJsonArray(json_raw_data, valid_data));

    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {json_field}, cm);
    segment->LoadFieldData(load_info);

    proto::plan::GenericValue val;
    val.set_string_val("1");
    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"a"}),
        proto::plan::OpType::NotEqual,
        val,
        std::vector<proto::plan::GenericValue>());
    auto plan =
        std::make_shared<plan::FilterBitsNode>(DEFAULT_PLANNODE_ID, unary_expr);
    auto result = query::ExecuteQueryExpr(
        plan, segment.get(), json_raw_data.size(), MAX_TIMESTAMP);

    ASSERT_EQ(result.size(), json_raw_data.size());
    EXPECT_FALSE(result[0]);
    EXPECT_TRUE(result[1]);
    for (int i = 2; i <= 5; ++i) {
        EXPECT_FALSE(result[i]) << "row " << i;
    }
    EXPECT_TRUE(result[6]);
    EXPECT_FALSE(result[7]);
    EXPECT_EQ(result.count(), 2);
}

TEST(JsonStatsBinaryRangeTest, ShreddingMatchesRawData) {
    auto schema = std::make_shared<Schema>();
    auto json_fid = schema->AddDebugField("json", DataType::JSON, true);
    auto gate_fid = schema->AddDebugField("gate", DataType::INT64);

    const std::vector<std::string> json_raw_data = {
        R"({"n": 1.0, "s": "alpha", "i": 9007199254740992, "u": 9223372036854775809, "rare": 1.0, "sparse_i": 9007199254740992})",
        R"({"n": 2.0, "s": "beta", "i": 9007199254740993, "rare": "not-a-number", "sparse_i": 9007199254740993})",
        R"({"n": 3.5, "s": "gamma", "i": 9007199254740994})",
        R"({"n": "2", "s": 2, "i": "9007199254740993"})",
        R"({"other": 0})",
        R"({"n": null, "s": null, "i": null})",
        R"({"n": 4.0, "s": "delta", "i": 1})",
        R"({"n": 5.0, "s": "epsilon", "i": 2})",
    };
    const std::vector<uint8_t> valid_data{0b01111111};

    auto stats = BuildAndLoadJsonKeyStats(json_raw_data,
                                          json_fid,
                                          TestLocalPath,
                                          1203,
                                          2203,
                                          3203,
                                          json_fid.get(),
                                          5203,
                                          1,
                                          &valid_data);
    auto stats_segment = segcore::CreateSealedSegment(schema);
    stats_segment->LoadJsonStats(json_fid, stats);
    auto pinned_stats = stats_segment->GetJsonStats(nullptr, json_fid);
    ASSERT_NE(pinned_stats.get(), nullptr);
    EXPECT_FALSE(pinned_stats.get()
                     ->GetShreddingField(milvus::index::JsonPointer({"n"}),
                                         JSONType::DOUBLE)
                     .empty());
    EXPECT_FALSE(pinned_stats.get()
                     ->GetShreddingField(milvus::index::JsonPointer({"s"}),
                                         JSONType::STRING)
                     .empty());
    EXPECT_FALSE(pinned_stats.get()
                     ->GetShreddingField(milvus::index::JsonPointer({"i"}),
                                         JSONType::INT64)
                     .empty());
    EXPECT_TRUE(pinned_stats.get()
                    ->GetShreddingField(milvus::index::JsonPointer({"rare"}),
                                        JSONType::DOUBLE)
                    .empty());
    EXPECT_TRUE(pinned_stats.get()
                    ->GetShreddingField(milvus::index::JsonPointer({"rare"}),
                                        JSONType::STRING)
                    .empty());
    EXPECT_TRUE(
        pinned_stats.get()
            ->GetShreddingField(milvus::index::JsonPointer({"sparse_i"}),
                                JSONType::INT64)
            .empty());
    auto raw_segment = segcore::CreateSealedSegment(schema);

    auto make_json_field = [&] {
        auto field =
            std::make_shared<FieldData<milvus::Json>>(DataType::JSON, true);
        field->FillFieldData(MakeNullableJsonArray(json_raw_data, valid_data));
        return field;
    };
    auto cm = milvus::storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    auto stats_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    stats_segment->LoadFieldData(stats_load_info);
    stats_segment->DropFieldData(json_fid);
    ASSERT_FALSE(stats_segment->HasFieldData(json_fid));
    auto raw_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, json_fid.get(), {make_json_field()}, cm);
    raw_segment->LoadFieldData(raw_load_info);

    std::vector<int64_t> gate_values = {0, 1, 2, 3, 4, 5, 6, 7};
    auto gate_field =
        std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
    gate_field->FillFieldData(gate_values.data(), gate_values.size());
    auto gate_load_info = PrepareSingleFieldInsertBinlog(
        0, 0, 0, gate_fid.get(), {gate_field}, cm);
    stats_segment->LoadFieldData(gate_load_info);
    raw_segment->LoadFieldData(gate_load_info);

    auto evaluate = [&](const expr::TypedExprPtr& filter_expr,
                        const segcore::SegmentInternalInterface* segment,
                        exec::OffsetVector* offsets = nullptr) {
        return EvalExprOnce(
            filter_expr, segment, json_raw_data.size(), offsets);
    };
    auto expect_same = [](const ColumnVectorPtr& raw,
                          const ColumnVectorPtr& shredded) {
        ASSERT_EQ(raw->size(), shredded->size());
        TargetBitmapView raw_result(raw->GetRawData(), raw->size());
        TargetBitmapView raw_valid(raw->GetValidRawData(), raw->size());
        TargetBitmapView shredded_result(shredded->GetRawData(),
                                         shredded->size());
        TargetBitmapView shredded_valid(shredded->GetValidRawData(),
                                        shredded->size());
        for (size_t i = 0; i < raw->size(); ++i) {
            EXPECT_EQ(shredded_valid[i], raw_valid[i]) << "row " << i;
            EXPECT_EQ(shredded_result[i], raw_result[i]) << "row " << i;
        }
    };
    auto expect_batched_same = [&](const expr::TypedExprPtr& filter_expr) {
        ExprBatchSizeGuard batch_size_guard(3);
        auto raw_batches = EvalExprInBatches(
            filter_expr, raw_segment.get(), json_raw_data.size());
        auto stats_batches = EvalExprInBatches(
            filter_expr, stats_segment.get(), json_raw_data.size());
        EXPECT_EQ(raw_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        EXPECT_EQ(stats_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        expect_same(raw_batches.result, stats_batches.result);
    };

    proto::plan::GenericValue number_lower;
    number_lower.set_int64_val(2);
    proto::plan::GenericValue number_upper;
    number_upper.set_float_val(4.0);
    auto number_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        number_lower,
        number_upper,
        true,
        true);
    expect_same(evaluate(number_expr, raw_segment.get()),
                evaluate(number_expr, stats_segment.get()));
    exec::OffsetVector offsets = {7, 2, 4, 1, 3, 5, 6, 0, 2};
    expect_same(evaluate(number_expr, raw_segment.get(), &offsets),
                evaluate(number_expr, stats_segment.get(), &offsets));
    expect_batched_same(number_expr);

    auto unary_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"n"}),
        proto::plan::OpType::GreaterEqual,
        number_lower,
        std::vector<proto::plan::GenericValue>());
    {
        ExprBatchSizeGuard batch_size_guard(3);
        auto raw_batches = EvalExprInBatches(
            unary_expr, raw_segment.get(), json_raw_data.size());
        auto stats_batches = EvalExprInBatches(
            unary_expr, stats_segment.get(), json_raw_data.size());
        EXPECT_EQ(raw_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        EXPECT_EQ(stats_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        expect_same(raw_batches.result, stats_batches.result);
    }

    proto::plan::GenericValue gate_lower;
    gate_lower.set_int64_val(3);
    auto gate_expr = std::make_shared<expr::UnaryRangeFilterExpr>(
        expr::ColumnInfo(gate_fid, DataType::INT64),
        proto::plan::OpType::GreaterEqual,
        gate_lower,
        std::vector<proto::plan::GenericValue>());
    auto conjunct_expr = std::make_shared<expr::LogicalBinaryExpr>(
        expr::LogicalBinaryExpr::OpType::And, gate_expr, unary_expr);
    {
        ExprBatchSizeGuard batch_size_guard(3);
        auto raw_batches = EvalExprInBatches(
            conjunct_expr, raw_segment.get(), json_raw_data.size());
        auto stats_batches = EvalExprInBatches(
            conjunct_expr, stats_segment.get(), json_raw_data.size());
        EXPECT_EQ(raw_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        EXPECT_EQ(stats_batches.batch_sizes, (std::vector<int64_t>{3, 3, 2}));
        expect_same(raw_batches.result, stats_batches.result);
    }

    proto::plan::GenericValue string_lower;
    string_lower.set_string_val("beta");
    proto::plan::GenericValue string_upper;
    string_upper.set_string_val("gamma");
    auto string_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"s"}),
        string_lower,
        string_upper,
        true,
        false);
    expect_same(evaluate(string_expr, raw_segment.get()),
                evaluate(string_expr, stats_segment.get()));
    expect_same(evaluate(string_expr, raw_segment.get(), &offsets),
                evaluate(string_expr, stats_segment.get(), &offsets));
    expect_batched_same(string_expr);

    proto::plan::GenericValue precise_lower;
    precise_lower.set_float_val(9007199254740992.0);
    proto::plan::GenericValue precise_upper;
    precise_upper.set_float_val(9007199254740994.0);
    auto precise_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"i"}),
        precise_lower,
        precise_upper,
        false,
        false);
    auto raw_precise = evaluate(precise_expr, raw_segment.get());
    auto shredded_precise = evaluate(precise_expr, stats_segment.get());
    expect_same(raw_precise, shredded_precise);
    expect_same(evaluate(precise_expr, raw_segment.get(), &offsets),
                evaluate(precise_expr, stats_segment.get(), &offsets));
    TargetBitmapView precise_result(raw_precise->GetRawData(),
                                    raw_precise->size());
    TargetBitmapView precise_valid(raw_precise->GetValidRawData(),
                                   raw_precise->size());
    EXPECT_TRUE(precise_valid[1]);
    EXPECT_TRUE(precise_result[1]);

    proto::plan::GenericValue sparse_lower;
    sparse_lower.set_float_val(9007199254740992.0);
    proto::plan::GenericValue sparse_upper;
    sparse_upper.set_float_val(9007199254740994.0);
    auto sparse_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"sparse_i"}),
        sparse_lower,
        sparse_upper,
        false,
        false);
    auto raw_sparse = evaluate(sparse_expr, raw_segment.get());
    auto stats_sparse = evaluate(sparse_expr, stats_segment.get());
    expect_same(raw_sparse, stats_sparse);
    expect_same(evaluate(sparse_expr, raw_segment.get(), &offsets),
                evaluate(sparse_expr, stats_segment.get(), &offsets));
    expect_batched_same(sparse_expr);
    TargetBitmapView sparse_result(raw_sparse->GetRawData(),
                                   raw_sparse->size());
    TargetBitmapView sparse_valid(raw_sparse->GetValidRawData(),
                                  raw_sparse->size());
    for (size_t i = 0; i < raw_sparse->size(); ++i) {
        EXPECT_EQ(sparse_valid[i], i < 2) << "row " << i;
        EXPECT_EQ(sparse_result[i], i == 1) << "row " << i;
    }

    proto::plan::GenericValue uint64_double;
    uint64_double.set_float_val(9223372036854775808.0);
    auto uint64_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
        expr::ColumnInfo(json_fid, DataType::JSON, {"u"}),
        uint64_double,
        uint64_double,
        true,
        true);
    auto raw_uint64 = evaluate(uint64_expr, raw_segment.get());
    auto stats_uint64 = evaluate(uint64_expr, stats_segment.get());
    expect_same(raw_uint64, stats_uint64);
    expect_same(evaluate(uint64_expr, raw_segment.get(), &offsets),
                evaluate(uint64_expr, stats_segment.get(), &offsets));
    TargetBitmapView uint64_result(raw_uint64->GetRawData(),
                                   raw_uint64->size());
    TargetBitmapView uint64_valid(raw_uint64->GetValidRawData(),
                                  raw_uint64->size());
    EXPECT_TRUE(uint64_valid[0]);
    EXPECT_TRUE(uint64_result[0]);

    proto::plan::GenericValue nan_lower;
    nan_lower.set_float_val(std::numeric_limits<double>::quiet_NaN());
    proto::plan::GenericValue nan_upper;
    nan_upper.set_float_val(10.0);
    auto expect_nan_range_matches_raw =
        [&](const std::string& path,
            const std::vector<size_t>& known_numeric_rows) {
            auto nan_expr = std::make_shared<expr::BinaryRangeFilterExpr>(
                expr::ColumnInfo(json_fid, DataType::JSON, {path}),
                nan_lower,
                nan_upper,
                true,
                true);
            auto raw_nan = evaluate(nan_expr, raw_segment.get());
            auto stats_nan = evaluate(nan_expr, stats_segment.get());
            expect_same(raw_nan, stats_nan);

            TargetBitmapView result(raw_nan->GetRawData(), raw_nan->size());
            TargetBitmapView valid(raw_nan->GetValidRawData(), raw_nan->size());
            for (size_t i = 0; i < raw_nan->size(); ++i) {
                EXPECT_FALSE(result[i]) << "path " << path << ", row " << i;
                const auto is_known_numeric =
                    std::find(known_numeric_rows.begin(),
                              known_numeric_rows.end(),
                              i) != known_numeric_rows.end();
                EXPECT_EQ(valid[i], is_known_numeric)
                    << "path " << path << ", row " << i;
            }
        };
    expect_nan_range_matches_raw("n", {0, 1, 2, 6});
    expect_nan_range_matches_raw("rare", {0});
}
