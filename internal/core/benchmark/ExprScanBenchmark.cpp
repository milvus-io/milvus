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

// Filter hot-path benchmark: BinaryRange / Term / Unary / JsonContains /
// BinaryArith x sealed / growing x nullable / non-nullable x with / without
// bitmap_input.
// Rows per segment: MILVUS_EXPR_BENCH_ROWS (comma list, default 1000000).

#include <benchmark/benchmark.h>
#include <fmt/core.h>
#include <folly/init/Init.h>

#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <tuple>
#include <unistd.h>
#include <vector>

#include "cachinglayer/Manager.h"
#include "common/Common.h"
#include "common/Consts.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "common/common_type_c.h"
#include "exec/QueryContext.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/function/init_c.h"
#include "expr/ITypeExpr.h"
#include "pb/plan.pb.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/Utils.h"
#include "segcore/arrow_fs_c.h"
#include "storage/LocalChunkManagerSingleton.h"
#include "storage/MmapManager.h"
#include "storage/RemoteChunkManagerSingleton.h"
#include "test_utils/Constants.h"
#include "test_utils/DataGen.h"
#include "test_utils/storage_test_utils.h"

// Declared extern in test_utils/Constants.h; all_tests defines them in
// init_gtest.cpp.
std::string TestLocalPath;
std::string TestRemotePath;
std::string TestMmapPath;

namespace {

using namespace milvus;
using namespace milvus::segcore;

enum class ExprKind { BinaryRange, Term, Unary, JsonContains, BinaryArith };

struct BenchSegment {
    SchemaPtr schema;
    FieldId pk, i64, json;
    std::unique_ptr<SegmentInternalInterface> segment;
};

int64_t
ValueOf(int64_t row) {
    return static_cast<int64_t>((static_cast<uint64_t>(row) * 2654435761ULL) %
                                1000ULL);
}

bool
CandidateBit(int64_t row) {
    return ((static_cast<uint64_t>(row) * 0x9E3779B97F4A7C15ULL) >> 63) != 0;
}

GeneratedData
MakeBenchDataset(const BenchSegment& seg, int64_t n, bool nullable) {
    std::vector<int64_t> pk(n), values(n);
    std::vector<std::string> jsons(n);
    FixedVector<bool> valid(n);
    for (int64_t i = 0; i < n; ++i) {
        pk[i] = i;
        values[i] = ValueOf(i);
        valid[i] = !nullable || i % 5 != 0;  // 20% NULL when nullable
        jsons[i] = fmt::format(R"({{"a":{},"arr":[{},{},{}]}})",
                               values[i],
                               values[i] % 7,
                               values[i] % 11,
                               values[i] % 13);
    }
    auto record = std::make_unique<InsertRecordProto>();
    record->set_num_rows(n);
    auto add = [&](FieldId field_id, const void* data, const bool* validity) {
        auto array =
            CreateDataArrayFrom(data, validity, n, (*seg.schema)[field_id]);
        record->mutable_fields_data()->AddAllocated(array.release());
    };
    add(seg.pk, pk.data(), nullptr);
    add(seg.i64, values.data(), nullable ? valid.data() : nullptr);
    add(seg.json, jsons.data(), nullable ? valid.data() : nullptr);

    GeneratedData dataset;
    dataset.schema_ = seg.schema;
    dataset.raw_ = record.release();
    dataset.row_ids_.assign(pk.begin(), pk.end());
    dataset.timestamps_.assign(n, 1);
    return dataset;
}

// PrepareSingleFieldInsertBinlog puts binlogs under a per-segment path, so two
// cached fixtures never share remote files.
std::unique_ptr<SegmentSealed>
LoadSealed(const SchemaPtr& schema,
           const GeneratedData& dataset,
           int64_t segment_id) {
    const auto row_count = static_cast<int64_t>(dataset.row_ids_.size());
    auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                  .GetRemoteChunkManager();
    LoadFieldDataInfo load_info;
    auto add_field = [&](int64_t field_id, FieldDataPtr data) {
        auto info = PrepareSingleFieldInsertBinlog(
            kCollectionID, kPartitionID, segment_id, field_id, {data}, cm);
        load_info.field_infos.merge(info.field_infos);
    };
    auto row_ids = std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
    row_ids->FillFieldData(dataset.row_ids_.data(), row_count);
    add_field(RowFieldID.get(), row_ids);
    auto timestamps =
        std::make_shared<FieldData<int64_t>>(DataType::INT64, false);
    timestamps->FillFieldData(dataset.timestamps_.data(), row_count);
    add_field(TimestampFieldID.get(), timestamps);
    const auto fields = schema->get_fields();
    for (const auto& data : dataset.raw_->fields_data()) {
        add_field(data.field_id(),
                  CreateFieldDataFromDataArray(
                      row_count, &data, fields.at(FieldId(data.field_id()))));
    }
    auto segment = CreateSealedSegment(schema, empty_index_meta, segment_id);
    segment->LoadFieldData(load_info);
    return segment;
}

std::unique_ptr<BenchSegment>
BuildBenchSegment(int64_t n, bool nullable, bool sealed) {
    static int64_t next_segment_id = 700000;
    auto seg = std::make_unique<BenchSegment>();
    seg->schema = std::make_shared<Schema>();
    seg->pk = seg->schema->AddDebugField("pk", DataType::INT64);
    seg->i64 = seg->schema->AddDebugField("i64", DataType::INT64, nullable);
    seg->json = seg->schema->AddDebugField("json", DataType::JSON, nullable);
    seg->schema->set_primary_field_id(seg->pk);

    auto dataset = MakeBenchDataset(*seg, n, nullable);
    if (sealed) {
        seg->segment = LoadSealed(seg->schema, dataset, next_segment_id++);
    } else {
        SegmentGrowingPtr growing =
            CreateGrowingSegment(seg->schema, empty_index_meta);
        growing->PreInsert(n);
        growing->Insert(0,
                        n,
                        dataset.row_ids_.data(),
                        dataset.timestamps_.data(),
                        dataset.raw_);
        seg->segment = std::move(growing);
    }
    return seg;
}

std::map<std::tuple<int64_t, bool, bool>, std::unique_ptr<BenchSegment>>&
BenchCache() {
    static std::map<std::tuple<int64_t, bool, bool>,
                    std::unique_ptr<BenchSegment>>
        cache;
    return cache;
}

const BenchSegment&
GetBenchSegment(int64_t n, bool nullable, bool sealed) {
    static std::mutex mutex;
    std::lock_guard<std::mutex> guard(mutex);
    auto& slot = BenchCache()[{n, nullable, sealed}];
    if (!slot) {
        slot = BuildBenchSegment(n, nullable, sealed);
    }
    return *slot;
}

proto::plan::GenericValue
I64Val(int64_t v) {
    proto::plan::GenericValue value;
    value.set_int64_val(v);
    return value;
}

expr::TypedExprPtr
MakeExpr(ExprKind kind, const BenchSegment& seg, bool nullable) {
    const expr::ColumnInfo i64_col(seg.i64, DataType::INT64, {}, nullable);
    switch (kind) {
        case ExprKind::BinaryRange:  // ~50% selectivity
            return std::make_shared<expr::BinaryRangeFilterExpr>(
                i64_col, I64Val(100), I64Val(600), true, false);
        case ExprKind::Term: {
            std::vector<proto::plan::GenericValue> vals;
            for (int64_t v : {1, 7, 42, 99, 250, 500, 777, 999}) {
                vals.push_back(I64Val(v));
            }
            return std::make_shared<expr::TermFilterExpr>(i64_col, vals);
        }
        case ExprKind::Unary:
            return std::make_shared<expr::UnaryRangeFilterExpr>(
                i64_col, proto::plan::OpType::LessThan, I64Val(500));
        case ExprKind::JsonContains:
            return std::make_shared<expr::JsonContainsExpr>(
                expr::ColumnInfo(seg.json, DataType::JSON, {"arr"}, nullable),
                proto::plan::JSONContainsExpr_JSONOp_Contains,
                true,
                std::vector<proto::plan::GenericValue>{I64Val(3)});
        case ExprKind::BinaryArith:  // i64 % 7 == 3
            return std::make_shared<expr::BinaryArithOpEvalRangeExpr>(
                i64_col,
                proto::plan::OpType::Equal,
                proto::plan::ArithOpType::Mod,
                I64Val(3),
                I64Val(7));
    }
    return nullptr;
}

void
RunExprScan(benchmark::State& state,
            ExprKind kind,
            bool sealed,
            bool nullable,
            bool with_bitmap) {
    const int64_t n = state.range(0);
    const auto& seg = GetBenchSegment(n, nullable, sealed);
    const auto logical = MakeExpr(kind, seg, nullable);
    const int64_t batch_size = EXEC_EVAL_EXPR_BATCH_SIZE.load();
    TargetBitmap pattern;
    if (with_bitmap) {
        pattern = TargetBitmap(n, false);
        for (int64_t i = 0; i < n; ++i) {
            if (CandidateBit(i)) {
                pattern.set(i);
            }
        }
    }

    // One full pass over the segment. When `matched` is non-null, the pass also
    // counts rows that are candidates and (match & known); the timed loop
    // passes nullptr so counting stays out of the measured region.
    auto run_pass = [&](int64_t* matched) -> bool {
        auto query_context = std::make_shared<exec::QueryContext>(
            DEAFULT_QUERY_ID, seg.segment.get(), n, MAX_TIMESTAMP);
        exec::ExecContext exec_context(query_context.get());
        auto compiled =
            exec::CompileExpressions({logical}, &exec_context, {}, false);
        exec::EvalCtx eval_ctx(&exec_context);
        int64_t processed = 0;
        while (processed < n) {
            const int64_t expected = std::min(batch_size, n - processed);
            TargetBitmap active;
            if (with_bitmap) {
                // A per-batch candidate bitmap, as a conjunction hands to its
                // children.
                active.append(pattern, processed, expected);
                if (matched != nullptr) {
                    eval_ctx.set_bitmap_input(active.clone());
                } else {
                    eval_ctx.set_bitmap_input(std::move(active));
                }
            }
            VectorPtr result;
            compiled[0]->Eval(eval_ctx, result);
            auto column = std::static_pointer_cast<ColumnVector>(result);
            if (column == nullptr ||
                static_cast<int64_t>(column->size()) != expected) {
                state.SkipWithError("unexpected expression batch size");
                return false;
            }
            if (matched != nullptr) {
                TargetBitmapView match(column->GetRawData(), column->size());
                TargetBitmapView known(column->GetValidRawData(),
                                       column->size());
                for (int64_t i = 0; i < expected; ++i) {
                    if ((!with_bitmap || active[i]) && match[i] && known[i]) {
                        ++*matched;
                    }
                }
            }
            eval_ctx.clear_bitmap_input();
            processed += expected;
        }
        return true;
    };

    for (auto _ : state) {
        if (!run_pass(nullptr)) {
            return;
        }
    }
    // Untimed counting pass. Base and head are expected to report the same
    // matched count; compare_expr_bench.py fails on a mismatch, which must be
    // explained in the PR.
    int64_t matched = 0;
    if (!run_pass(&matched)) {
        return;
    }
    state.counters["matched"] = static_cast<double>(matched);
    state.SetItemsProcessed(state.iterations() * n);
}

std::vector<int64_t>
BenchRows() {
    std::vector<int64_t> rows;
    const char* env = std::getenv("MILVUS_EXPR_BENCH_ROWS");
    std::stringstream in(env != nullptr && *env != '\0' ? env : "1000000");
    std::string item;
    while (std::getline(in, item, ',')) {
        rows.push_back(std::stoll(item));
    }
    return rows;
}

void
RegisterExprScanBenchmarks() {
    const std::vector<std::pair<ExprKind, const char*>> kinds = {
        {ExprKind::BinaryRange, "BinaryRange"},
        {ExprKind::Term, "Term"},
        {ExprKind::Unary, "Unary"},
        {ExprKind::JsonContains, "JsonContains"},
        {ExprKind::BinaryArith, "BinaryArith"},
    };
    const auto rows = BenchRows();
    for (const auto& [kind, kind_name] : kinds) {
        for (bool sealed : {false, true}) {
            for (bool nullable : {false, true}) {
                for (bool with_bitmap : {false, true}) {
                    const auto name =
                        fmt::format("ExprScan/{}/{}/{}/{}",
                                    kind_name,
                                    sealed ? "sealed" : "growing",
                                    nullable ? "nullable" : "non_nullable",
                                    with_bitmap ? "bitmap_input" : "no_bitmap");
                    const ExprKind k = kind;
                    auto* bench = benchmark::RegisterBenchmark(
                        name.c_str(),
                        [k, sealed, nullable, with_bitmap](
                            benchmark::State& state) {
                            RunExprScan(
                                state, k, sealed, nullable, with_bitmap);
                        });
                    for (auto n : rows) {
                        bench->Arg(n);
                    }
                    bench->Unit(benchmark::kMillisecond);
                }
            }
        }
    }
}

// Mirrors the main() of all_tests in init_gtest.cpp: test paths, storage
// singletons, arrow filesystem and caching layer.
void
InitBenchmarkEnvironment() {
    const char* env = std::getenv("MILVUS_EXPR_BENCH_DIR");
    const std::string base =
        env != nullptr && *env != '\0'
            ? std::string(env)
            : (std::filesystem::temp_directory_path() /
               fmt::format("milvus_expr_scan_bench_{}", getpid()))
                  .string();
    TestLocalPath = base + "/local_data/";
    TestRemotePath = base + "/remote_data/";
    TestMmapPath = base + "/mmap_data/";
    for (const auto& dir : {TestLocalPath, TestRemotePath, TestMmapPath}) {
        std::filesystem::create_directories(dir);
    }

    InitExecExpressionFunctionFactory();
    milvus::storage::LocalChunkManagerSingleton::GetInstance().Init(
        TestLocalPath);
    milvus::storage::RemoteChunkManagerSingleton::GetInstance().Init(
        get_default_local_storage_config());
    milvus::storage::MmapManager::GetInstance().Init(get_default_mmap_config());

    CStorageConfig arrow_fs_config = {};
    arrow_fs_config.root_path = TestLocalPath.c_str();
    arrow_fs_config.storage_type = "local";
    auto status = InitArrowFileSystem(arrow_fs_config);
    AssertInfo(status.error_code == 0, "failed to init arrow filesystem");

    static const int64_t mb = 1024 * 1024;
    milvus::cachinglayer::Manager::ConfigureTieredStorage(
        {CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable,
         CacheWarmupPolicy::CacheWarmupPolicy_Disable},
        {1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb, 1024 * mb},
        true,
        true,
        {10, true, 30},
        std::chrono::milliseconds(0),
        std::chrono::milliseconds(-1));
}

}  // namespace

int
main(int argc, char** argv) {
    // Strip --benchmark_* flags before gflags (folly::Init) sees argv.
    ::benchmark::Initialize(&argc, argv);
    if (::benchmark::ReportUnrecognizedArguments(argc, argv)) {
        return 1;
    }
    folly::Init folly_init(&argc, &argv, false);
    InitBenchmarkEnvironment();
    RegisterExprScanBenchmarks();
    ::benchmark::RunSpecifiedBenchmarks();
    BenchCache().clear();  // release segments before singletons go away
    ::benchmark::Shutdown();
    return 0;
}
