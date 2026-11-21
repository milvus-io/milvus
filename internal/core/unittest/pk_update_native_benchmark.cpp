// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <algorithm>
#include <array>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <limits>
#include <map>
#include <numeric>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>

#if defined(__APPLE__)
#include <mach/mach.h>
#elif defined(__linux__)
#include <unistd.h>
#endif

#include "common/Utils.h"
#include "exec/expression/ExprCache.h"
#include "query/PlanProto.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "segcore/UpdatesRecord.h"
#ifdef MILVUS_ENABLE_PK_UPDATE_NATIVE_BENCHMARK
#include "segcore/PkUpdateReadObserver.h"
#endif
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

namespace milvus::segcore {
namespace {

using Json = nlohmann::json;
using Plan = std::unique_ptr<query::RetrievePlan>;
using Result = proto::segcore::RetrieveResults;

constexpr int64_t kLimit = 1000;
constexpr int64_t kFirstPk = 2000000;

#ifdef MILVUS_ENABLE_PK_UPDATE_NATIVE_BENCHMARK
using Clock = std::chrono::steady_clock;

constexpr size_t kPlans = 100;
constexpr size_t kIterations = 1000;
constexpr size_t kRows = 1000000;

double
ElapsedMs(Clock::time_point start, Clock::time_point finish = Clock::now()) {
    return std::chrono::duration<double, std::milli>(finish - start).count();
}

uint64_t
ResidentBytes() {
#if defined(__APPLE__)
    mach_task_basic_info_data_t info{};
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(),
                  MACH_TASK_BASIC_INFO,
                  reinterpret_cast<task_info_t>(&info),
                  &count) == KERN_SUCCESS)
        return info.resident_size;
#elif defined(__linux__)
    uint64_t total, resident;
    if (std::ifstream("/proc/self/statm") >> total >> resident)
        return resident * sysconf(_SC_PAGESIZE);
#endif
    return 0;
}

Json
ReadJson(const std::filesystem::path& path) {
    std::ifstream input(path);
    return Json::parse(input);
}
#endif

struct Columns {
    std::vector<int64_t> ips;
    std::vector<std::string> urls;
};

#ifdef MILVUS_ENABLE_PK_UPDATE_NATIVE_BENCHMARK
struct FieldWork {
    uint64_t raw_batches = 0;
    uint64_t raw_rows = 0;
    uint64_t index_calls = 0;
    uint64_t reverse_calls = 0;
    uint64_t reverse_scanned = 0;
    uint64_t reverse_selected = 0;
    uint64_t visited_reads = 0;
    uint64_t visited_bitmap_bits = 0;
    uint64_t overlay_calls = 0;
    uint64_t overlay_batches = 0;
    uint64_t overlay_input_rows = 0;
    uint64_t overlay_selected = 0;
    uint64_t dictionary_probes = 0;

    void
    Observe(const PkUpdateReadEvent& event) {
        switch (event.path) {
            case PkUpdateReadPath::RawPredicate:
                ++raw_batches;
                raw_rows += event.rows;
                break;
            case PkUpdateReadPath::IndexedPredicate:
                ++index_calls;
                break;
            case PkUpdateReadPath::Reverse:
                ++reverse_calls;
                reverse_scanned += event.updates.scanned_entries;
                reverse_selected += event.updates.selected_offsets;
                visited_reads += event.updates.used_visited;
                visited_bitmap_bits += event.visited_bitmap_bits;
                break;
            case PkUpdateReadPath::Overlay:
                ++overlay_calls;
                overlay_input_rows += event.rows;
                overlay_selected += event.updates.selected_offsets;
                overlay_batches += event.updates.selected_offsets > 0;
                dictionary_probes += event.updates.dictionary_probes;
                break;
        }
    }

    Json
    Report() const {
        return {{"raw_batches", raw_batches},
                {"raw_rows", raw_rows},
                {"index_calls", index_calls},
                {"reverse_calls", reverse_calls},
                {"reverse_scanned", reverse_scanned},
                {"reverse_selected", reverse_selected},
                {"visited_reads", visited_reads},
                {"visited_bitmap_bits", visited_bitmap_bits},
                {"overlay_calls", overlay_calls},
                {"overlay_batches", overlay_batches},
                {"overlay_input_rows", overlay_input_rows},
                {"overlay_selected", overlay_selected},
                {"dictionary_probes", dictionary_probes}};
    }
};

struct PredicateWork {
    FieldId ip;
    FieldId url;
    FieldWork ip_work;
    FieldWork url_work;
};

thread_local PredicateWork* current_work = nullptr;

class MeasurePredicateWork {
 public:
    explicit MeasurePredicateWork(PredicateWork& work) {
        current_work = &work;
        pk_update_read_observer = [](FieldId field,
                                     const PkUpdateReadEvent& event) {
            if (field.get() == current_work->ip.get())
                current_work->ip_work.Observe(event);
            else if (field.get() == current_work->url.get())
                current_work->url_work.Observe(event);
        };
    }

    MeasurePredicateWork(const MeasurePredicateWork&) = delete;
    MeasurePredicateWork&
    operator=(const MeasurePredicateWork&) = delete;

    ~MeasurePredicateWork() {
        pk_update_read_observer = nullptr;
        current_work = nullptr;
    }
};
#endif

class PkUpdateNativeBenchmark : public ::testing::Test {
 protected:
    void
    SetUp() override {
        cache_ = exec::ExprResCacheManager::IsEnabled();
        exec::ExprResCacheManager::SetEnabled(false);
        prefetch_ = ENABLE_DRIVER_PREFETCH.exchange(false);
        batch_size_ = EXEC_EVAL_EXPR_BATCH_SIZE.exchange(8192);
    }

    void
    TearDown() override {
        EXEC_EVAL_EXPR_BATCH_SIZE.store(batch_size_);
        ENABLE_DRIVER_PREFETCH.store(prefetch_);
        exec::ExprResCacheManager::SetEnabled(cache_);
    }

    void
    InitSchema(bool nullable = false) {
        schema_ = std::make_shared<Schema>();
        pk_ = schema_->AddDebugField("pk", DataType::INT64);
        ip_ = schema_->AddDebugField("ClientIP", DataType::INT64, nullable);
        std::map<std::string, std::string> params;
        url_ = schema_->AddDebugVarcharField(FieldName("URL"),
                                             DataType::VARCHAR,
                                             65535,
                                             nullable,
                                             false,
                                             false,
                                             params,
                                             std::nullopt);
        schema_->set_primary_field_id(pk_);
    }

    GeneratedData
    MakeData(const Columns& values, const std::vector<int64_t>& keys) const {
        auto raw = std::make_unique<InsertRecordProto>();
        raw->set_num_rows(keys.size());
        for (const auto field : {pk_, ip_, url_}) {
            auto* array = raw->add_fields_data();
            array->set_field_id(field.get());
            array->set_type(static_cast<proto::schema::DataType>(
                (*schema_)[field].get_data_type()));
            if ((*schema_)[field].is_nullable()) {
                array->mutable_valid_data()->Resize(keys.size(), true);
            }
            if (field.get() == url_.get()) {
                auto* data = array->mutable_scalars()
                                 ->mutable_string_data()
                                 ->mutable_data();
                data->Reserve(keys.size());
                for (const auto& url : values.urls) *data->Add() = url;
            } else {
                const auto& ints = field.get() == pk_.get() ? keys : values.ips;
                array->mutable_scalars()
                    ->mutable_long_data()
                    ->mutable_data()
                    ->Add(ints.begin(), ints.end());
            }
        }
        GeneratedData data(std::move(raw));
        data.schema_ = schema_;
        data.row_ids_.resize(keys.size());
        std::iota(data.row_ids_.begin(), data.row_ids_.end(), 0);
        data.timestamps_.assign(keys.size(), 1);
        return data;
    }

    template <typename T>
    void
    LoadSort(ChunkedSegmentSealedImpl& target,
             FieldId field,
             const std::vector<T>& values) const {
        auto built = BuildTestScalarIndex<T>(
            index::families::kSort, values.size(), values.data());
        LoadIndexInfo info{};
        info.field_id = field.get();
        info.field_type = (*schema_)[field].get_data_type();
        info.index_params = GenIndexParams(built.get());
        SetTestIndexMetadata(info, *built, index::families::kSort);
        info.index = CreateTestIndex("pk-update-native", std::move(built));
        target.LoadIndex(info);
    }

    void
    Load(const Columns& values,
         const std::vector<int64_t>& keys,
         bool indexed) {
        const auto id = ++segment_id_;
        auto sealed = CreateSealedSegment(schema_, nullptr, id);
        auto* physical = dynamic_cast<ChunkedSegmentSealedImpl*>(sealed.get());
        ASSERT_NE(physical, nullptr);
        const auto path = TestRemotePath + std::to_string(id) + "/";
        std::filesystem::create_directories(path);
        auto cm = storage::RemoteChunkManagerSingleton::GetInstance()
                      .GetRemoteChunkManager();
        auto data = MakeData(values, keys);
        const auto binlogs = PrepareInsertBinlog(
            kCollectionID,
            kPartitionID,
            id,
            data,
            cm,
            "",
            indexed ? std::vector<int64_t>{ip_.get(), url_.get()}
                    : std::vector<int64_t>{},
            path);
        physical->LoadFieldData(binlogs);
        if (indexed) {
            LoadSort(*physical, ip_, values.ips);
            LoadSort(*physical, url_, values.urls);
        }
        for (auto field : {ip_, url_}) {
            ASSERT_EQ(physical->HasFieldData(field), !indexed);
            ASSERT_EQ(physical->HasIndex(field), indexed);
            ASSERT_EQ(physical->IndexHasRawData(field), indexed);
        }
        ASSERT_EQ(physical->get_row_count(), keys.size());
        segment_ = std::move(sealed);
    }

    void
    Apply(int64_t pk,
          Timestamp ts,
          uint32_t ordinal,
          std::optional<int64_t> ip,
          const std::optional<std::string>& url) {
        IdArray ids;
        ids.mutable_int_id()->add_data(pk);
        UpdateAssignment ip_assignment{ip_, {}};
        UpdateAssignment url_assignment{url_, {}};
        if (ip)
            ip_assignment.literal.mutable_scalar_value()->set_long_data(*ip);
        else
            ip_assignment.literal.set_null_value(google::protobuf::NULL_VALUE);
        if (url)
            url_assignment.literal.mutable_scalar_value()->set_string_data(
                *url);
        else
            url_assignment.literal.set_null_value(google::protobuf::NULL_VALUE);
        segment_->ApplyUpdates(
            ids, ts, ordinal, {ip_assignment, url_assignment});
    }

    void
    Column(proto::plan::ColumnInfo* column, FieldId field) const {
        column->set_field_id(field.get());
        column->set_data_type(static_cast<proto::schema::DataType>(
            (*schema_)[field].get_data_type()));
        column->set_nullable((*schema_)[field].is_nullable());
    }

    proto::plan::Expr
    Conjunction(const Json& params) const {
        proto::plan::Expr expr;
        auto* conjunction = expr.mutable_binary_expr();
        conjunction->set_op(proto::plan::BinaryExpr::LogicalAnd);
        auto* bounds = conjunction->mutable_left()->mutable_binary_range_expr();
        Column(bounds->mutable_column_info(), ip_);
        bounds->set_lower_inclusive(true);
        bounds->set_upper_inclusive(false);
        bounds->mutable_lower_value()->set_int64_val(params.at("lower"));
        bounds->mutable_upper_value()->set_int64_val(params.at("upper"));
        auto* term = conjunction->mutable_right()->mutable_term_expr();
        Column(term->mutable_column_info(), url_);
        for (const auto& url : params.at("urls"))
            term->add_values()->set_string_val(url.get<std::string>());
        return expr;
    }

    proto::plan::Expr
    KeyEquality(int64_t key) const {
        proto::plan::Expr expr;
        auto* equals = expr.mutable_unary_range_expr();
        Column(equals->mutable_column_info(), pk_);
        equals->set_op(proto::plan::Equal);
        equals->mutable_value()->set_int64_val(key);
        return expr;
    }

    Plan
    MakePlan(const proto::plan::Expr& expr,
             const std::vector<FieldId>& output = {}) const {
        proto::plan::PlanNode proto;
        *proto.mutable_query()->mutable_predicates() = expr;
        proto.mutable_query()->set_limit(kLimit);
        for (auto field : output) proto.add_output_field_ids(field.get());
        return query::ProtoParser(schema_).CreateRetrievePlan(proto);
    }

    std::unique_ptr<Result>
    Retrieve(const Plan& plan, Timestamp ts) const {
        return segment_->Retrieve(nullptr,
                                  plan.get(),
                                  ts,
                                  std::numeric_limits<int64_t>::max(),
                                  false);
    }

    static std::vector<int64_t>
    ExpectedOffsets(const Json& rows) {
        std::vector<int64_t> offsets;
        for (const auto& row : rows) offsets.push_back(row.at(0));
        return offsets;
    }

    void
    VerifyOffsets(const Result& result, const std::vector<int64_t>& offsets) {
        ASSERT_EQ(result.offset_size(), offsets.size());
        ASSERT_TRUE(std::equal(
            offsets.begin(), offsets.end(), result.offset().begin()));
    }

    void
    VerifyProjected(const Result& result, const Json& rows, bool keys) {
        const DataArray* ips = nullptr;
        const DataArray* urls = nullptr;
        for (const auto& column : result.fields_data()) {
            if (column.field_id() == ip_.get())
                ips = &column;
            if (column.field_id() == url_.get())
                urls = &column;
        }
        ASSERT_NE(ips, nullptr);
        ASSERT_NE(urls, nullptr);
        if (keys)
            ASSERT_EQ(result.ids().int_id().data_size(), rows.size());
        const auto& ip_validity = GetFieldDataRowValidData(*ips);
        const auto& url_validity = GetFieldDataRowValidData(*urls);
        ASSERT_TRUE(ip_validity.empty() || ip_validity.size() == rows.size());
        ASSERT_TRUE(url_validity.empty() || url_validity.size() == rows.size());
        size_t pos = 0;
        for (const auto& row : rows) {
            if (keys)
                EXPECT_EQ(result.ids().int_id().data(pos),
                          kFirstPk + row.at(0).get<int64_t>());
            const bool ip_valid = ip_validity.empty() || ip_validity.Get(pos);
            const bool url_valid =
                url_validity.empty() || url_validity.Get(pos);
            ASSERT_EQ(ip_valid, !row.at(1).is_null());
            ASSERT_EQ(url_valid, !row.at(2).is_null());
            if (ip_valid)
                EXPECT_EQ(ips->scalars().long_data().data(pos),
                          row.at(1).get<int64_t>());
            if (url_valid)
                EXPECT_EQ(urls->scalars().string_data().data(pos),
                          row.at(2).get<std::string>());
            ++pos;
        }
    }

#ifdef MILVUS_ENABLE_PK_UPDATE_NATIVE_BENCHMARK
    static Json
    Latency(std::vector<double> samples) {
        auto ordered = samples;
        std::sort(ordered.begin(), ordered.end());
        const auto count = ordered.size();
        auto percentile = [&](size_t p) {
            return ordered[(p * count + 99) / 100 - 1];
        };
        return {
            {"min_ms", ordered.front()},
            {"median_ms", (ordered[(count - 1) / 2] + ordered[count / 2]) / 2},
            {"p95_ms", percentile(95)},
            {"p99_ms", percentile(99)},
            {"max_ms", ordered.back()},
            {"mean_ms",
             std::accumulate(ordered.begin(), ordered.end(), 0.0) / count},
            { "samples_ms",
              std::move(samples) }};
    }

    std::vector<double>
    TimeRetrieve(const std::vector<Plan>& plans,
                 Timestamp ts,
                 const std::vector<std::vector<int64_t>>& expected,
                 size_t field_count,
                 const PredicateWork* work = nullptr) {
        std::vector<double> samples;
        samples.reserve(kIterations);
        for (size_t i = 0; i < kIterations; ++i) {
            const auto query = i % plans.size();
            const auto evaluations = [](const FieldWork& field) {
                return field.raw_batches + field.index_calls;
            };
            const auto ip_before = work ? evaluations(work->ip_work) : 0;
            const auto url_before = work ? evaluations(work->url_work) : 0;
            const auto start = Clock::now();
            const auto result = Retrieve(plans[query], ts);
            const auto stop = Clock::now();
            samples.push_back(ElapsedMs(start, stop));
            if (work) {
                EXPECT_GT(evaluations(work->ip_work), ip_before);
                EXPECT_GT(evaluations(work->url_work), url_before);
            }
            EXPECT_EQ(result->scanned_remote_bytes(), 0);
            EXPECT_EQ(result->fields_data_size(), field_count);
            EXPECT_NO_FATAL_FAILURE(VerifyOffsets(*result, expected[query]));
        }
        return samples;
    }
#endif

    SchemaPtr schema_;
    FieldId pk_{0}, ip_{0}, url_{0};
    std::unique_ptr<SegmentSealed> segment_;
    int64_t segment_id_ = 41000000;
    bool cache_;
    bool prefetch_;
    int64_t batch_size_;
};

TEST_F(PkUpdateNativeBenchmark, SnapshotProjection) {
    InitSchema(true);
    // One duplicate PK and one row with a future-only update. A second-phase
    // read also requests a duplicate physical offset.
    ASSERT_NO_FATAL_FAILURE(Load({{10, 20, 30}, {"a", "b", "c"}},
                                 {kFirstPk, kFirstPk, kFirstPk + 2},
                                 false));
    Apply(kFirstPk, 100, 0, 42, "changed");
    Apply(kFirstPk, 102, 0, std::nullopt, std::nullopt);
    Apply(kFirstPk + 2, 103, 0, 99, "future");
    const auto all = MakePlan(KeyEquality(kFirstPk), {pk_, ip_, url_});
    for (Timestamp ts : {99, 100, 101, 102, 103}) {
        SCOPED_TRACE(ts);
        const auto result = Retrieve(all, ts);
        ASSERT_NO_FATAL_FAILURE(VerifyOffsets(*result, {0, 1}));
        ASSERT_EQ(result->ids().int_id().data_size(), 2);
        EXPECT_EQ(result->ids().int_id().data(0), kFirstPk);
        EXPECT_EQ(result->ids().int_id().data(1), kFirstPk);
        const auto rows =
            ts < 100   ? Json::parse(R"([[0,10,"a"],[1,20,"b"]])")
            : ts < 102 ? Json::parse(R"([[0,42,"changed"],[1,42,"changed"]])")
                       : Json::parse(R"([[0,null,null],[1,null,null]])");
        ASSERT_NO_FATAL_FAILURE(VerifyProjected(*result, rows, false));
        const std::array<int64_t, 3> offsets{2, 0, 0};
        const auto duplicate = segment_->Retrieve(
            nullptr, all.get(), offsets.data(), offsets.size(), {}, ts);
        auto expected =
            Json::array({ts < 103 ? Json::parse(R"([2,30,"c"])")
                                  : Json::parse(R"([2,99,"future"])"),
                         rows.at(0),
                         rows.at(0)});
        ASSERT_NO_FATAL_FAILURE(VerifyProjected(*duplicate, expected, false));
    }
}

#ifdef MILVUS_ENABLE_PK_UPDATE_NATIVE_BENCHMARK
TEST_F(PkUpdateNativeBenchmark, DISABLED_ClickBench) {
#ifndef NDEBUG
    FAIL() << "Timing requires a Release or RelWithDebInfo build";
#endif
    const auto* fixture_env = std::getenv("PK_UPDATE_BENCH_FIXTURE");
    const auto* role = std::getenv("PK_UPDATE_BENCH_ROLE");
    const auto* residency = std::getenv("PK_UPDATE_BENCH_RESIDENCY");
    ASSERT_NE(fixture_env, nullptr);
    ASSERT_NE(role, nullptr);
    ASSERT_NE(residency, nullptr);
    ASSERT_TRUE(std::string_view(role) == "candidate" ||
                std::string_view(role) == "mirror");
    ASSERT_TRUE(std::string_view(residency) == "raw" ||
                std::string_view(residency) == "stl_sort");
    const bool updated = std::string_view(role) == "candidate";
    const bool indexed = std::string_view(residency) == "stl_sort";
    const std::filesystem::path fixture(fixture_env);
    const auto manifest = ReadJson(fixture / "manifest.json");
    const auto truth = ReadJson(fixture / "oracle.json");
    const auto& params = truth.at("parameters");
    const auto& snapshots = truth.at("snapshots");
    const auto rounds = manifest.at("update_rounds").get<size_t>();
    const auto distinct = manifest.at("updated_rows").get<size_t>();
    const Timestamp latest = manifest.at("query_ts");
    ASSERT_EQ(params.size(), kPlans);
    ASSERT_EQ(manifest.at("rows"), kRows);
    ASSERT_EQ(manifest.at("limit"), kLimit);
    ASSERT_EQ(snapshots.size(), rounds + 1);
    ASSERT_FALSE(exec::ExprResCacheManager::IsEnabled());
    InitSchema();
    Json report{{"role", role},
                {"residency", residency},
                {"raw_scalar_columns", !indexed},
                {"scalar_index_family", indexed ? "STL_SORT" : ""},
                {"index_reverse_lookup", indexed},
                {"rows", kRows},
                {"query_threads", 1},
                {"expression_cache", false},
                {"parameters", kPlans},
                {"warmup_queries", kPlans},
                {"iterations", kIterations},
                {"limit", kLimit},
                {"expression_output_fields", Json::array()},
                {"query_ts", latest},
                {"update_rounds", rounds},
                {"updated_rows", distinct},
                {"fixture", fixture.string()},
                {"shape", "AND(BinaryRange(ClientIP), Term(URL IN ...))"}};
    report["rss_bytes"]["before_load"] = ResidentBytes();
    const auto base_parse_start = Clock::now();
    {
        Columns values;
        values.ips.reserve(kRows);
        values.urls.reserve(kRows);
        std::ifstream input(fixture /
                            (updated ? "base.jsonl" : "mirror.jsonl"));
        ASSERT_TRUE(input.is_open());
        for (std::string line; std::getline(input, line);) {
            const auto row = Json::parse(line);
            values.ips.push_back(row.at(0));
            values.urls.push_back(row.at(1));
        }
        ASSERT_EQ(values.ips.size(), kRows);
        std::vector<int64_t> keys(kRows);
        std::iota(keys.begin(), keys.end(), kFirstPk);
        report["base_fixture_parse_ms"] = ElapsedMs(base_parse_start);
        const auto load_start = Clock::now();
        ASSERT_NO_FATAL_FAILURE(Load(values, keys, indexed));
        report["load_ms"] = ElapsedMs(load_start);
    }
    report["rss_bytes"]["after_load"] = ResidentBytes();
    std::vector<Json> mutations;
    const auto updates_parse_start = Clock::now();
    if (updated) {
        std::ifstream input(fixture / "updates.jsonl");
        ASSERT_TRUE(input.is_open());
        mutations.reserve(distinct * rounds);
        for (std::string line; std::getline(input, line);)
            mutations.push_back(Json::parse(line));
        ASSERT_EQ(mutations.size(), distinct * rounds);
    }
    report["updates_fixture_parse_ms"] =
        updated ? ElapsedMs(updates_parse_start) : 0;
    const auto apply_start = Clock::now();
    for (const auto& mutation : mutations) {
        Apply(kFirstPk + mutation.at(0).get<int64_t>(),
              mutation.at(1),
              mutation.at(2),
              mutation.at(3).get<int64_t>(),
              mutation.at(4).get<std::string>());
    }
    report["apply_ms"] = updated ? ElapsedMs(apply_start) : 0;
    std::vector<Json>().swap(mutations);
    report["rss_bytes"]["after_apply"] = ResidentBytes();
    const auto& records = segment_->GetUpdatesRecord();
    const auto store = records.StoreStatistics();
    ASSERT_EQ(store.fields, updated ? 2 : 0);
    ASSERT_EQ(store.versions, updated ? distinct * rounds * 2 : 0);
    ASSERT_EQ(store.distinct_field_offsets, updated ? distinct * 2 : 0);
    report["store"] = {{"field_versions", store.versions},
                       {"distinct_field_offsets", store.distinct_field_offsets},
                       {"entry_and_membership_capacity_bytes",
                        store.entry_and_membership_bytes}};

    const auto plan_start = Clock::now();
    std::vector<Plan> filters, projected;
    for (const auto& param : params) {
        const auto expr = Conjunction(param);
        filters.push_back(MakePlan(expr));
        projected.push_back(MakePlan(expr, {pk_, ip_, url_}));
        ASSERT_TRUE(filters.back()->field_ids_.empty());
        ASSERT_EQ(filters.back()->plan_node_->limit_, kLimit);
    }
    report["conjunction_plan_ms"] = ElapsedMs(plan_start);
    std::vector<std::vector<int64_t>> expected;
    size_t correctness_queries = 0;
    PredicateWork historical{ip_, url_, {}, {}};
    for (size_t i = 0; i < kPlans; ++i) {
        for (const auto& [timestamp, queries] : snapshots.items()) {
            const auto ts = static_cast<Timestamp>(std::stoull(timestamp));
            if (!updated && ts != latest)
                continue;
            SCOPED_TRACE(::testing::Message()
                         << "parameter " << i << " timestamp " << ts);
            const auto& query = queries.at(i);
            const auto& rows = query.at("rows");
            const auto offsets = ExpectedOffsets(rows);
            ASSERT_FALSE(offsets.empty());
            ASSERT_GT(query.at("url_matches").get<size_t>(), 0);
            ASSERT_LE(offsets.size(), kLimit);
            const auto result = [&] {
                if (ts == latest)
                    return Retrieve(projected[i], ts);
                MeasurePredicateWork observe(historical);
                return Retrieve(projected[i], ts);
            }();
            ASSERT_NO_FATAL_FAILURE(VerifyOffsets(*result, offsets));
            ASSERT_EQ(result->has_more_result(),
                      query.at("total").get<size_t>() > offsets.size());
            ASSERT_NO_FATAL_FAILURE(VerifyProjected(*result, rows, true));
            ++correctness_queries;
            if (ts == latest)
                expected.push_back(offsets);
        }
    }
    report["projected_correctness_queries"] = correctness_queries;
    report["historical_work"] = {{"ClientIP", historical.ip_work.Report()},
                                 {"URL", historical.url_work.Report()}};
    for (const auto& plan : filters) Retrieve(plan, latest);
    report["rss_bytes"]["after_warmup"] = ResidentBytes();

    // Check the production route on every timed sequential request. Work
    // assertions and protobuf result comparisons are outside the stopwatch.
    PredicateWork timed{ip_, url_, {}, {}};
    const auto before = records.Diagnostics();
    {
        MeasurePredicateWork observe(timed);
        report["conjunction"] =
            Latency(TimeRetrieve(filters, latest, expected, 0, &timed));
    }
    const auto after = records.Diagnostics();
    for (const auto* work : {&timed.ip_work, &timed.url_work}) {
        EXPECT_GE(work->index_calls, indexed ? kIterations : 0);
        EXPECT_EQ(work->index_calls > 0, indexed);
        EXPECT_EQ(work->raw_rows > 0, !indexed);
        if (updated && indexed) {
            EXPECT_EQ(work->reverse_calls, kIterations);
            EXPECT_EQ(work->reverse_selected, distinct * kIterations);
            EXPECT_EQ(work->reverse_scanned, work->reverse_selected);
            EXPECT_EQ(work->visited_reads, rounds > 1 ? kIterations : 0);
            EXPECT_EQ(work->visited_bitmap_bits,
                      rounds > 1 ? kRows * kIterations : 0);
        } else {
            EXPECT_EQ(work->reverse_calls, 0);
        }
        if (updated && !indexed) {
            EXPECT_GT(work->overlay_batches, 0);
            EXPECT_GT(work->dictionary_probes, 0);
            EXPECT_EQ(work->overlay_selected, work->dictionary_probes);
        } else {
            EXPECT_EQ(work->overlay_calls, 0);
        }
    }
    EXPECT_EQ(after.reverse_scans - before.reverse_scans,
              timed.ip_work.reverse_calls + timed.url_work.reverse_calls);
    EXPECT_EQ(after.reverse_selected - before.reverse_selected,
              timed.ip_work.reverse_selected + timed.url_work.reverse_selected);
    EXPECT_EQ(after.overlay_batches - before.overlay_batches,
              timed.ip_work.overlay_batches + timed.url_work.overlay_batches);
    EXPECT_EQ(
        after.dictionary_probes - before.dictionary_probes,
        timed.ip_work.dictionary_probes + timed.url_work.dictionary_probes);
    report["conjunction"]["work"] = {{"ClientIP", timed.ip_work.Report()},
                                     {"URL", timed.url_work.Report()}};
    report["rss_bytes"]["after_conjunction"] = ResidentBytes();

    if (rounds == 3) {
        if (updated) {
            for (const auto& plan : filters) Retrieve(plan, latest);
            report["conjunction_without_observer"] =
                Latency(TimeRetrieve(filters, latest, expected, 0));
        }
        const auto& equality = truth.at("equality");
        ASSERT_EQ(equality.size(), kPlans);
        std::vector<Plan> empty, selected;
        std::vector<std::vector<int64_t>> matches;
        for (const auto& row : equality) {
            const auto offset = row.at(0).get<int64_t>();
            const auto equal = KeyEquality(kFirstPk + offset);
            empty.push_back(MakePlan(equal));
            selected.push_back(MakePlan(equal, {ip_, url_}));
            matches.push_back({offset});
            const auto projected_result = Retrieve(selected.back(), latest);
            ASSERT_NO_FATAL_FAILURE(
                VerifyOffsets(*projected_result, matches.back()));
            ASSERT_NO_FATAL_FAILURE(
                VerifyProjected(*projected_result, Json::array({row}), false));
        }
        for (const auto& plan : empty) Retrieve(plan, latest);
        for (const auto& plan : selected) Retrieve(plan, latest);
        const auto empty_before = records.Diagnostics();
        report["equality_empty"] =
            Latency(TimeRetrieve(empty, latest, matches, 0));
        const auto empty_after = records.Diagnostics();
        report["equality_selected"] =
            Latency(TimeRetrieve(selected, latest, matches, 2));
        const auto selected_after = records.Diagnostics();
        EXPECT_EQ(
            empty_after.dictionary_probes - empty_before.dictionary_probes, 0);
        EXPECT_EQ(
            selected_after.dictionary_probes - empty_after.dictionary_probes,
            updated ? kIterations : 0);
        report["equality_selected"]["dictionary_probes"] =
            selected_after.dictionary_probes - empty_after.dictionary_probes;
        report["equality_selected"]["overlay_batches"] =
            selected_after.overlay_batches - empty_after.overlay_batches;
        report["rss_bytes"]["after_equality"] = ResidentBytes();
    }
    EXPECT_FALSE(exec::ExprResCacheManager::IsEnabled());
#if defined(__APPLE__) || defined(__linux__)
    for (const auto& rss : report.at("rss_bytes"))
        EXPECT_GT(rss.get<uint64_t>(), 0);
#endif
    std::cout << "PK_UPDATE_NATIVE_BENCH " << report.dump() << '\n';
}
#endif

}  // namespace
}  // namespace milvus::segcore
