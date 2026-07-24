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

// Tests for the `bloom_match` C++ probe path (plan proto BloomFilterExpr →
// expr::BloomFilterExpr → PhyBloomFilterExpr), per design doc
// docs/design-docs/design_docs/20260707-bloom-filter-expression.md.
//
// Golden-vector conformance consumes the same
// client/sbbf/testdata/golden_vectors.json used by the Go builder tests;
// the C++ prober must reproduce every membership answer bit-identically.

#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <arrow/io/memory.h>
#include <parquet/bloom_filter.h>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <memory>
#include <random>
#include <string>
#include <unordered_set>
#include <vector>

#include "xxhash.h"

#include "common/EasyAssert.h"
#include "common/IndexMeta.h"
#include "common/Schema.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "exec/QueryContext.h"
#include "exec/Task.h"
#include "exec/expression/BloomFilterExpr.h"
#include "exec/expression/EvalCtx.h"
#include "exec/expression/Expr.h"
#include "exec/expression/LogicalUnaryExpr.h"
#include "expr/ITypeExpr.h"
#include "index/BitmapIndex.h"
#include "index/ScalarIndexSort.h"
#include "pb/plan.pb.h"
#include "query/ExecPlanNodeVisitor.h"
#include "query/PlanProto.h"
#include "segcore/SegmentGrowingImpl.h"
#include "segcore/SegmentSealed.h"
#include "segcore/Types.h"
#include "test_utils/DataGen.h"
#include "test_utils/GenExprProto.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

using namespace milvus;
using namespace milvus::exec;
using namespace milvus::segcore;

namespace {

// Locate client/sbbf/testdata/golden_vectors.json relative to this source
// file (repo root is four levels up from internal/core/unittest/<file>).
std::filesystem::path
GoldenVectorsPath() {
    // Co-located with this test (internal/core/unittest/testdata/bloom) so the
    // C++ unittest environment — which does not check out the standalone
    // client/ module — can always find it. The authoritative copy the client
    // SDK ships is client/sbbf/testdata/golden_vectors.json; both are generated
    // from the same spec and pinned identical by CppGeneratedFixtureIsReproducible.
    auto path =
        std::filesystem::path(__FILE__).parent_path()  // internal/core/unittest
        / "testdata" / "bloom" / "golden_vectors.json";
    if (!std::filesystem::exists(path)) {
        if (const char* env = std::getenv("MILVUS_SBBF_GOLDEN_VECTORS")) {
            path = env;
        }
    }
    return path;
}

std::filesystem::path
CppGeneratedFixturePath() {
    auto path =
        std::filesystem::path(__FILE__).parent_path()  // internal/core/unittest
        / "testdata" / "bloom" / "cpp_generated_100_int64.json";
    if (!std::filesystem::exists(path)) {
        if (const char* env = std::getenv("MILVUS_SBBF_CPP_FIXTURE")) {
            path = env;
        }
    }
    return path;
}

std::string
HexDecode(const std::string& hex) {
    EXPECT_EQ(hex.size() % 2, 0);
    auto nibble = [](char c) -> int {
        if (c >= '0' && c <= '9')
            return c - '0';
        if (c >= 'a' && c <= 'f')
            return c - 'a' + 10;
        if (c >= 'A' && c <= 'F')
            return c - 'A' + 10;
        ADD_FAILURE() << "invalid hex char: " << c;
        return 0;
    };
    std::string out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        out.push_back(
            static_cast<char>((nibble(hex[i]) << 4) | nibble(hex[i + 1])));
    }
    return out;
}

std::string
HexEncode(const std::string& bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string hex;
    hex.reserve(bytes.size() * 2);
    for (unsigned char byte : bytes) {
        hex.push_back(kHex[byte >> 4]);
        hex.push_back(kHex[byte & 0x0f]);
    }
    return hex;
}

std::string
SerializeMbf1(std::string bitset, uint64_t n_declared, double fpr_declared) {
    std::string out(32 + bitset.size(), '\0');
    auto put_u16 = [&](size_t off, uint16_t v) {
        out[off] = static_cast<char>(v & 0xff);
        out[off + 1] = static_cast<char>((v >> 8) & 0xff);
    };
    auto put_u32 = [&](size_t off, uint32_t v) {
        for (int i = 0; i < 4; ++i) {
            out[off + i] = static_cast<char>((v >> (8 * i)) & 0xff);
        }
    };
    auto put_u64 = [&](size_t off, uint64_t v) {
        for (int i = 0; i < 8; ++i) {
            out[off + i] = static_cast<char>((v >> (8 * i)) & 0xff);
        }
    };
    out[0] = 'M';
    out[1] = 'B';
    out[2] = 'F';
    out[3] = '1';
    put_u16(4, 1);  // version
    put_u16(6, 1);  // algo = parquet_sbbf_xxh64
    put_u64(8, n_declared);
    uint64_t fpr_bits;
    static_assert(sizeof(fpr_bits) == sizeof(fpr_declared));
    std::memcpy(&fpr_bits, &fpr_declared, sizeof(fpr_bits));
    put_u64(16, fpr_bits);
    put_u32(24, static_cast<uint32_t>(bitset.size() / 32));
    put_u32(28, 0);  // reserved
    std::memcpy(out.data() + 32, bitset.data(), bitset.size());
    return out;
}

// Minimal SBBF builder mirroring client/sbbf (Go) and Arrow's
// parquet::BlockSplitBloomFilter. Used only to construct filters for
// expression-level tests; cross-language bit-compatibility of the layout is
// pinned separately by the golden vectors.
class TestSbbfBuilder {
 public:
    explicit TestSbbfBuilder(uint32_t num_blocks)
        : num_blocks_(num_blocks), words_(size_t(num_blocks) * 8, 0) {
        // SBBF invariant: power-of-two number of blocks.
        EXPECT_NE(num_blocks, 0);
        EXPECT_EQ(num_blocks & (num_blocks - 1), 0);
    }

    void
    AddInt64(int64_t v) {
        uint8_t buf[8];
        auto u = static_cast<uint64_t>(v);
        for (int i = 0; i < 8; ++i) {
            buf[i] = static_cast<uint8_t>(u >> (8 * i));
        }
        AddHash(XXH64(buf, sizeof(buf), 0));
    }

    void
    AddString(const std::string& s) {
        AddHash(XXH64(s.data(), s.size(), 0));
    }

    // Serialize as an MBF1 envelope (header + little-endian words).
    std::string
    Serialize(uint64_t n_declared = 0, double fpr_declared = 0.001) const {
        std::string out(32 + words_.size() * 4, '\0');
        auto put_u16 = [&](size_t off, uint16_t v) {
            out[off] = static_cast<char>(v & 0xff);
            out[off + 1] = static_cast<char>((v >> 8) & 0xff);
        };
        auto put_u32 = [&](size_t off, uint32_t v) {
            for (int i = 0; i < 4; ++i) {
                out[off + i] = static_cast<char>((v >> (8 * i)) & 0xff);
            }
        };
        auto put_u64 = [&](size_t off, uint64_t v) {
            for (int i = 0; i < 8; ++i) {
                out[off + i] = static_cast<char>((v >> (8 * i)) & 0xff);
            }
        };
        out[0] = 'M';
        out[1] = 'B';
        out[2] = 'F';
        out[3] = '1';
        put_u16(4, 1);  // version
        put_u16(6, 1);  // algo = parquet_sbbf_xxh64
        put_u64(8, n_declared);
        uint64_t fpr_bits;
        static_assert(sizeof(fpr_bits) == sizeof(fpr_declared));
        std::memcpy(&fpr_bits, &fpr_declared, sizeof(fpr_bits));
        put_u64(16, fpr_bits);
        put_u32(24, num_blocks_);
        put_u32(28, 0);  // reserved
        for (size_t i = 0; i < words_.size(); ++i) {
            put_u32(32 + i * 4, words_[i]);
        }
        return out;
    }

 private:
    void
    AddHash(uint64_t h) {
        static constexpr uint32_t kSalt[8] = {0x47b6137bU,
                                              0x44974d91U,
                                              0x8824ad5bU,
                                              0xa2b7289dU,
                                              0x705495c7U,
                                              0x2df1424bU,
                                              0x9efc4947U,
                                              0x5c6bfb31U};
        const auto block = static_cast<uint32_t>(
            ((h >> 32) * static_cast<uint64_t>(num_blocks_)) >> 32);
        const auto key = static_cast<uint32_t>(h);
        for (int i = 0; i < 8; ++i) {
            words_[size_t(block) * 8 + i] |= uint32_t(1)
                                             << ((key * kSalt[i]) >> 27);
        }
    }

    uint32_t num_blocks_;
    std::vector<uint32_t> words_;
};

nlohmann::json
BuildCppGeneratedFixture() {
    constexpr uint64_t kSeed = 0x6d696c7675735342ULL;  // "milvusSB"
    constexpr size_t kMemberCount = 100;
    constexpr double kFpr = 0.001;
    parquet::BlockSplitBloomFilter builder;
    builder.Init(
        parquet::BlockSplitBloomFilter::OptimalNumOfBytes(kMemberCount, kFpr));
    std::mt19937_64 rng(kSeed);
    std::unordered_set<int64_t> seen;
    std::vector<int64_t> values;
    values.reserve(kMemberCount);
    while (values.size() < kMemberCount) {
        const auto value = static_cast<int64_t>(rng());
        if (seen.insert(value).second) {
            values.push_back(value);
            builder.InsertHash(builder.Hash(value));
        }
    }

    auto output = parquet::CreateOutputStream();
    builder.WriteTo(output.get());
    auto serialized = output->Finish().ValueOrDie()->ToString();
    const auto bitset =
        serialized.substr(serialized.size() - builder.GetBitsetSize());

    nlohmann::json values_json = nlohmann::json::array();
    for (const auto value : values) {
        values_json.push_back(std::to_string(value));
    }
    return {
        {"generator", "apache_arrow_parquet_block_split_bloom_filter"},
        {"seed", kSeed},
        {"fpr", kFpr},
        {"int_values", values_json},
        {"blob_hex", HexEncode(SerializeMbf1(bitset, kMemberCount, kFpr))},
    };
}

milvus::ErrorCode
CatchSegcoreErrorCode(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const milvus::SegcoreError& e) {
        return e.get_error_code();
    } catch (...) {
        ADD_FAILURE() << "expected SegcoreError, got another exception type";
        return milvus::ErrorCode::UnexpectedError;
    }
    ADD_FAILURE() << "expected SegcoreError, got no exception";
    return milvus::ErrorCode::Success;
}

}  // namespace

// ---------------------------------------------------------------------------
// Golden-vector conformance: the C++ prober must reproduce every membership
// answer from the shared vector file bit-identically.
// ---------------------------------------------------------------------------
TEST(BloomFilterExprTest, GoldenVectorConformance) {
    auto path = GoldenVectorsPath();
    ASSERT_TRUE(std::filesystem::exists(path))
        << "golden vector file not found at " << path
        << " (set MILVUS_SBBF_GOLDEN_VECTORS to override)";
    std::ifstream in(path);
    ASSERT_TRUE(in.is_open());
    auto doc = nlohmann::json::parse(in);

    ASSERT_TRUE(doc.contains("cases"));
    ASSERT_GT(doc["cases"].size(), 0);
    int total_probes = 0;
    for (const auto& c : doc["cases"]) {
        const auto name = c["name"].get<std::string>();
        const auto blob = HexDecode(c["blob_hex"].get<std::string>());
        auto view = SplitBlockBloomFilterView::Parse(blob);

        for (const auto& probe : c["probes"]) {
            const auto kind = probe["kind"].get<std::string>();
            const bool expect = probe["expect"].get<bool>();
            const bool member = probe["member"].get<bool>();
            bool got;
            if (kind == "int64") {
                // int64 values are decimal strings (survive double-precision
                // JSON parsers).
                int64_t v = std::stoll(probe["int64"].get<std::string>());
                got = view.TestInt64(v);
            } else {
                ASSERT_EQ(kind, "string") << "case " << name;
                const auto s = probe["string"].get<std::string>();
                got = view.TestBytes(s.data(), s.size());
            }
            EXPECT_EQ(got, expect)
                << "case " << name << " probe " << probe.dump();
            if (member) {
                // No false negatives, ever.
                EXPECT_TRUE(got) << "false negative in case " << name << ": "
                                 << probe.dump();
            }
            ++total_probes;
        }
    }
    // Sanity: the vector file actually pinned something.
    EXPECT_GT(total_probes, 30);
}

// The checked-in fixture is generated from the C++ implementation, not from
// the Go builder. Keep generation opt-in so ordinary test runs cannot modify
// the checkout; CI instead proves that the recorded bytes are reproducible.
TEST(BloomFilterExprTest, CppGeneratedFixtureIsReproducible) {
    const auto path = CppGeneratedFixturePath();
    const auto generated = BuildCppGeneratedFixture();
    if (std::getenv("MILVUS_SBBF_REGEN_CPP_FIXTURE") != nullptr) {
        std::ofstream out(path);
        ASSERT_TRUE(out) << "cannot write C++ SBBF fixture " << path;
        out << generated.dump(2) << '\n';
        ASSERT_TRUE(out.good()) << "cannot flush C++ SBBF fixture " << path;
        return;
    }

    std::ifstream in(path);
    ASSERT_TRUE(in) << "C++ SBBF fixture missing: " << path
                    << "; regenerate with "
                       "MILVUS_SBBF_REGEN_CPP_FIXTURE=1";
    nlohmann::json recorded;
    ASSERT_NO_THROW(in >> recorded);
    EXPECT_EQ(recorded, generated)
        << "C++ SBBF fixture diverged; regenerate intentionally with "
           "MILVUS_SBBF_REGEN_CPP_FIXTURE=1";
}

// ---------------------------------------------------------------------------
// MBF1 envelope validation: every malformed header is rejected with
// SegcoreError{ExprInvalid} (input error — the request blob is to blame).
// ---------------------------------------------------------------------------
TEST(BloomFilterExprTest, MalformedEnvelopeRejected) {
    TestSbbfBuilder builder(4);
    builder.AddInt64(42);
    const std::string good = builder.Serialize(1, 0.001);
    // The good blob parses.
    ASSERT_NO_THROW(SplitBlockBloomFilterView::Parse(good));

    auto expect_rejected = [](const std::string& blob, const char* what) {
        auto code = CatchSegcoreErrorCode(
            [&] { SplitBlockBloomFilterView::Parse(blob); });
        EXPECT_EQ(code, milvus::ErrorCode::ExprInvalid) << what;
    };

    // Empty / truncated below header size.
    expect_rejected("", "empty blob");
    expect_rejected(good.substr(0, 31), "truncated header");
    // Bad magic.
    {
        auto blob = good;
        blob[0] = 'X';
        expect_rejected(blob, "bad magic");
    }
    // Unsupported version.
    {
        auto blob = good;
        blob[4] = 2;
        expect_rejected(blob, "unsupported version");
    }
    // Unsupported algo.
    {
        auto blob = good;
        blob[6] = 2;
        expect_rejected(blob, "unsupported algo");
    }
    // Non-zero reserved field.
    {
        auto blob = good;
        blob[28] = 1;
        expect_rejected(blob, "non-zero reserved");
    }
    // num_blocks not a power of two.
    {
        auto blob = good;
        blob[24] = 3;
        expect_rejected(blob, "num_blocks not power of two");
    }
    // num_blocks == 0.
    {
        auto blob = good;
        blob[24] = 0;
        expect_rejected(blob, "num_blocks zero");
    }
    // Body length mismatch (truncated body).
    expect_rejected(good.substr(0, good.size() - 1), "short body");
    // Body length mismatch (extra bytes).
    expect_rejected(good + "x", "long body");
    // Hostile num_blocks pointing far past the body must be rejected by the
    // body-length check without any allocation or read.
    {
        auto blob = good;
        blob[24] = 0;
        blob[25] = 0;
        blob[26] = 16;  // num_blocks = 2^20
        blob[27] = 0;
        expect_rejected(blob, "num_blocks far beyond body");
    }
}

// ---------------------------------------------------------------------------
// ToString() is a cheap human-readable summary (blob length + declared element
// count), NOT a content hash — bloom_match does not participate in the
// expression result cache, so ToString need not be collision-resistant. It must
// never dump or hash the (up to 128 MiB) blob.
// ---------------------------------------------------------------------------
TEST(BloomFilterExprTest, ToStringSummarizesWithoutDumpingBlob) {
    const milvus::expr::ColumnInfo col(FieldId(101), DataType::INT64);

    TestSbbfBuilder builder(4);
    builder.AddInt64(1);
    builder.AddInt64(2);
    builder.AddInt64(3);
    const std::string blob = builder.Serialize(/*n_declared=*/3, 0.001);

    const std::string s = milvus::expr::BloomFilterExpr(col, blob).ToString();

    // Reports the declared element count and the blob length ...
    EXPECT_NE(s.find("DeclaredElements: 3"), std::string::npos) << s;
    EXPECT_NE(s.find("BlobBytes: " + std::to_string(blob.size())),
              std::string::npos)
        << s;
    // ... and never embeds the raw blob body (the 32-byte header + words).
    EXPECT_EQ(s.find(blob.substr(0, 8)), std::string::npos)
        << "ToString must not dump blob contents";
}

// ---------------------------------------------------------------------------
// ProtoParser dispatch: plan proto BloomFilterExpr maps to the logical expr;
// unsupported field types are rejected at plan-construction time.
// ---------------------------------------------------------------------------
TEST(BloomFilterExprTest, ProtoParserDispatchAndTypeCheck) {
    auto schema = std::make_shared<Schema>();
    schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
    auto pk_fid = schema->AddDebugField("pk", DataType::INT64);
    auto i64_fid = schema->AddDebugField("age64", DataType::INT64, true);
    auto str_fid = schema->AddDebugField("name", DataType::VARCHAR, true);
    auto json_fid = schema->AddDebugField("meta", DataType::JSON, true);
    auto double_fid = schema->AddDebugField("weight", DataType::DOUBLE);
    schema->set_primary_field_id(pk_fid);

    TestSbbfBuilder builder(4);
    builder.AddInt64(7);
    const std::string blob = builder.Serialize();

    query::ProtoParser parser(schema);

    auto make_expr_pb = [&](FieldId fid, DataType dt) {
        proto::plan::Expr expr_pb;
        auto* bf = expr_pb.mutable_bloom_filter_expr();
        auto* col = bf->mutable_column_info();
        col->set_field_id(fid.get());
        col->set_data_type(static_cast<proto::schema::DataType>(dt));
        bf->set_filter_blob(blob);
        return expr_pb;
    };

    // INT64 and VARCHAR parse into the logical BloomFilterExpr.
    for (auto [fid, dt] : {std::pair{i64_fid, DataType::INT64},
                           std::pair{str_fid, DataType::VARCHAR}}) {
        auto expr_pb = make_expr_pb(fid, dt);
        auto parsed = parser.ParseExprs(expr_pb);
        auto bf_expr =
            std::dynamic_pointer_cast<const expr::BloomFilterExpr>(parsed);
        ASSERT_NE(bf_expr, nullptr);
        EXPECT_EQ(bf_expr->column_.field_id_, fid);
        EXPECT_EQ(bf_expr->filter_blob_, blob);
    }

    // JSON parses too, and the nested path survives into the logical expr.
    {
        auto expr_pb = make_expr_pb(json_fid, DataType::JSON);
        auto* col = expr_pb.mutable_bloom_filter_expr()->mutable_column_info();
        col->add_nested_path("uid");
        auto parsed = parser.ParseExprs(expr_pb);
        auto bf_expr =
            std::dynamic_pointer_cast<const expr::BloomFilterExpr>(parsed);
        ASSERT_NE(bf_expr, nullptr);
        EXPECT_EQ(bf_expr->column_.field_id_, json_fid);
        ASSERT_EQ(bf_expr->column_.nested_path_.size(), 1);
        EXPECT_EQ(bf_expr->column_.nested_path_[0], "uid");
    }

    // DOUBLE is rejected at construction time with an input classification.
    {
        auto expr_pb = make_expr_pb(double_fid, DataType::DOUBLE);
        auto code = CatchSegcoreErrorCode([&] { parser.ParseExprs(expr_pb); });
        EXPECT_EQ(code, milvus::ErrorCode::ExprInvalid);
    }
}

// ---------------------------------------------------------------------------
// Expression-level evaluation over growing and sealed segments.
// ---------------------------------------------------------------------------
class BloomFilterExprEvalTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        schema_ = std::make_shared<Schema>();
        schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, 16, knowhere::metric::L2);
        pk_fid_ = schema_->AddDebugField("pk", DataType::INT64);
        i64_fid_ = schema_->AddDebugField("age64", DataType::INT64, true);
        str_fid_ = schema_->AddDebugField("name", DataType::VARCHAR, true);
        json_fid_ = schema_->AddDebugField("meta", DataType::JSON, true);
        schema_->set_primary_field_id(pk_fid_);

        dataset_ = std::make_unique<GeneratedData>(DataGen(schema_, N));
        i64_col_ = dataset_->get_col<int64_t>(i64_fid_);
        i64_valid_ = dataset_->get_col_valid(i64_fid_);
        str_col_ = dataset_->get_col<std::string>(str_fid_);
        str_valid_ = dataset_->get_col_valid(str_fid_);
    }

    // Replace the DataGen-randomized JSON column (and its validity flags) with
    // hand-crafted rows, so JSON-path tests control the exact value type at
    // the probed path per row. Must be called BEFORE BuildGrowing/BuildSealed:
    // both consume dataset_->raw_.
    void
    OverwriteJsonColumn(const std::vector<std::string>& rows,
                        const FixedVector<bool>& valid) {
        ASSERT_EQ(rows.size(), N);
        ASSERT_EQ(valid.size(), N);
        bool found = false;
        for (auto& fd : *dataset_->raw_->mutable_fields_data()) {
            if (fd.field_id() != json_fid_.get()) {
                continue;
            }
            found = true;
            auto* jd =
                fd.mutable_scalars()->mutable_json_data()->mutable_data();
            jd->Clear();
            for (const auto& s : rows) {
                jd->Add(std::string(s));
            }
            fd.mutable_valid_data()->Clear();
            for (bool v : valid) {
                fd.mutable_valid_data()->Add(v);
            }
        }
        ASSERT_TRUE(found) << "json field not present in generated dataset";
        json_col_.assign(rows.begin(), rows.end());
        json_valid_ = valid;
    }

    std::unique_ptr<SegmentGrowing>
    BuildGrowing() {
        auto seg = CreateGrowingSegment(schema_, empty_index_meta);
        seg->PreInsert(N);
        seg->Insert(0,
                    N,
                    dataset_->row_ids_.data(),
                    dataset_->timestamps_.data(),
                    dataset_->raw_);
        return seg;
    }

    std::unique_ptr<SegmentSealed>
    BuildSealed() {
        return CreateSealedWithFieldDataLoaded(schema_, *dataset_);
    }

    // Evaluate a bloom filter expr (optionally negated) on a segment and
    // check every row against the reference prober:
    //   NULL rows never match, under either polarity;
    //   valid rows match iff blob-probe(value) (xor negation);
    //   member rows are never missed (no false negatives);
    //   non-member valid rows only rarely match (FP-rate sanity).
    template <typename ValueVec, typename ProbeFn>
    void
    CheckEval(const SegmentInternalInterface* segment,
              const expr::ColumnInfo& column,
              const std::string& blob,
              const ValueVec& values,
              const FixedVector<bool>& valid,
              const std::unordered_set<size_t>& member_rows,
              ProbeFn probe,
              bool negated) {
        std::shared_ptr<expr::ITypeExpr> typed_expr =
            std::make_shared<expr::BloomFilterExpr>(column, blob);
        if (negated) {
            typed_expr = std::make_shared<expr::LogicalUnaryExpr>(
                expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
        }
        auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
        auto final = query::ExecuteQueryExpr(plan, segment, N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);

        auto view = SplitBlockBloomFilterView::Parse(blob);
        int64_t non_member_valid = 0;
        int64_t false_positives = 0;
        for (size_t i = 0; i < N; ++i) {
            if (!valid[i]) {
                // NULL never matches, under either polarity.
                ASSERT_FALSE(final[i])
                    << "NULL row " << i << " matched (negated=" << negated
                    << ")";
                continue;
            }
            const bool probed = probe(view, values[i]);
            const bool expected = negated ? !probed : probed;
            ASSERT_EQ(final[i], expected)
                << "row " << i << " value mismatch (negated=" << negated << ")";
            if (member_rows.count(i) != 0) {
                // No false negatives: members always probe true.
                ASSERT_TRUE(probed) << "false negative at member row " << i;
            } else {
                ++non_member_valid;
                if (probed) {
                    ++false_positives;
                }
            }
        }
        // FP-rate sanity: the filter is sized generously (64 blocks for
        // ~N/2 members → far below 1% FPR); 5% is a loose, non-flaky bound.
        if (non_member_valid > 0) {
            EXPECT_LE(false_positives, non_member_valid / 20 + 2)
                << "suspiciously high false-positive count";
        }
    }

    static constexpr size_t N = 2000;

    SchemaPtr schema_;
    FieldId pk_fid_, i64_fid_, str_fid_, json_fid_;
    std::unique_ptr<GeneratedData> dataset_;
    FixedVector<int64_t> i64_col_;
    FixedVector<bool> i64_valid_;
    FixedVector<std::string> str_col_;
    FixedVector<bool> str_valid_;
    std::vector<std::string> json_col_;
    FixedVector<bool> json_valid_;
};

TEST_F(BloomFilterExprEvalTest, Int64GrowingAndSealed) {
    // Members: values of every third valid row.
    TestSbbfBuilder builder(64);
    std::unordered_set<size_t> member_rows;
    std::unordered_set<int64_t> member_values;
    for (size_t i = 0; i < N; i += 3) {
        if (i64_valid_[i]) {
            builder.AddInt64(i64_col_[i]);
            member_values.insert(i64_col_[i]);
        }
    }
    // Duplicated values make non-chosen rows members too; account for that.
    for (size_t i = 0; i < N; ++i) {
        if (i64_valid_[i] && member_values.count(i64_col_[i]) != 0) {
            member_rows.insert(i);
        }
    }
    ASSERT_FALSE(member_rows.empty());
    const std::string blob = builder.Serialize(member_values.size(), 0.001);

    auto probe = [](const SplitBlockBloomFilterView& view, int64_t v) {
        return view.TestInt64(v);
    };
    expr::ColumnInfo column(i64_fid_, DataType::INT64, {}, true);

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const SegmentInternalInterface* seg :
         {static_cast<const SegmentInternalInterface*>(growing.get()),
          static_cast<const SegmentInternalInterface*>(sealed.get())}) {
        CheckEval(seg,
                  column,
                  blob,
                  i64_col_,
                  i64_valid_,
                  member_rows,
                  probe,
                  /*negated=*/false);
        CheckEval(seg,
                  column,
                  blob,
                  i64_col_,
                  i64_valid_,
                  member_rows,
                  probe,
                  /*negated=*/true);
    }
}

TEST_F(BloomFilterExprEvalTest, VarcharGrowingAndSealed) {
    TestSbbfBuilder builder(64);
    std::unordered_set<size_t> member_rows;
    std::unordered_set<std::string> member_values;
    for (size_t i = 0; i < N; i += 3) {
        if (str_valid_[i]) {
            builder.AddString(str_col_[i]);
            member_values.insert(str_col_[i]);
        }
    }
    for (size_t i = 0; i < N; ++i) {
        if (str_valid_[i] && member_values.count(str_col_[i]) != 0) {
            member_rows.insert(i);
        }
    }
    ASSERT_FALSE(member_rows.empty());
    const std::string blob = builder.Serialize(member_values.size(), 0.001);

    auto probe = [](const SplitBlockBloomFilterView& view,
                    const std::string& s) {
        return view.TestBytes(s.data(), s.size());
    };
    expr::ColumnInfo column(str_fid_, DataType::VARCHAR, {}, true);

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const SegmentInternalInterface* seg :
         {static_cast<const SegmentInternalInterface*>(growing.get()),
          static_cast<const SegmentInternalInterface*>(sealed.get())}) {
        CheckEval(seg,
                  column,
                  blob,
                  str_col_,
                  str_valid_,
                  member_rows,
                  probe,
                  /*negated=*/false);
        CheckEval(seg,
                  column,
                  blob,
                  str_col_,
                  str_valid_,
                  member_rows,
                  probe,
                  /*negated=*/true);
    }
}

// ---------------------------------------------------------------------------
// JSON-path probes are STRICTLY TYPED: only values stored as int64 probe the
// int64 hash domain and only strings probe the UTF-8 domain. Any JSON double
// (including an integral 5.0) and any uint64 beyond int64 is NEVER a member
// (res=false, valid=true, so `not bloom_match` returns the row) -- a
// deliberate divergence from exact `in`, whose JSON semantics unify 5.0 == 5.
// Missing key / JSON null / bool / object / array have no probe value:
// res=false AND valid=false (three-valued), so neither polarity matches.

namespace {

enum class JsonVariant : int {
    kIntMember = 0,   // {"uid": 5}          member of the int blob
    kIntOther,        // {"uid": 7}          valid non-member
    kDoubleIntegral,  // {"uid": 5.0}        strictly typed: NOT a member
    kDoubleFraction,  // {"uid": 5.5}        definite non-member, valid
    kStrAlice,        // {"uid": "alice"}    string probe
    kStrFive,         // {"uid": "5"}        string, must not alias int64 5
    kBool,            // {"uid": true}       no probe value
    kNull,            // {"uid": null}       no probe value
    kMissing,         // {"other": 1}        no probe value
    kObject,          // {"uid": {"a": 1}}   no probe value
    kArray,           // {"uid": [5]}        no probe value
    kHugeUint,        // {"uid": 2^64-1}     definite non-member, valid
    kIntMin,          // {"uid": INT64_MIN}  int64 member probe
    kRowNull,         // whole-row NULL
    kVariantCount,
};

constexpr int kJsonVariants = static_cast<int>(JsonVariant::kVariantCount);

JsonVariant
JsonVariantAt(size_t row) {
    return static_cast<JsonVariant>(row % kJsonVariants);
}

std::string
JsonRowFor(JsonVariant v) {
    switch (v) {
        case JsonVariant::kIntMember:
            return R"({"uid": 5})";
        case JsonVariant::kIntOther:
            return R"({"uid": 7})";
        case JsonVariant::kDoubleIntegral:
            return R"({"uid": 5.0})";
        case JsonVariant::kDoubleFraction:
            return R"({"uid": 5.5})";
        case JsonVariant::kStrAlice:
            return R"({"uid": "alice"})";
        case JsonVariant::kStrFive:
            return R"({"uid": "5"})";
        case JsonVariant::kBool:
            return R"({"uid": true})";
        case JsonVariant::kNull:
            return R"({"uid": null})";
        case JsonVariant::kMissing:
            return R"({"other": 1})";
        case JsonVariant::kObject:
            return R"({"uid": {"a": 1}})";
        case JsonVariant::kArray:
            return R"({"uid": [5]})";
        case JsonVariant::kHugeUint:
            return R"({"uid": 18446744073709551615})";
        case JsonVariant::kIntMin:
            return R"({"uid": -9223372036854775808})";
        case JsonVariant::kRowNull:
            // content irrelevant; the row's validity flag is false.
            return R"({"uid": 5})";
        default:
            ADD_FAILURE() << "unhandled variant";
            return "{}";
    }
}

// Reference semantics of one row against a parsed view: {valid, probed}.
// Mirrors the DESIGN (value-type-driven hashing + three-valued gaps), not the
// implementation internals; the SBBF bit math itself is pinned by the golden
// vectors.
std::pair<bool, bool>
JsonReference(const SplitBlockBloomFilterView& view, JsonVariant v) {
    switch (v) {
        case JsonVariant::kIntMember:
            return {true, view.TestInt64(5)};
        case JsonVariant::kIntOther:
            return {true, view.TestInt64(7)};
        case JsonVariant::kDoubleIntegral:
            // Strictly typed: a stored double never probes the int64 domain,
            // even when it is exactly 5.0 and 5 is a member.
            return {true, false};
        case JsonVariant::kDoubleFraction:
            return {true, false};
        case JsonVariant::kStrAlice:
            return {true, view.TestBytes("alice", 5)};
        case JsonVariant::kStrFive:
            return {true, view.TestBytes("5", 1)};
        case JsonVariant::kHugeUint:
            return {true, false};
        case JsonVariant::kIntMin:
            return {true, view.TestInt64(std::numeric_limits<int64_t>::min())};
        case JsonVariant::kBool:
        case JsonVariant::kNull:
        case JsonVariant::kMissing:
        case JsonVariant::kObject:
        case JsonVariant::kArray:
        case JsonVariant::kRowNull:
            return {false, false};
        default:
            ADD_FAILURE() << "unhandled variant";
            return {false, false};
    }
}

}  // namespace

TEST_F(BloomFilterExprEvalTest, JsonPathGrowingAndSealed) {
    std::vector<std::string> rows(N);
    FixedVector<bool> valid(N, true);
    for (size_t i = 0; i < N; ++i) {
        const auto v = JsonVariantAt(i);
        rows[i] = JsonRowFor(v);
        if (v == JsonVariant::kRowNull) {
            valid[i] = false;
        }
    }
    OverwriteJsonColumn(rows, valid);

    TestSbbfBuilder builder(64);
    builder.AddInt64(5);
    builder.AddInt64(std::numeric_limits<int64_t>::min());
    const std::string blob = builder.Serialize(2, 0.001);
    const auto view = SplitBlockBloomFilterView::Parse(blob);
    // Members must probe true so the hard row-level guarantees below
    // (kIntMember / kIntMin rows match; kDoubleIntegral rows do NOT --
    // strict typing) flow from the reference, independent of
    // false-positive luck.
    ASSERT_TRUE(view.TestInt64(5));
    ASSERT_TRUE(view.TestInt64(std::numeric_limits<int64_t>::min()));

    expr::ColumnInfo column(
        json_fid_, DataType::JSON, std::vector<std::string>{"uid"}, true);

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const SegmentInternalInterface* seg :
         {static_cast<const SegmentInternalInterface*>(growing.get()),
          static_cast<const SegmentInternalInterface*>(sealed.get())}) {
        for (bool negated : {false, true}) {
            std::shared_ptr<expr::ITypeExpr> typed_expr =
                std::make_shared<expr::BloomFilterExpr>(column, blob);
            if (negated) {
                typed_expr = std::make_shared<expr::LogicalUnaryExpr>(
                    expr::LogicalUnaryExpr::OpType::LogicalNot, typed_expr);
            }
            auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
            auto final = query::ExecuteQueryExpr(plan, seg, N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            for (size_t i = 0; i < N; ++i) {
                const auto v = JsonVariantAt(i);
                const auto [row_valid, probed] = JsonReference(view, v);
                const bool expected = row_valid && (negated ? !probed : probed);
                ASSERT_EQ(final[i], expected)
                    << "row " << i << " variant " << static_cast<int>(v)
                    << " negated=" << negated;
            }
        }
    }
}

// String-built blob over the same rows: string members match by raw UTF-8,
// and the int64/string hash domains stay distinct (JSON int 5 does not alias
// the string "5" and vice versa).
TEST_F(BloomFilterExprEvalTest, JsonPathStringBlobKeepsTypesDistinct) {
    std::vector<std::string> rows(N);
    FixedVector<bool> valid(N, true);
    for (size_t i = 0; i < N; ++i) {
        const auto v = JsonVariantAt(i);
        rows[i] = JsonRowFor(v);
        if (v == JsonVariant::kRowNull) {
            valid[i] = false;
        }
    }
    OverwriteJsonColumn(rows, valid);

    TestSbbfBuilder builder(64);
    builder.AddString("alice");
    builder.AddString("5");
    const std::string blob = builder.Serialize(2, 0.001);
    const auto view = SplitBlockBloomFilterView::Parse(blob);
    ASSERT_TRUE(view.TestBytes("alice", 5));
    ASSERT_TRUE(view.TestBytes("5", 1));

    expr::ColumnInfo column(
        json_fid_, DataType::JSON, std::vector<std::string>{"uid"}, true);

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    for (const SegmentInternalInterface* seg :
         {static_cast<const SegmentInternalInterface*>(growing.get()),
          static_cast<const SegmentInternalInterface*>(sealed.get())}) {
        std::shared_ptr<expr::ITypeExpr> typed_expr =
            std::make_shared<expr::BloomFilterExpr>(column, blob);
        auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
        auto final = query::ExecuteQueryExpr(plan, seg, N, MAX_TIMESTAMP);
        ASSERT_EQ(final.size(), N);
        for (size_t i = 0; i < N; ++i) {
            const auto v = JsonVariantAt(i);
            const auto [row_valid, probed] = JsonReference(view, v);
            const bool expected = row_valid && probed;
            ASSERT_EQ(final[i], expected)
                << "row " << i << " variant " << static_cast<int>(v);
            // The string members must actually match (no false negatives).
            if (v == JsonVariant::kStrAlice || v == JsonVariant::kStrFive) {
                ASSERT_TRUE(final[i]) << "string member missed at row " << i;
            }
        }
    }
}

// Nested two-level path and the whole-document (root) path.
TEST_F(BloomFilterExprEvalTest, JsonNestedAndRootPath) {
    // Rows cycle through 6 shapes; the nested path {"a","b"} and the root
    // path each see a different value per shape.
    const std::vector<std::string> shapes = {
        R"({"a": {"b": 5}})",    // nested: int member;   root: object (gap)
        R"({"a": {"b": "x"}})",  // nested: string;       root: object (gap)
        R"({"a": {}})",          // nested: missing;      root: object (gap)
        R"(5)",                  // nested: missing;      root: int member
        R"("alice")",            // nested: missing;      root: string
        R"({"a": null})",        // nested: JSON null;    root: object (gap)
    };
    std::vector<std::string> rows(N);
    FixedVector<bool> valid(N, true);
    for (size_t i = 0; i < N; ++i) {
        rows[i] = shapes[i % shapes.size()];
    }
    OverwriteJsonColumn(rows, valid);

    TestSbbfBuilder builder(64);
    builder.AddInt64(5);
    const std::string blob = builder.Serialize(1, 0.001);
    const auto view = SplitBlockBloomFilterView::Parse(blob);
    ASSERT_TRUE(view.TestInt64(5));

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();

    // {valid, probed} per shape for a given path.
    using Ref = std::pair<bool, bool>;
    const std::vector<Ref> nested_ref = {
        {true, view.TestInt64(5)},
        {true, view.TestBytes("x", 1)},
        {false, false},
        {false, false},
        {false, false},
        {false, false},
    };
    const std::vector<Ref> root_ref = {
        {false, false},
        {false, false},
        {false, false},
        {true, view.TestInt64(5)},
        {true, view.TestBytes("alice", 5)},
        {false, false},
    };

    struct PathCase {
        std::vector<std::string> nested_path;
        const std::vector<Ref>* ref;
    };
    const PathCase cases[] = {
        {{"a", "b"}, &nested_ref},
        {{}, &root_ref},
    };

    for (const auto& c : cases) {
        expr::ColumnInfo column(json_fid_, DataType::JSON, c.nested_path, true);
        for (const SegmentInternalInterface* seg :
             {static_cast<const SegmentInternalInterface*>(growing.get()),
              static_cast<const SegmentInternalInterface*>(sealed.get())}) {
            std::shared_ptr<expr::ITypeExpr> typed_expr =
                std::make_shared<expr::BloomFilterExpr>(column, blob);
            auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
            auto final = query::ExecuteQueryExpr(plan, seg, N, MAX_TIMESTAMP);
            ASSERT_EQ(final.size(), N);
            for (size_t i = 0; i < N; ++i) {
                const auto [row_valid, probed] = (*c.ref)[i % shapes.size()];
                ASSERT_EQ(final[i], row_valid && probed)
                    << "row " << i << " shape " << i % shapes.size()
                    << " path depth " << c.nested_path.size();
            }
        }
    }
    // The int member at the nested path must actually match.
    // (Shape 0 probes TestInt64(5), asserted true above.)
}

// Offset-input (iterative filter) over a JSON path: candidates must be
// evaluated via ProcessDataByOffsets, per candidate row, not the first N
// rows sequentially.
TEST_F(BloomFilterExprEvalTest, JsonPathOffsetInput) {
    std::vector<std::string> rows(N);
    FixedVector<bool> valid(N, true);
    for (size_t i = 0; i < N; ++i) {
        const auto v = JsonVariantAt(i);
        rows[i] = JsonRowFor(v);
        if (v == JsonVariant::kRowNull) {
            valid[i] = false;
        }
    }
    OverwriteJsonColumn(rows, valid);

    TestSbbfBuilder builder(64);
    builder.AddInt64(5);
    builder.AddInt64(std::numeric_limits<int64_t>::min());
    const std::string blob = builder.Serialize(2, 0.001);
    const auto view = SplitBlockBloomFilterView::Parse(blob);
    ASSERT_TRUE(view.TestInt64(5));

    // Non-contiguous, out-of-order candidates spanning the whole segment,
    // covering member/non-member/gap/whole-row-NULL variants (1777 % 14 == 13
    // is a whole-row NULL; 1999 % 14 == 11 is the huge uint64).
    milvus::exec::OffsetVector offsets;
    for (auto o : std::vector<int32_t>{
             1999, 3, 402, 57, 1500, 800, 17, 1234, 999, 6, 1777, 450}) {
        offsets.emplace_back(o);
    }

    expr::ColumnInfo column(
        json_fid_, DataType::JSON, std::vector<std::string>{"uid"}, true);

    auto run = [&](const SegmentInternalInterface* segment) {
        std::shared_ptr<expr::ITypeExpr> typed_expr =
            std::make_shared<expr::BloomFilterExpr>(column, blob);
        auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        auto col_vec = milvus::test::gen_filter_res(
            filter_node.get(), segment, N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView res(col_vec->GetRawData(), col_vec->size());
        ASSERT_EQ(res.size(), offsets.size());

        for (size_t k = 0; k < offsets.size(); ++k) {
            const int32_t row = offsets[k];
            const auto [row_valid, probed] =
                JsonReference(view, JsonVariantAt(row));
            ASSERT_EQ(res[k], row_valid && probed)
                << "candidate k=" << k << " row=" << row;
        }

        // Candidate 0 (row 1999, huge uint64 -> false) vs sequential row 0
        // (int member 5 -> true): a first-N sequential scan provably differs,
        // so this test depends on the offset-input path.
        const auto [v0, p0] = JsonReference(view, JsonVariantAt(0));
        ASSERT_NE(res[0], v0 && p0)
            << "offset-input result indistinguishable from a first-N scan";
    };

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    run(growing.get());
    run(sealed.get());
}

TEST_F(BloomFilterExprEvalTest, MalformedBlobRejectedAtPhyConstruction) {
    // A malformed envelope must fail physical-expr construction (which
    // happens during plan compilation on evaluation), not silently
    // evaluate.
    auto growing = BuildGrowing();
    TestSbbfBuilder builder(4);
    builder.AddInt64(1);
    auto blob = builder.Serialize();
    blob[4] = 9;  // unsupported version

    expr::ColumnInfo column(i64_fid_, DataType::INT64, {}, true);
    auto typed_expr = std::make_shared<expr::BloomFilterExpr>(column, blob);
    auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
    EXPECT_ANY_THROW(
        query::ExecuteQueryExpr(plan, growing.get(), N, MAX_TIMESTAMP));
}

// ---------------------------------------------------------------------------
// Iterative-filter (offset input) correctness: when the upstream passes an
// explicit, NON-CONTIGUOUS, OUT-OF-ORDER offset vector, bloom_match must probe
// exactly those candidate rows and mark result bit k for offsets[k] -- not the
// first-N rows sequentially. This exercises the SetHasOffsetInput() wiring in
// PhyBloomFilterExpr::Eval(); without it, has_offset_input_ stays false, the
// expr runs ProcessDataChunks over the first offsets.size() rows instead of
// ProcessDataByOffsets over the candidates, and both the value mapping and the
// one-sided (no-false-negative) guarantee break.
TEST_F(BloomFilterExprEvalTest, Int64OffsetInputIterativeFilter) {
    // Small member set: the values at a handful of specific rows.
    const std::vector<size_t> member_seed_rows = {17, 402, 999, 1500, 1999};
    TestSbbfBuilder builder(64);
    std::unordered_set<int64_t> member_values;
    for (auto r : member_seed_rows) {
        if (i64_valid_[r]) {
            builder.AddInt64(i64_col_[r]);
            member_values.insert(i64_col_[r]);
        }
    }
    ASSERT_FALSE(member_values.empty());
    const std::string blob = builder.Serialize(member_values.size(), 0.001);
    auto view = SplitBlockBloomFilterView::Parse(blob);

    // Candidate offsets: deliberately non-contiguous and out-of-order, and
    // NOT a prefix of [0, N). Interleaves known members with non-members and
    // spans the whole segment so a first-N sequential scan would give a
    // provably different answer.
    milvus::exec::OffsetVector offsets;
    for (auto o : std::vector<int32_t>{
             1999, 3, 402, 57, 1500, 800, 17, 1234, 999, 6, 1777, 450}) {
        offsets.emplace_back(o);
    }

    expr::ColumnInfo column(i64_fid_, DataType::INT64, {}, true);

    auto run = [&](const SegmentInternalInterface* segment) {
        std::shared_ptr<expr::ITypeExpr> typed_expr =
            std::make_shared<expr::BloomFilterExpr>(column, blob);
        auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
            DEFAULT_PLANNODE_ID, typed_expr);
        auto col_vec = milvus::test::gen_filter_res(
            filter_node.get(), segment, N, MAX_TIMESTAMP, &offsets);
        BitsetTypeView res(col_vec->GetRawData(), col_vec->size());
        // Result size must equal the number of candidate offsets, one bit per
        // candidate, NOT the first offsets.size() rows.
        ASSERT_EQ(res.size(), offsets.size());

        bool saw_true = false;
        bool saw_false = false;
        for (size_t k = 0; k < offsets.size(); ++k) {
            const int32_t row = offsets[k];
            bool expected;
            if (!i64_valid_[row]) {
                // NULL never matches.
                expected = false;
            } else {
                expected = view.TestInt64(i64_col_[row]);
            }
            ASSERT_EQ(res[k], expected)
                << "candidate k=" << k << " row=" << row;
            saw_true |= res[k];
            saw_false |= !res[k];
            // Seed members must never be missed (no false negatives).
            if (member_values.count(i64_col_[row]) != 0 && i64_valid_[row]) {
                ASSERT_TRUE(res[k]) << "false negative at row " << row;
            }
        }
        // The candidate set mixes members and non-members, so both outcomes
        // must appear -- otherwise the probe collapsed to a constant.
        EXPECT_TRUE(saw_true);
        EXPECT_TRUE(saw_false);

        // Adversarial cross-check: a first-N sequential scan (offsets ignored)
        // would answer for rows 0..offsets.size()-1, which is a DIFFERENT
        // answer than the candidate mapping. Prove they differ so this test
        // actually depends on the offset-input path.
        bool differs_from_sequential = false;
        for (size_t k = 0; k < offsets.size(); ++k) {
            const int32_t seq_row = static_cast<int32_t>(k);
            const bool seq_expected =
                i64_valid_[seq_row] ? view.TestInt64(i64_col_[seq_row]) : false;
            if (res[k] != seq_expected) {
                differs_from_sequential = true;
                break;
            }
        }
        EXPECT_TRUE(differs_from_sequential)
            << "offset-input result is indistinguishable from a first-N scan; "
               "the test cannot detect a missing SetHasOffsetInput";
    };

    auto growing = BuildGrowing();
    auto sealed = BuildSealed();
    run(growing.get());
    run(sealed.get());
}

// ---------------------------------------------------------------------------
// Finding #5 regression: a SEALED segment may load a field INDEX-ONLY, leaving
// no raw field data (num_data_chunk_ == 0). bloom_match must still evaluate --
// via the scalar index's Reverse_Lookup -- and must not assert/crash. Builds an
// int64 field with an STL_SORT scalar index but WITHOUT loading its raw data,
// then runs bloom_match (contiguous and offset-input) over it.
TEST_F(BloomFilterExprEvalTest, Int64SealedScalarIndexOnly) {
    // Load every field EXCEPT age64 (i64_fid_) into the sealed segment, so
    // age64 has no raw field data; then attach an STL_SORT index for age64.
    std::vector<int64_t> load_fields;
    for (auto& f : schema_->get_fields()) {
        if (f.first != i64_fid_) {
            load_fields.push_back(f.first.get());
        }
    }
    auto sealed = CreateSealedWithFieldDataLoaded(
        schema_, *dataset_, false, GetExcludedFieldIds(schema_, load_fields));

    // Sanity: the field is genuinely index-only (no raw data) before we load
    // the index, and stays that way after.
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));

    LoadIndexInfo idx;
    idx.field_id = i64_fid_.get();
    idx.field_type = DataType::INT64;
    idx.index_params["index_type"] = "STL_SORT";
    // Build the index WITH the field's real nullability, so the index's
    // Reverse_Lookup returns nullopt exactly for the segment's NULL rows.
    // (GenScalarIndexing's raw-pointer Build would mark every row valid.)
    auto index = index::CreateScalarIndexSort<int64_t>();
    index->Build(N, i64_col_.data(), i64_valid_.data());
    idx.index_params = GenIndexParams(index.get());
    idx.cache_index = CreateTestCacheIndex("test", std::move(index));
    sealed->LoadIndex(idx);
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));
    ASSERT_TRUE(sealed->HasIndex(i64_fid_));

    // Members: values of every third valid row (same construction as the
    // raw-data eval test, so the reference/FP bounds are comparable).
    TestSbbfBuilder builder(64);
    std::unordered_set<size_t> member_rows;
    std::unordered_set<int64_t> member_values;
    for (size_t i = 0; i < N; i += 3) {
        if (i64_valid_[i]) {
            builder.AddInt64(i64_col_[i]);
            member_values.insert(i64_col_[i]);
        }
    }
    for (size_t i = 0; i < N; ++i) {
        if (i64_valid_[i] && member_values.count(i64_col_[i]) != 0) {
            member_rows.insert(i);
        }
    }
    ASSERT_FALSE(member_rows.empty());
    const std::string blob = builder.Serialize(member_values.size(), 0.001);

    auto probe = [](const SplitBlockBloomFilterView& view, int64_t v) {
        return view.TestInt64(v);
    };
    expr::ColumnInfo column(i64_fid_, DataType::INT64, {}, true);

    // Full-segment (contiguous) evaluation must match the reference and must
    // not crash -- this is the exact path that asserted before the fix.
    CheckEval(sealed.get(),
              column,
              blob,
              i64_col_,
              i64_valid_,
              member_rows,
              probe,
              /*negated=*/false);
    CheckEval(sealed.get(),
              column,
              blob,
              i64_col_,
              i64_valid_,
              member_rows,
              probe,
              /*negated=*/true);

    // Offset-input evaluation over the index-only field: routes through
    // ProcessIndexLookupByOffsets. Non-contiguous, out-of-order candidates.
    auto view = SplitBlockBloomFilterView::Parse(blob);
    milvus::exec::OffsetVector offsets;
    for (auto o : std::vector<int32_t>{
             1998, 5, 300, 1501, 9, 777, 1200, 33, 1950, 601}) {
        offsets.emplace_back(o);
    }
    std::shared_ptr<expr::ITypeExpr> typed_expr =
        std::make_shared<expr::BloomFilterExpr>(column, blob);
    auto filter_node = std::make_shared<milvus::plan::FilterBitsNode>(
        DEFAULT_PLANNODE_ID, typed_expr);
    auto col_vec = milvus::test::gen_filter_res(
        filter_node.get(), sealed.get(), N, MAX_TIMESTAMP, &offsets);
    BitsetTypeView res(col_vec->GetRawData(), col_vec->size());
    ASSERT_EQ(res.size(), offsets.size());
    for (size_t k = 0; k < offsets.size(); ++k) {
        const int32_t row = offsets[k];
        const bool expected =
            i64_valid_[row] ? view.TestInt64(i64_col_[row]) : false;
        ASSERT_EQ(res[k], expected)
            << "index-only offset candidate k=" << k << " row=" << row;
    }
}

// ---------------------------------------------------------------------------
// Regression: execution-path determination must use the field-data snapshot
// captured when the physical expression is constructed.  A sealed segment can
// be reopened while a query is in flight, so raw field data may become visible
// after SegmentExpr initialized num_data_chunk_ but before Bloom first calls
// DetermineExecPath().
//
// Start index-only, compile the physical Bloom expression (capturing
// num_data_chunk_ == 0), then load the raw field before the first Eval.  The
// expression must keep the construction-time ScalarIndex path and produce the
// exact bitmap.  Reading live HasFieldData() during DetermineExecPath switches
// to RawData while num_data_chunk_ is still zero, so the pre-fix code
// processes zero rows and fails its processed_size == real_batch_size check.
TEST_F(BloomFilterExprEvalTest,
       IndexOnlyConstructionSnapshotSurvivesRawFieldLoadBeforeFirstEval) {
    std::vector<int64_t> load_fields;
    for (const auto& field : schema_->get_fields()) {
        if (field.first != i64_fid_) {
            load_fields.push_back(field.first.get());
        }
    }
    auto sealed = CreateSealedWithFieldDataLoaded(
        schema_, *dataset_, false, GetExcludedFieldIds(schema_, load_fields));
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));

    LoadIndexInfo idx;
    idx.field_id = i64_fid_.get();
    idx.field_type = DataType::INT64;
    auto index = index::CreateScalarIndexSort<int64_t>();
    index->Build(N, i64_col_.data(), i64_valid_.data());
    idx.index_params = GenIndexParams(index.get());
    idx.cache_index = CreateTestCacheIndex("snapshot-race", std::move(index));
    sealed->LoadIndex(idx);
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));
    ASSERT_TRUE(sealed->HasIndex(i64_fid_));

    TestSbbfBuilder builder(64);
    uint64_t member_count = 0;
    for (size_t i = 0; i < N; i += 3) {
        if (i64_valid_[i]) {
            builder.AddInt64(i64_col_[i]);
            ++member_count;
        }
    }
    const std::string blob = builder.Serialize(member_count, 0.001);
    const expr::ColumnInfo column(i64_fid_, DataType::INT64, {}, true);
    auto logical = std::make_shared<expr::BloomFilterExpr>(column, blob);

    auto query_context = std::make_shared<QueryContext>(
        DEAFULT_QUERY_ID, sealed.get(), N, MAX_TIMESTAMP);
    ExecContext exec_context(query_context.get());
    auto compiled = CompileExpressions({logical}, &exec_context, {}, false);
    ASSERT_EQ(compiled.size(), 1u);
    ASSERT_EQ(compiled[0]->name(), "PhyBloomFilterExpr");

    // Make only age64 raw data visible after physical construction but before
    // the first path determination/Eval.
    std::vector<int64_t> excluded_fields = {RowFieldID.get(),
                                            TimestampFieldID.get()};
    for (const auto& field : schema_->get_fields()) {
        if (field.first != i64_fid_) {
            excluded_fields.push_back(field.first.get());
        }
    }
    LoadGeneratedDataIntoSegment(
        *dataset_, sealed.get(), false, excluded_fields);
    ASSERT_TRUE(sealed->HasFieldData(i64_fid_));
    ASSERT_TRUE(sealed->HasIndex(i64_fid_));

    auto physical = std::dynamic_pointer_cast<PhyBloomFilterExpr>(compiled[0]);
    ASSERT_NE(physical, nullptr);
    ASSERT_TRUE(physical->UseIndexCursor());

    EvalCtx eval_ctx(&exec_context);
    VectorPtr result;
    ASSERT_NO_THROW(compiled[0]->Eval(eval_ctx, result));
    // static_pointer_cast, not dynamic: the result is created inside
    // libmilvus_core while this TU lives in the test binary, and ColumnVector's
    // typeinfo is emitted in both images (header-defined class). On macOS
    // arm64 dynamic_cast compares typeinfo by pointer across the dylib
    // boundary and spuriously fails; the concrete type is guaranteed here
    // (SegmentExpr::Eval always yields a ColumnVector).
    ASSERT_NE(result, nullptr);
    auto column_result = std::static_pointer_cast<ColumnVector>(result);
    ASSERT_EQ(column_result->size(), N);

    const auto filter = SplitBlockBloomFilterView::Parse(blob);
    BitsetTypeView result_bits(column_result->GetRawData(), N);
    BitsetTypeView valid_bits(column_result->GetValidRawData(), N);
    for (size_t i = 0; i < N; ++i) {
        const bool expected_valid = i64_valid_[i];
        const bool expected_match =
            expected_valid && filter.TestInt64(i64_col_[i]);
        ASSERT_EQ(valid_bits[i], expected_valid) << "row " << i;
        ASSERT_EQ(result_bits[i], expected_match) << "row " << i;
    }
}

// ---------------------------------------------------------------------------
// Finding #6: the MBF1 parser must reject an oversized envelope with a clear
// input-classified SegcoreError before allocating/reading, even though the
// proxy already validates the envelope at plan-build time (sbbf.Parse, same
// 128 MB format cap). A hand-crafted plan must not force an unbounded body.
TEST(BloomFilterExprTest, OversizedEnvelopeRejected) {
    // A valid MBF1 header whose num_blocks claims the max body (128 MB), but
    // the actual blob is only a header -- the body-length check alone rejects
    // it. Here we assert the *whole-blob* upper bound triggers for a blob that
    // physically exceeds header + kMaxFilterBytes.
    const size_t kHeaderSize = SplitBlockBloomFilterView::kHeaderSize;
    const uint64_t kMaxFilterBytes = SplitBlockBloomFilterView::kMaxFilterBytes;

    // Build a syntactically-plausible but oversized blob: header + a body one
    // block larger than the maximum. We do not need real filter bits; the size
    // guard fires before any body byte is read.
    std::string oversized(kHeaderSize + kMaxFilterBytes + 32, '\0');
    oversized[0] = 'M';
    oversized[1] = 'B';
    oversized[2] = 'F';
    oversized[3] = '1';
    oversized[4] = 1;  // version
    oversized[6] = 1;  // algo

    auto code = CatchSegcoreErrorCode(
        [&] { SplitBlockBloomFilterView::Parse(oversized); });
    EXPECT_EQ(code, milvus::ErrorCode::ExprInvalid)
        << "oversized envelope must be rejected as an input error";

    // A blob exactly at the max envelope size is not rejected by the size
    // guard (it fails later on body-length/num_blocks, but NOT the size cap);
    // confirm the size guard boundary is inclusive-safe by checking a blob one
    // byte over the cap is the smallest thing the guard rejects on size alone.
    std::string just_over(kHeaderSize + kMaxFilterBytes + 1, '\0');
    just_over[0] = 'M';
    just_over[1] = 'B';
    just_over[2] = 'F';
    just_over[3] = '1';
    just_over[4] = 1;
    just_over[6] = 1;
    auto code2 = CatchSegcoreErrorCode(
        [&] { SplitBlockBloomFilterView::Parse(just_over); });
    EXPECT_EQ(code2, milvus::ErrorCode::ExprInvalid);
}

// ---------------------------------------------------------------------------
// Reverse-lookup cost gate: bloom_match's index-only fallback drives the
// scalar index's Reverse_Lookup once per row. A BITMAP index WITHOUT its
// offset cache reverse-looks-up in O(cardinality) per row, so evaluating over
// an index-only field whose only index is a bare BITMAP would cost
// O(rows * cardinality) -- a silent quadratic scan. IndexSupportsReverseLookup()
// (via ScalarIndex<T>::SupportFastReverseLookup(), which BitmapIndex overrides
// to return use_offset_cache_) therefore rejects such an index, so
// DetermineExecPath() stays on RawData; and since the field is index-only
// (no raw data), ExecVisitorImpl must throw a SegcoreError rather than scan.
//
// This mirrors Int64SealedScalarIndexOnly exactly, with two differences:
// (a) a BITMAP index (built WITHOUT an offset cache -- the in-memory
//     Build(N, values, valid) path never calls BuildOffsetCache(), so
//     use_offset_cache_ stays false) instead of STL_SORT, and
// (b) the eval must THROW (the reject path) instead of matching a reference.
TEST_F(BloomFilterExprEvalTest, Int64SealedBitmapIndexOnlyRejected) {
    // Load every field EXCEPT age64 (i64_fid_) into the sealed segment, so
    // age64 has no raw field data; then attach a BITMAP index for age64.
    std::vector<int64_t> load_fields;
    for (auto& f : schema_->get_fields()) {
        if (f.first != i64_fid_) {
            load_fields.push_back(f.first.get());
        }
    }
    auto sealed = CreateSealedWithFieldDataLoaded(
        schema_, *dataset_, false, GetExcludedFieldIds(schema_, load_fields));

    // Sanity: the field is genuinely index-only (no raw data) before we load
    // the index, and stays that way after.
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));

    LoadIndexInfo idx;
    idx.field_id = i64_fid_.get();
    idx.field_type = DataType::INT64;
    // A bare BITMAP index: the default in-memory Build does NOT enable the
    // offset cache (use_offset_cache_ stays false), so its per-row
    // Reverse_Lookup is O(cardinality) and SupportFastReverseLookup() == false.
    idx.index_params["index_type"] = "BITMAP";
    auto index = std::make_unique<index::BitmapIndex<int64_t>>();
    index->Build(N, i64_col_.data(), i64_valid_.data());
    // GenIndexParams reads index->Type(), which BitmapIndex sets to "BITMAP".
    idx.index_params = GenIndexParams(index.get());
    idx.cache_index = CreateTestCacheIndex("test", std::move(index));
    sealed->LoadIndex(idx);
    ASSERT_FALSE(sealed->HasFieldData(i64_fid_));
    ASSERT_TRUE(sealed->HasIndex(i64_fid_));

    // Members: values of every third valid row (same construction as the
    // sibling index-only test, so the SBBF blob is built identically).
    TestSbbfBuilder builder(64);
    std::unordered_set<int64_t> member_values;
    for (size_t i = 0; i < N; i += 3) {
        if (i64_valid_[i]) {
            builder.AddInt64(i64_col_[i]);
            member_values.insert(i64_col_[i]);
        }
    }
    ASSERT_FALSE(member_values.empty());
    const std::string blob = builder.Serialize(member_values.size(), 0.001);

    // The only index on this index-only field is a BITMAP without an offset
    // cache, so bloom_match must NOT fall back to a per-row Reverse_Lookup
    // scan; with no raw field data to read either, ExecVisitorImpl throws a
    // retriable FieldNotLoaded SegcoreError ("bloom_match cannot evaluate
    // field ... offset cache").
    expr::ColumnInfo column(i64_fid_, DataType::INT64, {}, true);
    auto typed_expr = std::make_shared<expr::BloomFilterExpr>(column, blob);
    auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
    auto code = CatchSegcoreErrorCode(
        [&] { query::ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    EXPECT_EQ(code, milvus::ErrorCode::FieldNotLoaded);
}

// JSON paths have no scalar-index reverse-lookup path. If a sealed segment
// does not have the raw JSON field loaded, the valid request must fail as a
// retriable load-state error, not as a permanent UnexpectedError.
TEST_F(BloomFilterExprEvalTest, JsonSealedWithoutRawDataIsFieldNotLoaded) {
    std::vector<int64_t> load_fields;
    for (auto& f : schema_->get_fields()) {
        if (f.first != json_fid_) {
            load_fields.push_back(f.first.get());
        }
    }
    auto sealed = CreateSealedWithFieldDataLoaded(
        schema_, *dataset_, false, GetExcludedFieldIds(schema_, load_fields));
    ASSERT_FALSE(sealed->HasFieldData(json_fid_));

    TestSbbfBuilder builder(1);
    builder.AddInt64(5);
    const std::string blob = builder.Serialize(1, 0.001);

    expr::ColumnInfo column(
        json_fid_, DataType::JSON, std::vector<std::string>{"uid"}, true);
    auto typed_expr = std::make_shared<expr::BloomFilterExpr>(column, blob);
    auto plan = milvus::test::CreateRetrievePlanByExpr(typed_expr);
    auto code = CatchSegcoreErrorCode(
        [&] { query::ExecuteQueryExpr(plan, sealed.get(), N, MAX_TIMESTAMP); });
    EXPECT_EQ(code, milvus::ErrorCode::FieldNotLoaded);
}

// ---------------------------------------------------------------------------
// Cacheability: a predicate containing bloom_match must be excluded from the
// FilterBitsNode result cache. bloom_match's ToString cache key is a slim summary
// that cannot distinguish two distinct blobs of equal length + declared count, so
// caching the predicate could reuse another query's bitmap and silently return
// wrong rows. PhyBloomFilterExpr::IsCacheable() returns false, and it PROPAGATES:
// any parent expression that contains it is non-cacheable too (the base class ANDs
// over children).
// ---------------------------------------------------------------------------
TEST_F(BloomFilterExprEvalTest, PredicateContainingBloomIsNotCacheable) {
    auto sealed = BuildSealed();
    auto is_cacheable = [&](const expr::TypedExprPtr& logical) {
        auto qc = std::make_shared<QueryContext>(
            DEAFULT_QUERY_ID, sealed.get(), N, MAX_TIMESTAMP);
        ExecContext ctx(qc.get());
        auto compiled = CompileExpressions({logical}, &ctx, {}, false);
        EXPECT_EQ(compiled.size(), 1u);
        // IsCacheable() is a pure structural check over the compiled expr tree
        // (no segment/context access), so it is valid to read after qc is gone.
        return compiled[0]->IsCacheable();
    };

    const expr::ColumnInfo col(i64_fid_, DataType::INT64, {}, true);

    // Baseline: a plain range predicate is cacheable (the default) — its ToString
    // faithfully captures its identity.
    proto::plan::GenericValue v;
    v.set_int64_val(42);
    expr::TypedExprPtr range = std::make_shared<expr::UnaryRangeFilterExpr>(
        col, proto::plan::OpType::LessThan, v);
    EXPECT_TRUE(is_cacheable(range))
        << "a plain range predicate must be cacheable";

    // bloom_match itself is not cacheable.
    TestSbbfBuilder builder(4);
    builder.AddInt64(42);
    const std::string blob = builder.Serialize(/*n_declared=*/1, 0.001);
    expr::TypedExprPtr bloom =
        std::make_shared<expr::BloomFilterExpr>(col, blob);
    EXPECT_FALSE(is_cacheable(bloom)) << "bloom_match must not be cacheable";

    // Non-cacheability propagates through a composite: `not bloom_match` (and by
    // the same child recursion, any AND/OR that contains bloom_match) is excluded.
    expr::TypedExprPtr neg = std::make_shared<expr::LogicalUnaryExpr>(
        expr::LogicalUnaryExpr::OpType::LogicalNot, bloom);
    EXPECT_FALSE(is_cacheable(neg))
        << "a predicate containing bloom_match must not be cacheable";
}
