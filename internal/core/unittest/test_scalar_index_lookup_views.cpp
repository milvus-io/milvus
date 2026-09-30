// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. Licensed under the Apache License, Version 2.0.

#include <gtest/gtest.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <future>
#include <numeric>
#include <string>
#include <vector>

#include "index/HybridScalarIndex.h"
#include "index/ScalarIndexSort.h"
#include "index/StringIndexMarisa.h"
#include "index/StringIndexSort.h"
#include "exec/expression/Expr.h"
#include "exec/AnnFusingPolicy.h"
#include "exec/expression/ConjunctExpr.h"
#include "segcore/ChunkedSegmentSealedImpl.h"
#include "storage/LocalChunkManagerSingleton.h"

namespace milvus::index {
namespace {

// The runner verifies provenance and SHA256 before exporting this immutable
// original-ID / approved-VARCHAR fixture. Never generate replacement values.
class ScalarIndexLookupViewsTest : public testing::Test {
 protected:
    void
    SetUp() override {
        const char* path = std::getenv("MILVUS_LOOKUP_VIEWS_FIXTURE");
        if (path == nullptr) {
            GTEST_SKIP() << "verified original-data fixture not configured";
        }
        std::ifstream input(path);
        ASSERT_TRUE(input.good());
        for (std::string line; std::getline(input, line);) {
            auto row = nlohmann::json::parse(line);
            ids_.push_back(row.at(0).get<int64_t>());
            strings_.push_back(row.at(1).get<std::string>());
        }
        ASSERT_EQ(ids_.size(), 65536);
        ASSERT_EQ(strings_.size(), ids_.size());
    }

    // Query permutations/repeated offsets do not duplicate source records.
    std::vector<int64_t>
    Offsets(size_t count) const {
        std::vector<int64_t> result(count);
        for (size_t i = 0; i < count; ++i) {
            result[i] = (i * 6143 + 17) % ids_.size();
        }
        if (count > 1) {
            result.back() = result.front();
        }
        return result;
    }

    template <typename T>
    void
    CheckBatch(ScalarIndex<T>& index, const std::vector<T>& expected) {
        for (size_t count : {0, 1, 2, 3, 31, 32, 33, 63, 64, 65, 257, 1025}) {
            auto offsets = Offsets(count);
            for (int order = 0; order < 2; ++order) {
                auto batch = index.Reverse_LookupViews(offsets);
                auto moved = std::move(batch);
                auto assigned = ScalarIndexLookupViews<T>(0);
                assigned = std::move(moved);
                ASSERT_EQ(assigned.size(), count);
                for (size_t i = 0; i < count; ++i) {
                    auto single = index.Reverse_Lookup(offsets[i]);
                    ASSERT_EQ(assigned.is_valid(i), single.has_value());
                    ASSERT_TRUE(single.has_value());
                    EXPECT_EQ(assigned.values()[i], *single);
                    EXPECT_EQ(assigned.values()[i], expected[offsets[i]]);
                }
                std::reverse(offsets.begin(), offsets.end());
            }
        }
    }

    std::vector<int64_t> ids_;
    std::vector<std::string> strings_;
};

TEST_F(ScalarIndexLookupViewsTest, NumericNativeAndDefault) {
    ScalarIndexSort<int64_t> index;
    index.Build(ids_.size(), ids_.data());
    CheckBatch<int64_t>(index, ids_);
    auto offsets = Offsets(257);
    auto fallback = index.ScalarIndex<int64_t>::Reverse_LookupViews(offsets);
    for (size_t i = 0; i < offsets.size(); ++i) {
        ASSERT_TRUE(fallback.is_valid(i));
        EXPECT_EQ(fallback.values()[i], ids_[offsets[i]]);
    }
    std::vector<int64_t> invalid{-1};
    EXPECT_ANY_THROW(index.Reverse_LookupViews(invalid));
    invalid[0] = ids_.size();
    EXPECT_ANY_THROW(index.Reverse_LookupViews(invalid));
}

TEST_F(ScalarIndexLookupViewsTest, StringNativeBorrowsAndDefaultOwns) {
    StringIndexSort index;
    index.Build(strings_.size(), strings_.data());
    CheckBatch<std::string>(index, strings_);
    auto offsets = Offsets(65);
    auto first = index.Reverse_LookupViews(offsets);
    auto second = index.Reverse_LookupViews(offsets);
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(first.values()[i].data(), second.values()[i].data());
    }
    auto fallback = [&] {
        StringIndexSort temporary;
        temporary.Build(strings_.size(), strings_.data());
        return temporary.ScalarIndex<std::string>::Reverse_LookupViews(offsets);
    }();
    // Unlike native views, default decoded values survive the source index.
    auto moved = std::move(fallback);
    for (size_t i = 0; i < offsets.size(); ++i) {
        ASSERT_TRUE(moved.is_valid(i));
        EXPECT_EQ(moved.values()[i], strings_[offsets[i]]);
        EXPECT_NE(first.values()[i].data(), moved.values()[i].data());
    }
}

TEST_F(ScalarIndexLookupViewsTest, NativeBatchDoesNotDispatchSingleRows) {
    class CountingIndex : public StringIndexSort {
     public:
        std::optional<std::string>
        Reverse_Lookup(size_t offset) const override {
            ++single_calls;
            return StringIndexSort::Reverse_Lookup(offset);
        }
        mutable size_t single_calls = 0;
    } index;
    index.Build(strings_.size(), strings_.data());
    auto offsets = Offsets(65);
    auto native = index.Reverse_LookupViews(offsets);
    EXPECT_EQ(index.single_calls, 0);
    auto fallback =
        index.ScalarIndex<std::string>::Reverse_LookupViews(offsets);
    EXPECT_EQ(index.single_calls, offsets.size());
    for (size_t i = 0; i < offsets.size(); ++i) {
        EXPECT_EQ(native.values()[i], fallback.values()[i]);
    }
}

TEST_F(ScalarIndexLookupViewsTest, MmapStringViews) {
    StringIndexSort memory;
    memory.Build(strings_.size(), strings_.data());
    auto serialized = memory.Serialize({});
    char directory[] = "/tmp/milvus-lookup-views-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    Config config;
    config[MMAP_FILE_PATH] = std::string(directory) + "/index";
    StringIndexSort mapped;
    mapped.Load(serialized, config);
    CheckBatch<std::string>(mapped, strings_);
}

TEST_F(ScalarIndexLookupViewsTest, MarisaCompatibilityOwnsDecodedValues) {
    auto offsets = Offsets(257);
    auto batch = [&] {
        StringIndexMarisa index;
        index.Build(strings_.size(), strings_.data());
        CheckBatch<std::string>(index, strings_);
        return index.Reverse_LookupViews(offsets);
    }();
    auto moved = std::move(batch);
    for (size_t i = 0; i < offsets.size(); ++i) {
        ASSERT_TRUE(moved.is_valid(i));
        EXPECT_EQ(moved.values()[i], strings_[offsets[i]]);
    }
}

TEST_F(ScalarIndexLookupViewsTest, HybridDelegates) {
    HybridScalarIndex<int64_t> numbers(7);
    numbers.Build(ids_.size(), ids_.data());
    CheckBatch<int64_t>(numbers, ids_);
    HybridScalarIndex<std::string> strings(7);
    strings.Build(strings_.size(), strings_.data());
    CheckBatch<std::string>(strings, strings_);
}

TEST_F(ScalarIndexLookupViewsTest, IndependentConcurrentBatches) {
    StringIndexSort index;
    index.Build(strings_.size(), strings_.data());
    std::vector<std::future<bool>> tasks;
    for (int worker = 0; worker < 4; ++worker) {
        tasks.push_back(std::async(std::launch::async, [&, worker] {
            auto offsets = Offsets(31 + worker);
            for (int batch = 0; batch < 100; ++batch) {
                auto values = index.Reverse_LookupViews(offsets);
                for (size_t i = 0; i < offsets.size(); ++i) {
                    if (!values.is_valid(i) ||
                        values.values()[i] != strings_[offsets[i]]) {
                        return false;
                    }
                }
                std::reverse(offsets.begin(), offsets.end());
            }
            return true;
        }));
    }
    for (auto& task : tasks) {
        EXPECT_TRUE(task.get());
    }
}

TEST_F(ScalarIndexLookupViewsTest, ExprMaskAndBatchCursor) {
    // The focused test runner does not use unittest/init_gtest.cpp. Initialize
    // the manager required by an empty sealed segment; an already initialized
    // full-suite manager keeps its existing configuration.
    char directory[] = "/tmp/milvus-views-expr-XXXXXX";
    ASSERT_NE(mkdtemp(directory), nullptr);
    storage::LocalChunkManagerSingleton::GetInstance().Init(directory);
    storage::MmapConfig config{};
    config.cache_read_ahead_policy = "willneed";
    config.mmap_path = directory;
    config.disk_limit = 512 * 1024 * 1024;
    config.fix_file_size = 4 * 1024 * 1024;
    storage::MmapManager::GetInstance().Init(config);
    class RecordingIndex : public StringIndexSort {
     public:
        ScalarIndexLookupViews<std::string>
        Reverse_LookupViews(ScalarIndexOffsets offsets) const override {
            read_offsets.insert(
                read_offsets.end(), offsets.begin(), offsets.end());
            largest_batch = std::max(largest_batch, offsets.size());
            return StringIndexSort::Reverse_LookupViews(offsets);
        }
        mutable std::vector<int64_t> read_offsets;
        mutable size_t largest_batch = 0;
    } index;
    index.Build(strings_.size(), strings_.data());
    auto schema = std::make_shared<Schema>();
    auto field = schema->AddDebugField("frozen_text", DataType::VARCHAR);
    auto segment = segcore::CreateSealedSegment(schema);
    class Reader : public exec::SegmentExpr {
     public:
        Reader(const segcore::SegmentInternalInterface* segment,
               FieldId field,
               const IndexBase* index)
            : SegmentExpr({},
                          "lookup-views-test",
                          nullptr,
                          segment,
                          field,
                          {},
                          DataType::VARCHAR,
                          65536,
                          1024,
                          0) {
            // Isolated test index is owned by the enclosing test, not a cache
            // cell. Production continues to use EnsurePinnedIndex().
            pinned_index_.emplace_back(index);
            num_index_chunk_ = 1;
        }
    } reader(segment.get(), field, &index);
    for (int mode : {0, 1, 2}) {
        auto schedule = Offsets(257);
        exec::OffsetVector offsets(schedule.begin(), schedule.end());
        TargetBitmap mask(offsets.size(), false);
        std::vector<int64_t> expected;
        for (size_t i = 0; i < offsets.size(); ++i) {
            bool active = mode == 0 || (mode == 2 && i % 3 != 1);
            mask[i] = active;
            if (active) {
                expected.push_back(offsets[i]);
            } else {
                offsets[i] = -1;  // Inactive offsets must never be read.
            }
        }
        TargetBitmap result(offsets.size(), true), valid(offsets.size(), true);
        size_t cursor = 0;
        auto consume = [&]<exec::FilterType filter_type>(
                           const std::string_view* values,
                           ValidityView validity,
                           const int32_t*,
                           int count,
                           TargetBitmapView output,
                           TargetBitmapView output_valid) {
            EXPECT_EQ(filter_type, exec::FilterType::random);
            for (int lane = 0; lane < count; ++lane) {
                EXPECT_EQ(values != nullptr, bool(mask[cursor + lane]));
                if (values != nullptr) {
                    EXPECT_EQ(values[lane], strings_[offsets[cursor + lane]]);
                    EXPECT_TRUE(!validity || validity[lane]);
                    output[lane] = false;
                    output_valid[lane] = true;
                }
            }
            cursor += count;
        };
        index.read_offsets.clear();
        index.largest_batch = 0;
        exec::FilterDiagnostics profile;
        {
            exec::FilterDiagnosticScope scope(&profile);
            EXPECT_EQ(
                reader.ProcessIndexLookupByOffsetsWithMask<std::string_view>(
                    consume, &offsets, result.view(), valid.view(), mask),
                offsets.size());
        }
        EXPECT_EQ(profile.index_path_rows, offsets.size());
        EXPECT_EQ(profile.index_read_rows, expected.size());
        EXPECT_EQ(profile.raw_path_rows, 0);
        EXPECT_GE(profile.index_path_ns, profile.index_read_ns);
        if (!expected.empty())
            EXPECT_GT(profile.index_read_ns, 0);
        EXPECT_EQ(exec::active_filter_diagnostics, nullptr);
        EXPECT_EQ(cursor, offsets.size());
        EXPECT_EQ(index.read_offsets, expected);
        if (mode == 0) {
            EXPECT_EQ(index.largest_batch, 64);
        }
        for (size_t i = 0; i < offsets.size(); ++i) {
            EXPECT_EQ(bool(result[i]), !bool(mask[i]));
            EXPECT_TRUE(valid[i]);
        }
    }
}

}  // namespace
}  // namespace milvus::index

namespace milvus::exec {
namespace {
class MetadataProbeExpr : public Expr {
 public:
    MetadataProbeExpr() : Expr(DataType::BOOL, {}, "metadata-probe", nullptr) {
    }
    FilterSourceInfo
    DescribeFilterSource() const override {
        ++described;
        return Expr::DescribeFilterSource();
    }
    mutable int described{0};
};

class OffsetMetadataProbeExpr : public MetadataProbeExpr {
 public:
    bool
    SupportOffsetInput() override {
        return true;
    }
};

TEST(AnnFusingPolicyFacts, ExecutionAndPolicyAreIndependent) {
    MetadataProbeExpr unknown;
    EXPECT_FALSE(unknown.SupportOffsetInput());
    EXPECT_FALSE(unknown.ConsiderAnnFusing(AnnFilterFusingRequest::Auto));
    EXPECT_EQ(unknown.described, 0);
    OffsetMetadataProbeExpr supported;
    supported.SetExprType(proto::plan::Expr::kTermExpr);
    auto facts = supported.DescribeFilterSource();
    EXPECT_EQ(facts.expr_type, proto::plan::Expr::kTermExpr);
    EXPECT_EQ(facts.data_type, DataType::NONE);
    supported.described = 0;
    EXPECT_FALSE(supported.ConsiderAnnFusing(AnnFilterFusingRequest::Baseline));
    EXPECT_EQ(supported.described, 0);
    // Forcing must not inspect policy facts or bypass actual offset capability.
    EXPECT_TRUE(
        supported.ConsiderAnnFusing(AnnFilterFusingRequest::ExplicitFusing));
    EXPECT_EQ(supported.described, 0);
    EXPECT_FALSE(
        unknown.ConsiderAnnFusing(AnnFilterFusingRequest::ExplicitFusing));
    for (bool is_and : {true, false}) {
        std::vector<ExprPtr> inputs{std::make_shared<OffsetMetadataProbeExpr>(),
                                    std::make_shared<MetadataProbeExpr>()};
        PhyConjunctFilterExpr combined(std::move(inputs), is_and, nullptr);
        EXPECT_FALSE(combined.SupportOffsetInput());
        EXPECT_FALSE(
            combined.ConsiderAnnFusing(AnnFilterFusingRequest::ExplicitFusing));
    }
}

TEST(AnnFusingPolicyFacts, UnknownIndexIsNotNoIndex) {
    EXPECT_EQ(index::ParseScalarIndexType("STL_SORT"),
              index::ScalarIndexType::STLSORT);
    EXPECT_EQ(index::ParseScalarIndexType("NONE"),
              index::ScalarIndexType::NONE);
    EXPECT_EQ(index::ParseScalarIndexType("future-index"),
              index::ScalarIndexType::UNKNOWN);
    EXPECT_EQ(index::FromString("future-index"), index::ScalarIndexType::NONE);
}

TEST(AnnFusingPolicyFacts, RejectLegacyPlugin) {
    const char* legacy = std::getenv("MILVUS_TEST_LEGACY_ANN_PLUGIN");
    if (!legacy) {
        GTEST_SKIP() << "frozen previous plugin not configured";
    }
    // No V4 symbol in the frozen V3 DSO: reject before interpreting its structs.
    EXPECT_FALSE(AnnFusingPolicy::Initialize(legacy, "/unused-legacy-config"));
    EXPECT_FALSE(AnnFusingPolicy::Instance().available());
}
}  // namespace
}  // namespace milvus::exec
