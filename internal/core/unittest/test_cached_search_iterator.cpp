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

#include <gtest/gtest.h>
#include <memory>
#include <random>
#include <unordered_set>
#include "common/BitsetView.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Utils.h"
#include "index/Index.h"
#include "knowhere/comp/index_param.h"
#include "query/CachedSearchIterator.h"
#include "index/VectorIndex.h"
#include "index/IndexFactory.h"
#include "knowhere/dataset.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/DataGen.h"
#include "test_cachinglayer/cachinglayer_test_utils.h"

using namespace milvus;
using namespace milvus::query;
using namespace milvus::segcore;
using namespace milvus::index;

namespace {
constexpr int64_t kDim = 16;
constexpr int64_t kNumVectors = 1000;
constexpr int64_t kNumQueries = 1;
constexpr int64_t kBatchSize = 100;
constexpr size_t kSizePerChunk = 128;
constexpr size_t kHnswM = 24;
constexpr size_t kHnswEfConstruction = 360;
constexpr size_t kHnswEf = 128;

const MetricType kMetricType = knowhere::metric::L2;
}  // namespace

enum class ConstructorType {
    VectorIndex = 0,
    RawData,
    VectorBase,
    ChunkedColumn
};

static const std::vector<ConstructorType> kConstructorTypes = {
    ConstructorType::VectorIndex,
    ConstructorType::RawData,
    ConstructorType::VectorBase,
    ConstructorType::ChunkedColumn,
};

static const std::vector<MetricType> kMetricTypes = {
    knowhere::metric::L2,
    knowhere::metric::IP,
    knowhere::metric::COSINE,
};

// this class does not support test concurrently
class CachedSearchIteratorTest
    : public ::testing::TestWithParam<std::tuple<ConstructorType, MetricType>> {
 private:
 protected:
    SearchInfo
    GetDefaultNormalSearchInfo() {
        return SearchInfo{
            .topk_ = kBatchSize,
            .round_decimal_ = -1,
            .metric_type_ = std::get<1>(GetParam()),
            .search_params_ =
                {
                    {knowhere::indexparam::EF, std::to_string(kHnswEf)},
                },
            .iterator_v2_info_ =
                SearchIteratorV2Info{
                    .batch_size = kBatchSize,
                },
        };
    }

    static DataType data_type_;
    static int64_t dim_;
    static int64_t nb_;
    static int64_t nq_;
    static FixedVector<float> base_dataset_;
    static FixedVector<float> query_dataset_;
    static IndexBasePtr index_hnsw_l2_;
    static IndexBasePtr index_hnsw_ip_;
    static IndexBasePtr index_hnsw_cos_;
    static knowhere::DataSetPtr knowhere_query_dataset_;
    static dataset::SearchDataset search_dataset_;
    static std::unique_ptr<ConcurrentVector<milvus::FloatVector>> vector_base_;
    static std::shared_ptr<ChunkedColumn> column_;
    static std::vector<std::vector<char>> column_data_;
    static std::shared_ptr<Schema> schema_;
    static FieldId fakevec_id_;

    IndexBase* index_hnsw_ = nullptr;
    MetricType metric_type_ = kMetricType;

    std::unique_ptr<CachedSearchIterator>
    DispatchIterator(const ConstructorType& constructor_type,
                     const SearchInfo& search_info,
                     const BitsetView& bitset) {
        switch (constructor_type) {
            case ConstructorType::VectorIndex:
                return std::make_unique<CachedSearchIterator>(
                    dynamic_cast<const VectorIndex&>(*index_hnsw_),
                    knowhere_query_dataset_,
                    search_info,
                    bitset);

            case ConstructorType::RawData:
                return std::make_unique<CachedSearchIterator>(
                    search_dataset_,
                    dataset::RawDataset{0, dim_, nb_, base_dataset_.data()},
                    search_info,
                    std::map<std::string, std::string>{},
                    bitset,
                    data_type_);

            case ConstructorType::VectorBase:
                return std::make_unique<CachedSearchIterator>(
                    search_dataset_,
                    vector_base_.get(),
                    nb_,
                    search_info,
                    std::map<std::string, std::string>{},
                    bitset,
                    data_type_);

            case ConstructorType::ChunkedColumn:
                return std::make_unique<CachedSearchIterator>(
                    column_.get(),
                    search_dataset_,
                    search_info,
                    std::map<std::string, std::string>{},
                    bitset,
                    data_type_);
            default:
                return nullptr;
        }
    }

    // use last distance of the first batch as range_filter
    // use first distance of the last batch as radius
    std::pair<float, float>
    GetRadiusAndRangeFilter() {
        const size_t num_rnds = (nb_ + kBatchSize - 1) / kBatchSize;
        SearchResult search_result;
        float radius, range_filter;
        bool get_radius_success = false;
        bool get_range_filter_sucess = false;
        SearchInfo search_info = GetDefaultNormalSearchInfo();
        auto iterator =
            DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
        for (size_t rnd = 0; rnd < num_rnds; ++rnd) {
            iterator->NextBatch(search_info, search_result);
            if (rnd == 0) {
                for (size_t i = kBatchSize - 1; i >= 0; --i) {
                    if (search_result.seg_offsets_[i] != -1) {
                        range_filter = search_result.distances_[i];
                        get_range_filter_sucess = true;
                        break;
                    }
                }
            } else {
                for (size_t i = 0; i < kBatchSize; ++i) {
                    if (search_result.seg_offsets_[i] != -1) {
                        radius = search_result.distances_[i];
                        get_radius_success = true;
                        break;
                    }
                }
            }
        }
        if (!get_radius_success || !get_range_filter_sucess) {
            throw std::runtime_error("Failed to get radius and range filter");
        }
        return {radius, range_filter};
    }

    static void
    BuildIndex() {
        auto dataset = knowhere::GenDataSet(nb_, dim_, base_dataset_.data());

        for (const auto& metric_type : kMetricTypes) {
            milvus::index::CreateIndexInfo create_index_info;
            create_index_info.field_type = data_type_;
            create_index_info.metric_type = metric_type;
            create_index_info.index_engine_version =
                knowhere::Version::GetCurrentVersion().VersionNumber();
            auto build_conf = knowhere::Json{
                {knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
                {knowhere::meta::DIM, std::to_string(dim_)},
                {knowhere::indexparam::M, std::to_string(kHnswM)},
                {knowhere::indexparam::EFCONSTRUCTION,
                 std::to_string(kHnswEfConstruction)}};
            create_index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
            if (metric_type == knowhere::metric::L2) {
                index_hnsw_l2_ =
                    milvus::index::IndexFactory::GetInstance().CreateIndex(
                        create_index_info,
                        milvus::storage::FileManagerContext());
                index_hnsw_l2_->BuildWithDataset(dataset, build_conf);
                ASSERT_EQ(index_hnsw_l2_->Count(), nb_);
            } else if (metric_type == knowhere::metric::IP) {
                index_hnsw_ip_ =
                    milvus::index::IndexFactory::GetInstance().CreateIndex(
                        create_index_info,
                        milvus::storage::FileManagerContext());
                index_hnsw_ip_->BuildWithDataset(dataset, build_conf);
                ASSERT_EQ(index_hnsw_ip_->Count(), nb_);
            } else if (metric_type == knowhere::metric::COSINE) {
                index_hnsw_cos_ =
                    milvus::index::IndexFactory::GetInstance().CreateIndex(
                        create_index_info,
                        milvus::storage::FileManagerContext());
                index_hnsw_cos_->BuildWithDataset(dataset, build_conf);
                ASSERT_EQ(index_hnsw_cos_->Count(), nb_);
            } else {
                FAIL() << "Unsupported metric type: " << metric_type;
            }
        }
    }

    static void
    SetUpVectorBase() {
        vector_base_ = std::make_unique<ConcurrentVector<milvus::FloatVector>>(
            dim_, kSizePerChunk);
        vector_base_->set_data_raw(0, base_dataset_.data(), nb_);

        ASSERT_EQ(vector_base_->num_chunk(),
                  (nb_ + kSizePerChunk - 1) / kSizePerChunk);
    }

    static void
    SetUpChunkedColumn() {
        auto field_meta = schema_->operator[](fakevec_id_);
        const size_t num_chunks_ = (nb_ + kSizePerChunk - 1) / kSizePerChunk;
        column_data_.resize(num_chunks_);

        size_t offset = 0;
        std::vector<std::unique_ptr<Chunk>> chunks;
        std::vector<int64_t> num_rows_per_chunk;
        for (size_t i = 0; i < num_chunks_; ++i) {
            const size_t rows =
                std::min(static_cast<size_t>(nb_ - offset), kSizePerChunk);
            num_rows_per_chunk.push_back(rows);
            const size_t buf_size = rows * dim_ * sizeof(float);
            auto& chunk_data = column_data_[i];
            chunk_data.resize(buf_size);
            memcpy(chunk_data.data(),
                   base_dataset_.cbegin() + offset * dim_,
                   rows * dim_ * sizeof(float));
            chunks.emplace_back(std::make_unique<FixedWidthChunk>(
                rows, dim_, chunk_data.data(), buf_size, sizeof(float), false));
            offset += rows;
        }
        auto translator = std::make_unique<TestChunkTranslator>(
            num_rows_per_chunk, "", std::move(chunks));
        column_ =
            std::make_shared<ChunkedColumn>(std::move(translator), field_meta);
    }

    static void
    SetUpTestSuite() {
        schema_ = std::make_shared<Schema>();
        fakevec_id_ = schema_->AddDebugField(
            "fakevec", DataType::VECTOR_FLOAT, dim_, kMetricType);

        // generate base dataset
        base_dataset_ =
            segcore::DataGen(schema_, nb_).get_col<float>(fakevec_id_);

        // generate query dataset
        query_dataset_ = {base_dataset_.cbegin(),
                          base_dataset_.cbegin() + nq_ * dim_};
        knowhere_query_dataset_ =
            knowhere::GenDataSet(nq_, dim_, query_dataset_.data());
        search_dataset_ = dataset::SearchDataset{
            .metric_type = kMetricType,
            .num_queries = nq_,
            .topk = kBatchSize,
            .round_decimal = -1,
            .dim = dim_,
            .query_data = query_dataset_.data(),
        };

        BuildIndex();
        SetUpVectorBase();
        SetUpChunkedColumn();
    }

    static void
    TearDownTestSuite() {
        base_dataset_.clear();
        query_dataset_.clear();
        index_hnsw_l2_.reset();
        index_hnsw_ip_.reset();
        index_hnsw_cos_.reset();
        knowhere_query_dataset_.reset();
        vector_base_.reset();
        column_.reset();
    }

    void
    SetUp() override {
        auto metric_type = std::get<1>(GetParam());
        if (metric_type == knowhere::metric::L2) {
            metric_type_ = knowhere::metric::L2;
            search_dataset_.metric_type = knowhere::metric::L2;
            index_hnsw_ = index_hnsw_l2_.get();
        } else if (metric_type == knowhere::metric::IP) {
            metric_type_ = knowhere::metric::IP;
            search_dataset_.metric_type = knowhere::metric::IP;
            index_hnsw_ = index_hnsw_ip_.get();
        } else if (metric_type == knowhere::metric::COSINE) {
            metric_type_ = knowhere::metric::COSINE;
            search_dataset_.metric_type = knowhere::metric::COSINE;
            index_hnsw_ = index_hnsw_cos_.get();
        } else {
            FAIL() << "Unsupported metric type: " << metric_type;
        }
    }

    void
    TearDown() override {
    }
};

// initialize static variables
DataType CachedSearchIteratorTest::data_type_ = DataType::VECTOR_FLOAT;
int64_t CachedSearchIteratorTest::dim_ = kDim;
int64_t CachedSearchIteratorTest::nb_ = kNumVectors;
int64_t CachedSearchIteratorTest::nq_ = kNumQueries;
IndexBasePtr CachedSearchIteratorTest::index_hnsw_l2_ = nullptr;
IndexBasePtr CachedSearchIteratorTest::index_hnsw_ip_ = nullptr;
IndexBasePtr CachedSearchIteratorTest::index_hnsw_cos_ = nullptr;
knowhere::DataSetPtr CachedSearchIteratorTest::knowhere_query_dataset_ =
    nullptr;
dataset::SearchDataset CachedSearchIteratorTest::search_dataset_;
FixedVector<float> CachedSearchIteratorTest::base_dataset_;
FixedVector<float> CachedSearchIteratorTest::query_dataset_;
std::unique_ptr<ConcurrentVector<milvus::FloatVector>>
    CachedSearchIteratorTest::vector_base_ = nullptr;
std::shared_ptr<ChunkedColumn> CachedSearchIteratorTest::column_ = nullptr;
std::vector<std::vector<char>> CachedSearchIteratorTest::column_data_;
std::shared_ptr<Schema> CachedSearchIteratorTest::schema_{nullptr};
FieldId CachedSearchIteratorTest::fakevec_id_(0);

/********* Testcases Start **********/

TEST_P(CachedSearchIteratorTest, NextBatchNormal) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    const std::vector<size_t> kBatchSizes = {
        1, 7, 43, 99, 100, 101, 1000, 1005};

    for (size_t batch_size : kBatchSizes) {
        std::cout << "batch_size: " << batch_size << std::endl;
        search_info.iterator_v2_info_->batch_size = batch_size;
        auto iterator =
            DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
        SearchResult search_result;

        iterator->NextBatch(search_info, search_result);

        for (size_t i = 0; i < nq_; ++i) {
            std::unordered_set<int64_t> seg_offsets;
            size_t cnt = 0;
            for (size_t j = 0; j < batch_size; ++j) {
                if (search_result.seg_offsets_[i * batch_size + j] == -1) {
                    break;
                }
                ++cnt;
                seg_offsets.insert(
                    search_result.seg_offsets_[i * batch_size + j]);
            }
            EXPECT_EQ(seg_offsets.size(), cnt);
            if (metric_type_ == knowhere::metric::L2) {
                EXPECT_EQ(search_result.distances_[i * batch_size], 0);
            }
        }
        EXPECT_EQ(search_result.unity_topK_, batch_size);
        EXPECT_EQ(search_result.total_nq_, nq_);
        EXPECT_EQ(search_result.seg_offsets_.size(), nq_ * batch_size);
        EXPECT_EQ(search_result.distances_.size(), nq_ * batch_size);
    }
}

TEST_P(CachedSearchIteratorTest, NextBatchDistBound) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    const size_t batch_size = kBatchSize;
    const float dist_bound_factor = PositivelyRelated(metric_type_) ? 0.5 : 1.5;
    float dist_bound = 0;

    {
        auto iterator =
            DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
        SearchResult search_result;
        iterator->NextBatch(search_info, search_result);

        bool found_dist_bound = false;
        // use the last distance of the first query * factor as the dist bound
        for (size_t j = batch_size - 1; j >= 0; --j) {
            if (search_result.seg_offsets_[j] != -1) {
                dist_bound = search_result.distances_[j] * dist_bound_factor;
                found_dist_bound = true;
                break;
            }
        }
        ASSERT_TRUE(found_dist_bound);

        search_info.iterator_v2_info_->last_bound = dist_bound;
        for (size_t rnd = 1; rnd < (nb_ + batch_size - 1) / batch_size; ++rnd) {
            iterator->NextBatch(search_info, search_result);
            for (size_t i = 0; i < nq_; ++i) {
                for (size_t j = 0; j < batch_size; ++j) {
                    if (search_result.seg_offsets_[i * batch_size + j] == -1) {
                        break;
                    }
                    if (PositivelyRelated(metric_type_)) {
                        EXPECT_LT(search_result.distances_[i * batch_size + j],
                                  dist_bound);
                    } else {
                        EXPECT_GT(search_result.distances_[i * batch_size + j],
                                  dist_bound);
                    }
                }
            }
        }
    }
}

TEST_P(CachedSearchIteratorTest, NextBatchDistBoundEmptyResults) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    const size_t batch_size = kBatchSize;
    const float dist_bound = PositivelyRelated(metric_type_)
                                 ? -std::numeric_limits<float>::max()
                                 : std::numeric_limits<float>::max();

    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    SearchResult search_result;

    search_info.iterator_v2_info_->last_bound = dist_bound;
    size_t total_cnt = 0;
    for (size_t rnd = 0; rnd < (nb_ + batch_size - 1) / batch_size; ++rnd) {
        iterator->NextBatch(search_info, search_result);
        for (size_t i = 0; i < nq_; ++i) {
            for (size_t j = 0; j < batch_size; ++j) {
                if (search_result.seg_offsets_[i * batch_size + j] == -1) {
                    break;
                }
                ++total_cnt;
            }
        }
    }
    EXPECT_EQ(total_cnt, 0);
}

TEST_P(CachedSearchIteratorTest, NextBatchRangeSearchRadius) {
    const size_t num_rnds = (nb_ + kBatchSize - 1) / kBatchSize;
    const auto [radius, range_filter] = GetRadiusAndRangeFilter();
    SearchResult search_result;

    SearchInfo search_info = GetDefaultNormalSearchInfo();
    search_info.search_params_[knowhere::meta::RADIUS] = radius;

    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    for (size_t rnd = 0; rnd < num_rnds; ++rnd) {
        iterator->NextBatch(search_info, search_result);
        for (size_t i = 0; i < nq_; ++i) {
            for (size_t j = 0; j < kBatchSize; ++j) {
                if (search_result.seg_offsets_[i * kBatchSize + j] == -1) {
                    break;
                }
                float dist = search_result.distances_[i * kBatchSize + j];
                if (PositivelyRelated(metric_type_)) {
                    ASSERT_GT(dist, radius);
                } else {
                    ASSERT_LT(dist, radius);
                }
            }
        }
    }
}

TEST_P(CachedSearchIteratorTest, NextBatchRangeSearchRadiusAndRangeFilter) {
    const size_t num_rnds = (nb_ + kBatchSize - 1) / kBatchSize;
    const auto [radius, range_filter] = GetRadiusAndRangeFilter();
    SearchResult search_result;

    SearchInfo search_info = GetDefaultNormalSearchInfo();
    search_info.search_params_[knowhere::meta::RADIUS] = radius;
    search_info.search_params_[knowhere::meta::RANGE_FILTER] = range_filter;

    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    for (size_t rnd = 0; rnd < num_rnds; ++rnd) {
        iterator->NextBatch(search_info, search_result);
        for (size_t i = 0; i < nq_; ++i) {
            for (size_t j = 0; j < kBatchSize; ++j) {
                if (search_result.seg_offsets_[i * kBatchSize + j] == -1) {
                    break;
                }
                float dist = search_result.distances_[i * kBatchSize + j];
                if (PositivelyRelated(metric_type_)) {
                    ASSERT_GT(dist, radius);
                    ASSERT_LE(dist, range_filter);
                } else {
                    ASSERT_LT(dist, radius);
                    ASSERT_GE(dist, range_filter);
                }
            }
        }
    }
}

TEST_P(CachedSearchIteratorTest,
       NextBatchRangeSearchLastBoundRadiusRangeFilter) {
    const size_t num_rnds = (nb_ + kBatchSize - 1) / kBatchSize;
    const auto [radius, range_filter] = GetRadiusAndRangeFilter();
    SearchResult search_result;
    const float diff = (radius + range_filter) / 2;
    const std::vector<float> last_bounds = {radius - diff,
                                            radius,
                                            radius + diff,
                                            range_filter,
                                            range_filter + diff};

    SearchInfo search_info = GetDefaultNormalSearchInfo();
    search_info.search_params_[knowhere::meta::RADIUS] = radius;
    search_info.search_params_[knowhere::meta::RANGE_FILTER] = range_filter;
    for (float last_bound : last_bounds) {
        search_info.iterator_v2_info_->last_bound = last_bound;
        auto iterator =
            DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
        for (size_t rnd = 0; rnd < num_rnds; ++rnd) {
            iterator->NextBatch(search_info, search_result);
            for (size_t i = 0; i < nq_; ++i) {
                for (size_t j = 0; j < kBatchSize; ++j) {
                    if (search_result.seg_offsets_[i * kBatchSize + j] == -1) {
                        break;
                    }
                    float dist = search_result.distances_[i * kBatchSize + j];
                    if (PositivelyRelated(metric_type_)) {
                        ASSERT_LE(dist, last_bound);
                        ASSERT_GT(dist, radius);
                        ASSERT_LE(dist, range_filter);
                    } else {
                        ASSERT_GT(dist, last_bound);
                        ASSERT_LT(dist, radius);
                        ASSERT_GE(dist, range_filter);
                    }
                }
            }
        }
    }
}

TEST_P(CachedSearchIteratorTest, NextBatchZeroBatchSize) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    SearchResult search_result;

    search_info.iterator_v2_info_->batch_size = 0;
    EXPECT_THROW(iterator->NextBatch(search_info, search_result), SegcoreError);
}

TEST_P(CachedSearchIteratorTest, NextBatchDiffBatchSizeComparedToInit) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    SearchResult search_result;

    search_info.iterator_v2_info_->batch_size = kBatchSize + 1;
    EXPECT_THROW(iterator->NextBatch(search_info, search_result), SegcoreError);
}

TEST_P(CachedSearchIteratorTest, NextBatchEmptySearchInfo) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    SearchResult search_result;

    SearchInfo empty_search_info;
    EXPECT_THROW(iterator->NextBatch(empty_search_info, search_result),
                 SegcoreError);
}

TEST_P(CachedSearchIteratorTest, NextBatchEmptyIteratorV2Info) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    auto iterator =
        DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
    SearchResult search_result;

    search_info.iterator_v2_info_ = std::nullopt;
    EXPECT_THROW(iterator->NextBatch(search_info, search_result), SegcoreError);
}

TEST_P(CachedSearchIteratorTest, NextBatchtAllBatchesNormal) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    const std::vector<size_t> kBatchSizes = {
        1, 7, 43, 99, 100, 101, 1000, 1005};

    for (size_t batch_size : kBatchSizes) {
        search_info.iterator_v2_info_->batch_size = batch_size;
        auto iterator =
            DispatchIterator(std::get<0>(GetParam()), search_info, nullptr);
        size_t total_cnt = 0;

        for (size_t rnd = 0; rnd < (nb_ + batch_size - 1) / batch_size; ++rnd) {
            SearchResult search_result;
            iterator->NextBatch(search_info, search_result);
            for (size_t i = 0; i < nq_; ++i) {
                std::unordered_set<int64_t> seg_offsets;
                size_t cnt = 0;
                for (size_t j = 0; j < batch_size; ++j) {
                    if (search_result.seg_offsets_[i * batch_size + j] == -1) {
                        break;
                    }
                    ++cnt;
                    seg_offsets.insert(
                        search_result.seg_offsets_[i * batch_size + j]);
                }
                total_cnt += cnt;
                // check no duplicate
                EXPECT_EQ(seg_offsets.size(), cnt);

                // only check if the first distance of the first batch is 0
                if (rnd == 0 && metric_type_ == knowhere::metric::L2) {
                    EXPECT_EQ(search_result.distances_[i * batch_size], 0);
                }
            }
            EXPECT_EQ(search_result.unity_topK_, batch_size);
            EXPECT_EQ(search_result.total_nq_, nq_);
            EXPECT_EQ(search_result.seg_offsets_.size(), nq_ * batch_size);
            EXPECT_EQ(search_result.distances_.size(), nq_ * batch_size);
        }
        if (std::get<0>(GetParam()) == ConstructorType::VectorIndex) {
            EXPECT_GE(total_cnt, nb_ * nq_ * 0.9);
        } else {
            EXPECT_EQ(total_cnt, nb_ * nq_);
        }
    }
}

TEST_P(CachedSearchIteratorTest, ConstructorWithInvalidSearchInfo) {
    EXPECT_THROW(
        DispatchIterator(std::get<0>(GetParam()), SearchInfo{}, nullptr),
        SegcoreError);

    EXPECT_THROW(
        DispatchIterator(
            std::get<0>(GetParam()), SearchInfo{.metric_type_ = ""}, nullptr),
        SegcoreError);

    EXPECT_THROW(DispatchIterator(std::get<0>(GetParam()),
                                  SearchInfo{.metric_type_ = metric_type_},
                                  nullptr),
                 SegcoreError);

    EXPECT_THROW(DispatchIterator(std::get<0>(GetParam()),
                                  SearchInfo{.metric_type_ = metric_type_,
                                             .iterator_v2_info_ = {}},
                                  nullptr),
                 SegcoreError);

    EXPECT_THROW(
        DispatchIterator(std::get<0>(GetParam()),
                         SearchInfo{.metric_type_ = metric_type_,
                                    .iterator_v2_info_ =
                                        SearchIteratorV2Info{.batch_size = 0}},
                         nullptr),
        SegcoreError);
}

TEST_P(CachedSearchIteratorTest, ConstructorWithInvalidParams) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    if (std::get<0>(GetParam()) == ConstructorType::VectorIndex) {
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         dynamic_cast<const VectorIndex&>(*index_hnsw_),
                         nullptr,
                         search_info,
                         nullptr),
                     SegcoreError);

        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         dynamic_cast<const VectorIndex&>(*index_hnsw_),
                         std::make_shared<knowhere::DataSet>(),
                         search_info,
                         nullptr),
                     SegcoreError);
    } else if (std::get<0>(GetParam()) == ConstructorType::RawData) {
        EXPECT_THROW(
            auto iterator = std::make_unique<CachedSearchIterator>(
                dataset::SearchDataset{},
                dataset::RawDataset{0, dim_, nb_, base_dataset_.data()},
                search_info,
                std::map<std::string, std::string>{},
                nullptr,
                data_type_),
            SegcoreError);
    } else if (std::get<0>(GetParam()) == ConstructorType::VectorBase) {
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         dataset::SearchDataset{},
                         vector_base_.get(),
                         nb_,
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);

        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         search_dataset_,
                         nullptr,
                         nb_,
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);

        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         search_dataset_,
                         vector_base_.get(),
                         0,
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);
    } else if (std::get<0>(GetParam()) == ConstructorType::ChunkedColumn) {
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         nullptr,
                         search_dataset_,
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         column_.get(),
                         dataset::SearchDataset{},
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);
    }
}

/********* Testcases End **********/

INSTANTIATE_TEST_SUITE_P(
    CachedSearchIteratorTests,
    CachedSearchIteratorTest,
    ::testing::Combine(::testing::ValuesIn(kConstructorTypes),
                       ::testing::ValuesIn(kMetricTypes)),
    [](const testing::TestParamInfo<std::tuple<ConstructorType, MetricType>>&
           info) {
        std::string constructor_type_str;
        ConstructorType constructor_type = std::get<0>(info.param);
        MetricType metric_type = std::get<1>(info.param);
        switch (constructor_type) {
            case ConstructorType::VectorIndex:
                constructor_type_str = "VectorIndex";
                break;
            case ConstructorType::RawData:
                constructor_type_str = "RawData";
                break;
            case ConstructorType::VectorBase:
                constructor_type_str = "VectorBase";
                break;
            case ConstructorType::ChunkedColumn:
                constructor_type_str = "ChunkedColumn";
                break;
            default:
                constructor_type_str = "Unknown constructor type";
        };
        if (metric_type == knowhere::metric::L2) {
            constructor_type_str += "_L2";
        } else if (metric_type == knowhere::metric::IP) {
            constructor_type_str += "_IP";
        } else if (metric_type == knowhere::metric::COSINE) {
            constructor_type_str += "_COSINE";
        } else {
            constructor_type_str += "_Unknown";
        }
        return constructor_type_str;
    });
