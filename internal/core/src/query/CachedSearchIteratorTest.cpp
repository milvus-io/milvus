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
#include "common/StructElementOffsets.h"
#include "common/Utils.h"
#include "common/VectorArray.h"
#include "query/Utils.h"
#include "index/contracts/query/IIndexReaderBase.h"
#include "index/contracts/query/IVectorReader.h"
#include "knowhere/comp/index_param.h"
#include "query/CachedSearchIterator.h"
#include "knowhere/dataset.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "segcore/test_utils/ConsumerIndexTestUtils.h"

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

enum class ConstructorType { VectorIndex = 0, VectorBase, ChunkedColumn };

static const std::vector<ConstructorType> kConstructorTypes = {
    ConstructorType::VectorIndex,
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
        SearchInfo info;
        info.topk_ = kBatchSize;
        info.round_decimal_ = -1;
        info.metric_type_ = std::get<1>(GetParam());
        info.search_params_ = {
            {knowhere::indexparam::EF, std::to_string(kHnswEf)},
        };
        SearchIteratorV2Info iter_info;
        iter_info.batch_size = kBatchSize;
        info.iterator_v2_info_ = iter_info;
        return info;
    }

    static DataType data_type_;
    static int64_t dim_;
    static int64_t nb_;
    static int64_t nq_;
    static FixedVector<float> base_dataset_;
    static FixedVector<float> query_dataset_;
    // Reader owners outlive every iterator: knowhere iterators borrow their
    // index's row-id mapping even after CachedSearchIterator construction.
    static IIndexReaderBasePtr index_hnsw_l2_;
    static IIndexReaderBasePtr index_hnsw_ip_;
    static IIndexReaderBasePtr index_hnsw_cos_;
    static knowhere::DataSetPtr knowhere_query_dataset_;
    static dataset::SearchDataset search_dataset_;
    static std::unique_ptr<ConcurrentVector<milvus::FloatVector>> vector_base_;
    static std::shared_ptr<ChunkedColumn> column_;
    static std::vector<std::vector<char>> column_data_;
    static std::shared_ptr<Schema> schema_;
    static FieldId fakevec_id_;

    const IVectorReader* index_hnsw_ = nullptr;
    MetricType metric_type_ = kMetricType;

    std::unique_ptr<CachedSearchIterator>
    DispatchIterator(const ConstructorType& constructor_type,
                     const SearchInfo& search_info,
                     const BitsetView& bitset) {
        switch (constructor_type) {
            case ConstructorType::VectorIndex:
                return std::make_unique<CachedSearchIterator>(
                    *index_hnsw_, knowhere_query_dataset_, search_info, bitset);

            case ConstructorType::VectorBase: {
                // The snapshot only has to outlive the constructor: it walks
                // every chunk through it before returning, and the iterators
                // it produces are then owned by the returned object, whose
                // chunks this test never reclaims.
                auto chunks = vector_base_->acquire_chunks();
                return std::make_unique<CachedSearchIterator>(
                    search_dataset_,
                    vector_base_.get(),
                    chunks,
                    nb_,
                    search_info,
                    std::map<std::string, std::string>{},
                    bitset,
                    data_type_);
            }

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
        for (const auto& metric_type : kMetricTypes) {
            auto build_conf = knowhere::Json{
                {knowhere::meta::DIM, std::to_string(dim_)},
                {knowhere::indexparam::M, std::to_string(kHnswM)},
                {knowhere::indexparam::EFCONSTRUCTION,
                 std::to_string(kHnswEfConstruction)}};
            auto opened = milvus::test::consumer::BuildVectorReader<float>(
                data_type_,
                knowhere::IndexEnum::INDEX_HNSW,
                metric_type,
                dim_,
                nb_,
                base_dataset_.data(),
                std::move(build_conf));
            ASSERT_EQ(opened.reader->Count(), nb_);
            if (metric_type == knowhere::metric::L2) {
                index_hnsw_l2_ = std::move(opened.reader);
            } else if (metric_type == knowhere::metric::IP) {
                index_hnsw_ip_ = std::move(opened.reader);
            } else if (metric_type == knowhere::metric::COSINE) {
                index_hnsw_cos_ = std::move(opened.reader);
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
            auto chunk_mmap_guard =
                std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
            chunks.emplace_back(
                std::make_unique<FixedWidthChunk>(rows,
                                                  dim_,
                                                  chunk_data.data(),
                                                  buf_size,
                                                  sizeof(float),
                                                  false,
                                                  chunk_mmap_guard));
            offset += rows;
        }
        auto translator = std::make_unique<TestChunkTranslator>(
            num_rows_per_chunk, "", std::move(chunks));
        auto slot =
            cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
                std::move(translator), nullptr);
        column_ = std::make_shared<ChunkedColumn>(std::move(slot), field_meta);
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
            kMetricType,
            nq_,
            kBatchSize,
            -1,
            dim_,
            query_dataset_.data(),
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
            index_hnsw_ =
                dynamic_cast<const IVectorReader*>(index_hnsw_l2_.get());
        } else if (metric_type == knowhere::metric::IP) {
            metric_type_ = knowhere::metric::IP;
            search_dataset_.metric_type = knowhere::metric::IP;
            index_hnsw_ =
                dynamic_cast<const IVectorReader*>(index_hnsw_ip_.get());
        } else if (metric_type == knowhere::metric::COSINE) {
            metric_type_ = knowhere::metric::COSINE;
            search_dataset_.metric_type = knowhere::metric::COSINE;
            index_hnsw_ =
                dynamic_cast<const IVectorReader*>(index_hnsw_cos_.get());
        } else {
            FAIL() << "Unsupported metric type: " << metric_type;
        }
        ASSERT_NE(index_hnsw_, nullptr);
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
IIndexReaderBasePtr CachedSearchIteratorTest::index_hnsw_l2_ = nullptr;
IIndexReaderBasePtr CachedSearchIteratorTest::index_hnsw_ip_ = nullptr;
IIndexReaderBasePtr CachedSearchIteratorTest::index_hnsw_cos_ = nullptr;
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

    {
        SearchInfo si;
        si.metric_type_ = "";
        EXPECT_THROW(DispatchIterator(std::get<0>(GetParam()), si, nullptr),
                     SegcoreError);
    }

    {
        SearchInfo si;
        si.metric_type_ = metric_type_;
        EXPECT_THROW(DispatchIterator(std::get<0>(GetParam()), si, nullptr),
                     SegcoreError);
    }

    {
        SearchInfo si;
        si.metric_type_ = metric_type_;
        si.iterator_v2_info_ = SearchIteratorV2Info{};
        EXPECT_THROW(DispatchIterator(std::get<0>(GetParam()), si, nullptr),
                     SegcoreError);
    }

    {
        SearchInfo si;
        si.metric_type_ = metric_type_;
        SearchIteratorV2Info iter_info;
        iter_info.batch_size = 0;
        si.iterator_v2_info_ = iter_info;
        EXPECT_THROW(DispatchIterator(std::get<0>(GetParam()), si, nullptr),
                     SegcoreError);
    }
}

TEST_P(CachedSearchIteratorTest, ConstructorWithInvalidParams) {
    SearchInfo search_info = GetDefaultNormalSearchInfo();
    if (std::get<0>(GetParam()) == ConstructorType::VectorIndex) {
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         *index_hnsw_, nullptr, search_info, nullptr),
                     SegcoreError);

        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         *index_hnsw_,
                         std::make_shared<knowhere::DataSet>(),
                         search_info,
                         nullptr),
                     SegcoreError);
    } else if (std::get<0>(GetParam()) == ConstructorType::VectorBase) {
        auto chunks = vector_base_->acquire_chunks();
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         dataset::SearchDataset{},
                         vector_base_.get(),
                         chunks,
                         nb_,
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);

        // Null column: rejected before the snapshot is ever consulted, so an
        // unacquired snapshot is the honest thing to pass here.
        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         search_dataset_,
                         nullptr,
                         ChunkSnapshot{},
                         nb_,
                         search_info,
                         std::map<std::string, std::string>{},
                         nullptr,
                         data_type_),
                     SegcoreError);

        EXPECT_THROW(auto iterator = std::make_unique<CachedSearchIterator>(
                         search_dataset_,
                         vector_base_.get(),
                         chunks,
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

// Test that CachedSearchIterator correctly adjusts chunk_size for nullable
// ChunkedColumn (uses valid_count_per_chunk instead of total chunk_row_nums)
TEST(CachedSearchIteratorNullableTest, ChunkedColumnWithPartialNulls) {
    constexpr int64_t dim = 16;
    constexpr int64_t chunk_rows = 100;      // logical rows per chunk
    constexpr int64_t valid_per_chunk = 50;  // even rows valid, odd null
    constexpr int64_t num_chunks = 2;
    constexpr int64_t total_valid = valid_per_chunk * num_chunks;
    constexpr int64_t batch_size = 10;

    // Create schema with nullable vector field
    auto schema = std::make_shared<Schema>();
    auto fakevec_id = schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2, true);
    auto field_meta = schema->operator[](fakevec_id);

    // Generate random vector data (enough for all valid vectors)
    auto base_schema = std::make_shared<Schema>();
    base_schema->AddDebugField(
        "fakevec", DataType::VECTOR_FLOAT, dim, knowhere::metric::L2);
    auto dataset = segcore::DataGen(base_schema, total_valid);
    auto base_data =
        dataset.get_col<float>(base_schema->get_field_id(FieldName("fakevec")));

    // Build chunks with 50% null vectors.
    // Nullable vector chunks store ONLY valid vectors contiguously in data
    // section (matching NullableVectorChunkWriter behavior), while the null
    // bitmap covers ALL logical rows.
    std::vector<std::unique_ptr<Chunk>> chunks;
    std::vector<int64_t> num_rows_per_chunk;
    std::vector<std::vector<char>> chunk_buffers;

    for (int64_t c = 0; c < num_chunks; c++) {
        num_rows_per_chunk.push_back(chunk_rows);

        int null_bitmap_bytes = (chunk_rows + 7) / 8;
        // Data section: only valid vectors stored contiguously
        int vector_data_size = valid_per_chunk * dim * sizeof(float);
        int buf_size = null_bitmap_bytes + vector_data_size;

        chunk_buffers.emplace_back(buf_size, 0);
        char* buf = chunk_buffers.back().data();

        // Even rows valid, odd rows null
        for (int j = 0; j < chunk_rows; j++) {
            if (j % 2 == 0) {
                buf[j >> 3] |= (1 << (j & 0x07));
            }
        }

        // Copy valid vectors contiguously into data section
        memcpy(buf + null_bitmap_bytes,
               base_data.data() + c * valid_per_chunk * dim,
               vector_data_size);

        auto chunk_mmap_guard =
            std::make_shared<ChunkMmapGuard>(nullptr, 0, "");
        chunks.emplace_back(
            std::make_unique<FixedWidthChunk>(chunk_rows,
                                              dim,
                                              buf,
                                              buf_size,
                                              sizeof(float),
                                              true,
                                              chunk_mmap_guard));
    }

    auto translator = std::make_unique<TestChunkTranslator>(
        num_rows_per_chunk, "", std::move(chunks));
    auto slot =
        cachinglayer::Manager::GetInstance().CreateCacheSlot<milvus::Chunk>(
            std::move(translator), nullptr);
    auto column = std::make_shared<ChunkedColumn>(std::move(slot), field_meta);
    column->BuildValidRowIds(nullptr);

    // Verify offset_mapping setup
    const auto& offset_mapping = column->GetOffsetMapping();
    ASSERT_TRUE(offset_mapping.IsEnabled());
    ASSERT_EQ(offset_mapping.GetValidCount(), total_valid);

    // Build query from first valid vector
    std::vector<float> query_data(base_data.begin(), base_data.begin() + dim);

    // Search bitsets stay in logical row space; CachedSearchIterator attaches
    // the chunk-local physical->logical window before entering Knowhere BF.
    TargetBitmap search_bitset(chunk_rows * num_chunks, false);
    BitsetView search_bitview(search_bitset);

    dataset::SearchDataset search_dataset{
        knowhere::metric::L2,
        1,
        batch_size,
        -1,
        dim,
        query_data.data(),
    };

    SearchInfo search_info;
    search_info.topk_ = batch_size;
    search_info.round_decimal_ = -1;
    search_info.metric_type_ = knowhere::metric::L2;
    SearchIteratorV2Info iter_v2_info;
    iter_v2_info.batch_size = batch_size;
    search_info.iterator_v2_info_ = iter_v2_info;

    CachedSearchIterator iter(column.get(),
                              search_dataset,
                              search_info,
                              std::map<std::string, std::string>{},
                              search_bitview,
                              DataType::VECTOR_FLOAT);

    SearchResult result;
    iter.NextBatch(search_info, result);

    ASSERT_EQ(result.seg_offsets_.size(), batch_size);
    ASSERT_EQ(result.distances_.size(), batch_size);

    // Knowhere BF returns logical offsets through the BitsetView out-id window.
    int valid_count = 0;
    for (auto& offset : result.seg_offsets_) {
        if (offset == INVALID_SEG_OFFSET) {
            continue;
        }
        valid_count++;
        ASSERT_GE(offset, 0);
        ASSERT_LT(offset, chunk_rows * num_chunks);
        ASSERT_EQ(offset % 2, 0) << "logical offset should be a valid row";
    }
    ASSERT_GT(valid_count, 0);
}

TEST(CachedSearchIteratorTieBreakTest, ElementLevelTieContinuation) {
    constexpr int64_t kTestDim = 4;
    constexpr int64_t kBatch = 2;
    auto vec_col =
        std::make_unique<ConcurrentVector<milvus::VectorArray>>(kTestDim, 16);

    const std::vector<float> row0_vecs = {
        1.f, 0.f, 0.f, 0.f, 0.f, 1.f, 0.f, 0.f, -1.f, 0.f, 0.f, 0.f};
    const std::vector<float> row1_vecs = {
        0.f, -1.f, 0.f, 0.f, 0.f, 0.f, 1.f, 0.f, 0.f, 0.f, -1.f, 0.f};
    std::vector<milvus::VectorArray> rows;
    rows.emplace_back(row0_vecs.data(), 3, kTestDim, DataType::VECTOR_FLOAT);
    rows.emplace_back(row1_vecs.data(), 3, kTestDim, DataType::VECTOR_FLOAT);
    vec_col->set_data_raw(0, rows.data(), 2);

    auto elem_offsets =
        std::make_shared<StructElementOffsetsSealed>(std::vector<int32_t>{0, 3, 6});
    const std::vector<int64_t> row_pks = {10, 5};
    PkAccessor pk_accessor = [&row_pks](int64_t doc_offset) -> std::optional<PkType> {
        EXPECT_GE(doc_offset, 0);
        EXPECT_LT(doc_offset, static_cast<int64_t>(row_pks.size()));
        if (doc_offset < 0 ||
            doc_offset >= static_cast<int64_t>(row_pks.size())) {
            return std::nullopt;
        }
        return PkType{row_pks[doc_offset]};
    };

    const std::vector<float> query_vec(kTestDim, 0.f);
    dataset::SearchDataset search_ds{
        knowhere::metric::L2, 1, kBatch, -1, kTestDim, query_vec.data()};
    TargetBitmap bitset_data(6, false);
    BitsetView bitset(bitset_data);

    auto encode_i64_pk = [](int64_t v) {
        std::string s(8, '\0');
        uint64_t u = static_cast<uint64_t>(v);
        for (int i = 7; i >= 0; --i) {
            s[i] = static_cast<char>(u & 0xff);
            u >>= 8;
        }
        return s;
    };

    auto run_page = [&](std::optional<float> last_bound,
                        std::optional<int64_t> last_pk,
                        std::optional<int32_t> last_elem_idx) {
        SearchInfo info;
        info.topk_ = kBatch;
        info.round_decimal_ = -1;
        info.metric_type_ = knowhere::metric::L2;
        info.struct_element_offsets_ = elem_offsets;
        SearchIteratorV2Info v2;
        v2.batch_size = kBatch;
        v2.last_bound = last_bound;
        if (last_pk.has_value()) {
            v2.last_primary_key = encode_i64_pk(*last_pk);
            v2.primary_key_type = DataType::INT64;
        }
        v2.last_element_index = last_elem_idx;
        info.iterator_v2_info_ = v2;

        auto chunks = vec_col->acquire_chunks();
        CachedSearchIterator iter(search_ds,
                                  vec_col.get(),
                                  chunks,
                                  2,
                                  info,
                                  std::map<std::string, std::string>{},
                                  bitset,
                                  DataType::VECTOR_FLOAT,
                                  false,
                                  pk_accessor);
        SearchResult sr;
        iter.NextBatch(info, sr);
        FinalizeVectorSearchOffsets(sr, elem_offsets.get());
        return sr;
    };

    // Expected global (dist=1.0, pk, elem_idx) order:
    // (5,0), (5,1), (5,2), (10,0), (10,1), (10,2)
    auto p1 = run_page(std::nullopt, std::nullopt, std::nullopt);
    ASSERT_EQ(p1.seg_offsets_, (std::vector<int64_t>{1, 1}));
    ASSERT_EQ(p1.element_indices_, (std::vector<int32_t>{0, 1}));

    auto p2 = run_page(1.0f, 5, 1);
    ASSERT_EQ(p2.seg_offsets_, (std::vector<int64_t>{1, 0}));
    ASSERT_EQ(p2.element_indices_, (std::vector<int32_t>{2, 0}));

    auto p3 = run_page(1.0f, 10, 0);
    ASSERT_EQ(p3.seg_offsets_, (std::vector<int64_t>{0, 0}));
    ASSERT_EQ(p3.element_indices_, (std::vector<int32_t>{1, 2}));

    auto p4 = run_page(1.0f, 10, 2);
    ASSERT_EQ(p4.seg_offsets_,
              (std::vector<int64_t>{INVALID_SEG_OFFSET, INVALID_SEG_OFFSET}));

    // Incomplete cursor (last_pk without last_element_index) falls back to
    // strict dist > last_bound so sibling elements are not skipped/duplicated.
    auto incomplete = run_page(1.0f, 5, std::nullopt);
    ASSERT_EQ(incomplete.seg_offsets_,
              (std::vector<int64_t>{INVALID_SEG_OFFSET, INVALID_SEG_OFFSET}));
}

TEST(CachedSearchIteratorTieBreakTest, EmptyVarCharPkTieContinuation) {
    constexpr int64_t kTestDim = 4;
    auto vec_col =
        std::make_unique<ConcurrentVector<milvus::FloatVector>>(kTestDim, 16);
    const std::vector<float> vecs = {
        1.f, 0.f, 0.f, 0.f, 0.f, 1.f, 0.f, 0.f};
    vec_col->set_data_raw(0, vecs.data(), 2);

    const std::vector<std::string> row_pks = {"a", ""};
    PkAccessor pk_accessor = [&row_pks](int64_t doc_offset) -> std::optional<PkType> {
        if (doc_offset < 0 ||
            doc_offset >= static_cast<int64_t>(row_pks.size())) {
            return std::nullopt;
        }
        return PkType{row_pks[doc_offset]};
    };

    const std::vector<float> query_vec(kTestDim, 0.f);
    dataset::SearchDataset search_ds{
        knowhere::metric::L2, 1, 1, -1, kTestDim, query_vec.data()};
    TargetBitmap bitset_data(2, false);
    BitsetView bitset(bitset_data);

    auto run_page = [&](std::optional<float> last_bound,
                        std::optional<std::string> last_pk) {
        SearchInfo info;
        info.topk_ = 1;
        info.round_decimal_ = -1;
        info.metric_type_ = knowhere::metric::L2;
        SearchIteratorV2Info v2;
        v2.batch_size = 1;
        v2.last_bound = last_bound;
        if (last_pk.has_value()) {
            v2.last_primary_key = *last_pk;
            v2.primary_key_type = DataType::VARCHAR;
        }
        info.iterator_v2_info_ = v2;

        auto chunks = vec_col->acquire_chunks();
        CachedSearchIterator iter(search_ds,
                                  vec_col.get(),
                                  chunks,
                                  2,
                                  info,
                                  std::map<std::string, std::string>{},
                                  bitset,
                                  DataType::VECTOR_FLOAT,
                                  false,
                                  pk_accessor);
        SearchResult sr;
        iter.NextBatch(info, sr);
        return sr;
    };

    auto p1 = run_page(std::nullopt, std::nullopt);
    ASSERT_EQ(p1.seg_offsets_, (std::vector<int64_t>{1}));

    auto p2 = run_page(1.0f, std::string(""));
    ASSERT_EQ(p2.seg_offsets_, (std::vector<int64_t>{0}));

    auto p3 = run_page(1.0f, std::string("a"));
    ASSERT_EQ(p3.seg_offsets_, (std::vector<int64_t>{INVALID_SEG_OFFSET}));
}

TEST(CachedSearchIteratorTieBreakTest,
     MultiNextBatchPreservesTieOverflowAndLookahead) {
    constexpr int64_t kTestDim = 4;
    constexpr int64_t kBatch = 2;
    auto vec_col =
        std::make_unique<ConcurrentVector<milvus::FloatVector>>(kTestDim, 16);
    // 4 vectors at L2 distance 1.0 (PKs 40, 10, 30, 20) + 1 at L2 distance 4.0 (PK 99).
    const std::vector<float> vecs = {
        1.f, 0.f, 0.f, 0.f,
        0.f, 1.f, 0.f, 0.f,
        -1.f, 0.f, 0.f, 0.f,
        0.f, -1.f, 0.f, 0.f,
        2.f, 0.f, 0.f, 0.f,
    };
    vec_col->set_data_raw(0, vecs.data(), 5);

    const std::vector<int64_t> row_pks = {40, 10, 30, 20, 99};
    PkAccessor pk_accessor = [&row_pks](int64_t doc_offset) -> std::optional<PkType> {
        if (doc_offset < 0 ||
            doc_offset >= static_cast<int64_t>(row_pks.size())) {
            return std::nullopt;
        }
        return PkType{row_pks[doc_offset]};
    };

    const std::vector<float> query_vec(kTestDim, 0.f);
    dataset::SearchDataset search_ds{
        knowhere::metric::L2, 1, kBatch, -1, kTestDim, query_vec.data()};
    TargetBitmap bitset_data(5, false);
    BitsetView bitset(bitset_data);

    SearchInfo info;
    info.topk_ = kBatch;
    info.round_decimal_ = -1;
    info.metric_type_ = knowhere::metric::L2;
    SearchIteratorV2Info v2;
    v2.batch_size = kBatch;
    info.iterator_v2_info_ = v2;

    auto chunks = vec_col->acquire_chunks();
    CachedSearchIterator iter(search_ds,
                              vec_col.get(),
                              chunks,
                              5,
                              info,
                              std::map<std::string, std::string>{},
                              bitset,
                              DataType::VECTOR_FLOAT,
                              false,
                              pk_accessor);

    SearchResult p1;
    iter.NextBatch(info, p1);
    ASSERT_EQ(p1.seg_offsets_, (std::vector<int64_t>{1, 3}));  // PKs 10, 20

    SearchResult p2;
    iter.NextBatch(info, p2);
    ASSERT_EQ(p2.seg_offsets_, (std::vector<int64_t>{2, 0}));  // PKs 30, 40

    SearchResult p3;
    iter.NextBatch(info, p3);
    ASSERT_EQ(p3.seg_offsets_, (std::vector<int64_t>{4, INVALID_SEG_OFFSET}));  // PK 99

    SearchResult p4;
    iter.NextBatch(info, p4);
    ASSERT_EQ(p4.seg_offsets_,
              (std::vector<int64_t>{INVALID_SEG_OFFSET, INVALID_SEG_OFFSET}));
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
