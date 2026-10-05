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
#include <chrono>
#include <random>
#include <unordered_set>
#include "common/BitsetView.h"
#include "common/FastMem.h"
#include "common/QueryInfo.h"
#include "common/QueryResult.h"
#include "common/Utils.h"
#include "query/Utils.h"
#include "index/Index.h"
#include "knowhere/comp/index_param.h"
#include "query/CachedSearchIterator.h"
#include "index/VectorIndex.h"
#include "index/IndexFactory.h"
#include "knowhere/dataset.h"
#include "knowhere/sparse_utils.h"
#include "query/helper.h"
#include "segcore/ConcurrentVector.h"
#include "segcore/InsertRecord.h"
#include "segcore/SegmentGrowing.h"
#include "mmap/ChunkedColumn.h"
#include "test_utils/DataGen.h"
#include "test_utils/cachinglayer_test_utils.h"
#include "test_utils/storage_test_utils.h"

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

TEST(CachedSearchIteratorStrictPkTest, ExactCorpusAcrossRecreatedPages) {
    constexpr int64_t num_rows = 257;
    constexpr int64_t dim = 4;
    constexpr int64_t batch_size = 17;
    std::vector<float> vectors(num_rows * dim, 1.0f);
    std::vector<float> query(dim, 1.0f);
    auto base = knowhere::GenDataSet(num_rows, dim, vectors.data());
    auto query_ds = knowhere::GenDataSet(1, dim, query.data());
    auto vector_base =
        std::make_unique<ConcurrentVector<milvus::FloatVector>>(dim, 128);
    vector_base->set_data_raw(0, vectors.data(), num_rows);

    for (const auto& metric : kMetricTypes) {
        CreateIndexInfo index_info;
        index_info.field_type = DataType::VECTOR_FLOAT;
        index_info.metric_type = metric;
        index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
        index_info.index_engine_version =
            knowhere::Version::GetCurrentVersion().VersionNumber();
        auto index = IndexFactory::GetInstance().CreateIndex(
            index_info, storage::FileManagerContext());
        index->BuildWithDataset(
            base,
            {{knowhere::meta::METRIC_TYPE, metric},
             {knowhere::meta::DIM, std::to_string(dim)},
             {knowhere::indexparam::M, "16"},
             {knowhere::indexparam::EFCONSTRUCTION, "128"}});

        for (bool indexed : {false, true}) {
            for (bool varchar_pk : {false, true}) {
                for (bool sorted_pks : {false, true}) {
                    SCOPED_TRACE(::testing::Message()
                                 << metric << " indexed=" << indexed
                                 << " varchar=" << varchar_pk
                                 << " pk_sorted=" << sorted_pks);
                    std::vector<PkType> pks;
                    for (int64_t i = 0; i < num_rows; ++i) {
                        auto pk = sorted_pks ? i + 1 : num_rows - i;
                        if (varchar_pk) {
                            auto value = std::to_string(pk);
                            pks.emplace_back(
                                std::string(4 - value.size(), '0') + value);
                        } else {
                            pks.emplace_back(pk);
                        }
                    }
                    auto expected = pks;
                    std::sort(expected.begin(), expected.end());
                    size_t max_pk_batch = 0;
                    auto getter = [&](const std::vector<int64_t>& offsets) {
                        max_pk_batch = std::max(max_pk_batch, offsets.size());
                        std::vector<PkType> values;
                        for (auto offset : offsets) {
                            values.push_back(pks.at(offset));
                        }
                        return values;
                    };
                    SearchInfo info;
                    info.topk_ = batch_size;
                    info.round_decimal_ = -1;
                    info.metric_type_ = metric;
                    info.search_params_ = {{knowhere::indexparam::EF, "64"}};
                    SearchIteratorV2Info cursor;
                    cursor.batch_size = batch_size;
                    cursor.cursor_version = 2;
                    info.iterator_v2_info_ = cursor;
                    if (indexed) {
                        // Keep the independent ANN baseline as evidence: the
                        // exact cursor must also retrieve graph-unreachable rows.
                        const auto& vector_index =
                            dynamic_cast<const VectorIndex&>(*index);
                        auto reference_params =
                            vector_index.PrepareSearchParams(info);
                        reference_params[knowhere::meta::RANGE_SEARCH_K] = -1;
                        reference_params
                            [knowhere::meta::RETAIN_ITERATOR_ORDER] = false;
                        auto reference = vector_index.VectorIterators(
                            query_ds, reference_params, BitsetView{}, nullptr);
                        ASSERT_TRUE(reference.has_value());
                        ASSERT_EQ(reference.value().size(), 1);
                        std::vector<PkType> available;
                        std::vector<bool> seen(num_rows, false);
                        while (true) {
                            auto more = reference.value()[0]->HasNext();
                            ASSERT_TRUE(more.has_value());
                            if (!more.value()) {
                                break;
                            }
                            auto next = reference.value()[0]->Next();
                            ASSERT_TRUE(next.has_value());
                            const auto offset = next.value().first;
                            ASSERT_GE(offset, 0);
                            ASSERT_LT(offset, num_rows);
                            ASSERT_FALSE(seen.at(offset));
                            seen.at(offset) = true;
                            available.push_back(pks.at(offset));
                        }
                        std::sort(available.begin(), available.end());
                        std::cout << "PK_ANN_ORACLE metric=" << metric
                                  << " available=" << available.size()
                                  << " corpus=" << num_rows
                                  << " missing_offsets=";
                        for (size_t offset = 0; offset < num_rows; ++offset) {
                            if (!seen[offset]) {
                                std::cout << offset << ",";
                            }
                        }
                        std::cout << std::endl;
                    }
                    auto vector_getter =
                        [&](const std::vector<int64_t>& offsets) {
                            auto data = std::make_unique<DataArray>();
                            auto* array = data->mutable_vectors();
                            array->set_dim(dim);
                            auto* values =
                                array->mutable_float_vector()->mutable_data();
                            if (indexed) {
                                auto ids = knowhere::GenIdsDataSet(
                                    offsets.size(), offsets.data());
                                auto raw =
                                    dynamic_cast<const VectorIndex&>(*index)
                                        .GetVector(ids);
                                const auto* first =
                                    reinterpret_cast<const float*>(raw.data());
                                values->Add(first,
                                            first + offsets.size() * dim);
                            } else {
                                for (auto offset : offsets) {
                                    const auto* first =
                                        vectors.data() + offset * dim;
                                    values->Add(first, first + dim);
                                }
                            }
                            return data;
                        };
                    std::vector<PkType> actual;
                    for (size_t page = 0; page < 20; ++page) {
                        dataset::SearchDataset search_ds{
                            metric, 1, batch_size, -1, dim, query.data()};
                        auto iterator = std::make_unique<CachedSearchIterator>(
                            search_ds,
                            num_rows,
                            info,
                            std::map<std::string, std::string>{},
                            BitsetView{},
                            DataType::VECTOR_FLOAT,
                            vector_getter,
                            getter);
                        SearchResult result;
                        iterator->NextBatch(info, result);
                        ASSERT_TRUE(result.iterator_pk_cursor_executed_);
                        size_t count = 0;
                        for (size_t i = 0; i < result.seg_offsets_.size();
                             ++i) {
                            auto offset = result.seg_offsets_[i];
                            if (offset < 0) {
                                break;
                            }
                            actual.push_back(pks.at(offset));
                            info.iterator_v2_info_->last_bound =
                                result.distances_[i];
                            info.iterator_v2_info_->last_pk = pks.at(offset);
                            ++count;
                        }
                        if (count == 0) {
                            break;
                        }
                    }
                    EXPECT_EQ(actual, expected);
                    EXPECT_EQ(max_pk_batch,
                              std::min(expected.size(), size_t{256}));
                }
            }
        }
    }
}

TEST(CachedSearchIteratorStrictPkTest, ReadsActualGrowingPrimaryKeys) {
    constexpr int64_t rows = 20;
    for (const auto pk_type : {DataType::INT64, DataType::VARCHAR}) {
        auto schema = std::make_shared<Schema>();
        auto pk_field = schema->AddDebugField("pk", pk_type);
        schema->set_primary_field_id(pk_field);
        auto data = DataGen(schema, rows);
        for (auto& field : *data.raw_->mutable_fields_data()) {
            if (field.field_id() != pk_field.get()) {
                continue;
            }
            for (int64_t i = 0; i < rows; ++i) {
                if (pk_type == DataType::INT64) {
                    field.mutable_scalars()->mutable_long_data()->set_data(
                        i, rows - i);
                } else {
                    field.mutable_scalars()->mutable_string_data()->set_data(
                        i, "pk-" + std::to_string(rows - i));
                }
            }
        }
        auto segment = CreateGrowingSegment(schema, empty_index_meta);
        auto offset = segment->PreInsert(rows);
        segment->Insert(offset,
                        rows,
                        data.row_ids_.data(),
                        data.timestamps_.data(),
                        data.raw_);
        SearchResult result;
        auto getter = CachedSearchIterator::MakePrimaryKeyGetter(
            *segment, nullptr, result);
        const auto keys = getter({3, 0, 19});
        if (pk_type == DataType::INT64) {
            EXPECT_EQ(
                keys,
                (std::vector<PkType>{int64_t(17), int64_t(20), int64_t(1)}));
        } else {
            EXPECT_EQ(keys,
                      (std::vector<PkType>{std::string("pk-17"),
                                           std::string("pk-20"),
                                           std::string("pk-1")}));
        }
    }
}

TEST(CachedSearchIteratorStrictPkTest, FirstBatchCost) {
    // Report a warm native selection microbenchmark, not RPC throughput.
    // PKs come from an in-memory mapper, so this excludes PK-column I/O.
    constexpr int64_t rows = 4096;
    constexpr int64_t dim = 16;
    constexpr int64_t batch = 32;
    std::vector<float> vectors(rows * dim, 1.0f);
    std::vector<float> query(dim, 1.0f);
    auto base = knowhere::GenDataSet(rows, dim, vectors.data());
    auto query_ds = knowhere::GenDataSet(1, dim, query.data());
    auto vector_base =
        std::make_unique<ConcurrentVector<milvus::FloatVector>>(dim, 128);
    vector_base->set_data_raw(0, vectors.data(), rows);
    CreateIndexInfo index_info;
    index_info.field_type = DataType::VECTOR_FLOAT;
    index_info.metric_type = knowhere::metric::L2;
    index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
    index_info.index_engine_version =
        knowhere::Version::GetCurrentVersion().VersionNumber();
    auto index = IndexFactory::GetInstance().CreateIndex(
        index_info, storage::FileManagerContext());
    index->BuildWithDataset(
        base,
        {{knowhere::meta::METRIC_TYPE, knowhere::metric::L2},
         {knowhere::meta::DIM, std::to_string(dim)},
         {knowhere::indexparam::M, "16"},
         {knowhere::indexparam::EFCONSTRUCTION, "128"}});
    for (bool indexed : {false, true}) {
        for (uint32_t version : {0U, 2U}) {
            std::vector<int64_t> elapsed;
            size_t read_keys = 0;
            for (int repetition = 0; repetition < 6; ++repetition) {
                read_keys = 0;
                auto getter = [&](const std::vector<int64_t>& offsets) {
                    read_keys += offsets.size();
                    std::vector<PkType> values;
                    for (auto offset : offsets) {
                        values.emplace_back(rows - offset);
                    }
                    return values;
                };
                SearchInfo info;
                info.metric_type_ = knowhere::metric::L2;
                info.topk_ = batch;
                info.round_decimal_ = -1;
                info.search_params_ = {{knowhere::indexparam::EF, "64"}};
                SearchIteratorV2Info cursor;
                cursor.batch_size = batch;
                cursor.cursor_version = version;
                info.iterator_v2_info_ = cursor;
                const auto begin = std::chrono::steady_clock::now();
                std::unique_ptr<CachedSearchIterator> iterator;
                if (version == 2) {
                    auto vector_getter =
                        [&, indexed](const std::vector<int64_t>& offsets) {
                            auto data = std::make_unique<DataArray>();
                            auto* array = data->mutable_vectors();
                            array->set_dim(dim);
                            auto* values =
                                array->mutable_float_vector()->mutable_data();
                            if (indexed) {
                                auto ids = knowhere::GenIdsDataSet(
                                    offsets.size(), offsets.data());
                                auto raw =
                                    dynamic_cast<const VectorIndex&>(*index)
                                        .GetVector(ids);
                                const auto* first =
                                    reinterpret_cast<const float*>(raw.data());
                                values->Add(first,
                                            first + offsets.size() * dim);
                            } else {
                                for (auto offset : offsets) {
                                    const auto* first =
                                        vectors.data() + offset * dim;
                                    values->Add(first, first + dim);
                                }
                            }
                            return data;
                        };
                    dataset::SearchDataset search_ds{
                        knowhere::metric::L2, 1, batch, -1, dim, query.data()};
                    iterator = std::make_unique<CachedSearchIterator>(
                        search_ds,
                        rows,
                        info,
                        std::map<std::string, std::string>{},
                        BitsetView{},
                        DataType::VECTOR_FLOAT,
                        vector_getter,
                        getter);
                } else if (indexed) {
                    iterator = std::make_unique<CachedSearchIterator>(
                        dynamic_cast<const VectorIndex&>(*index),
                        query_ds,
                        info,
                        BitsetView{},
                        nullptr,
                        getter);
                } else {
                    dataset::SearchDataset search_ds{
                        knowhere::metric::L2, 1, batch, -1, dim, query.data()};
                    auto chunks = vector_base->acquire_chunks();
                    iterator = std::make_unique<CachedSearchIterator>(
                        search_ds,
                        vector_base.get(),
                        chunks,
                        rows,
                        info,
                        std::map<std::string, std::string>{},
                        BitsetView{},
                        DataType::VECTOR_FLOAT,
                        getter);
                }
                SearchResult result;
                iterator->NextBatch(info, result);
                const auto micros =
                    std::chrono::duration_cast<std::chrono::microseconds>(
                        std::chrono::steady_clock::now() - begin)
                        .count();
                ASSERT_EQ(result.seg_offsets_.size(), batch);
                ASSERT_EQ(result.iterator_pk_cursor_executed_, version == 2);
                if (repetition > 0) {
                    elapsed.push_back(micros);
                }
            }
            std::sort(elapsed.begin(), elapsed.end());
            std::cout << "PK_CURSOR_COST backend="
                      << (indexed ? "HNSW" : "growing_BF")
                      << " version=" << version << " rows=" << rows
                      << " dim=" << dim << " batch=" << batch
                      << " median_us=" << elapsed[elapsed.size() / 2]
                      << " pk_candidates=" << read_keys << std::endl;
        }
    }
}

TEST(CachedSearchIteratorStrictPkTest, DenseTypesIncludeEveryCorpusRow) {
    constexpr int64_t rows = 257;
    constexpr int64_t batch = 17;
    for (auto type : {DataType::VECTOR_FLOAT,
                      DataType::VECTOR_FLOAT16,
                      DataType::VECTOR_BFLOAT16,
                      DataType::VECTOR_INT8,
                      DataType::VECTOR_BINARY}) {
        SCOPED_TRACE(static_cast<int>(type));
        const int64_t dim = type == DataType::VECTOR_BINARY ? 8 : 4;
        const auto bytes_per_row = GetDataTypeSize(type, dim);
        std::vector<uint8_t> query(bytes_per_row, 0);
        auto vector_getter = [=](const std::vector<int64_t>& offsets) {
            auto data = std::make_unique<DataArray>();
            auto* vector = data->mutable_vectors();
            vector->set_dim(dim);
            std::string bytes(offsets.size() * bytes_per_row, 0);
            switch (type) {
                case DataType::VECTOR_FLOAT:
                    vector->mutable_float_vector()->mutable_data()->Resize(
                        offsets.size() * dim, 0);
                    break;
                case DataType::VECTOR_FLOAT16:
                    vector->set_float16_vector(bytes);
                    break;
                case DataType::VECTOR_BFLOAT16:
                    vector->set_bfloat16_vector(bytes);
                    break;
                case DataType::VECTOR_INT8:
                    vector->set_int8_vector(bytes);
                    break;
                case DataType::VECTOR_BINARY:
                    vector->set_binary_vector(bytes);
                    break;
                default:
                    break;
            }
            return data;
        };
        auto pk_getter = [=](const std::vector<int64_t>& offsets) {
            std::vector<PkType> pks;
            for (auto offset : offsets) pks.emplace_back(rows - offset);
            return pks;
        };
        SearchInfo info;
        info.metric_type_ = type == DataType::VECTOR_BINARY
                                ? knowhere::metric::HAMMING
                                : knowhere::metric::L2;
        info.round_decimal_ = -1;
        info.topk_ = batch;
        SearchIteratorV2Info cursor;
        cursor.batch_size = batch;
        cursor.cursor_version = 2;
        info.iterator_v2_info_ = cursor;
        dataset::SearchDataset dataset{
            info.metric_type_, 1, batch, -1, dim, query.data()};
        std::vector<int64_t> actual;
        for (int page = 0; page < 20; ++page) {
            CachedSearchIterator iterator(dataset,
                                          rows,
                                          info,
                                          {},
                                          BitsetView{},
                                          type,
                                          vector_getter,
                                          pk_getter);
            SearchResult result;
            iterator.NextBatch(info, result);
            ASSERT_TRUE(result.iterator_pk_cursor_executed_);
            size_t count = 0;
            for (auto offset : result.seg_offsets_) {
                if (offset < 0)
                    break;
                actual.push_back(rows - offset);
                ++count;
            }
            if (count == 0)
                break;
            info.iterator_v2_info_->last_pk = PkType(actual.back());
            info.iterator_v2_info_->last_bound = result.distances_[count - 1];
        }
        ASSERT_EQ(actual.size(), rows);
        for (int64_t i = 0; i < rows; ++i) ASSERT_EQ(actual[i], i + 1);
    }
}

TEST(CachedSearchIteratorStrictPkTest, NullableCompactVectorsAndLogicalFilter) {
    const std::vector<int64_t> pks{100, 7, 3, 1, 0};
    const std::vector<float> vectors{0, 1, 1, 2, 1};
    BitsetType filtered(5, false);
    filtered.set(4, true);
    auto vector_getter = [&](const std::vector<int64_t>& offsets) {
        auto data = std::make_unique<DataArray>();
        auto* array = data->mutable_vectors();
        array->set_dim(1);
        for (auto offset : offsets) {
            data->add_valid_data(offset != 0);
            if (offset != 0)
                array->mutable_float_vector()->add_data(vectors[offset]);
        }
        return data;
    };
    auto pk_getter = [&](const std::vector<int64_t>& offsets) {
        std::vector<PkType> values;
        for (auto offset : offsets) values.emplace_back(pks[offset]);
        return values;
    };
    SearchInfo info;
    info.metric_type_ = knowhere::metric::L2;
    info.round_decimal_ = -1;
    info.topk_ = 2;
    SearchIteratorV2Info cursor;
    cursor.batch_size = 2;
    cursor.cursor_version = 2;
    info.iterator_v2_info_ = cursor;
    float query = 0;
    dataset::SearchDataset dataset{info.metric_type_, 1, 2, -1, 1, &query};
    CachedSearchIterator first(dataset,
                               5,
                               info,
                               {},
                               BitsetView(filtered),
                               DataType::VECTOR_FLOAT,
                               vector_getter,
                               pk_getter);
    SearchResult result;
    first.NextBatch(info, result);
    ASSERT_EQ(result.seg_offsets_, (std::vector<int64_t>{2, 1}));
    ASSERT_EQ(result.distances_, (std::vector<float>{1, 1}));
    info.iterator_v2_info_->last_bound = 1;
    info.iterator_v2_info_->last_pk = PkType(int64_t{7});
    CachedSearchIterator next(dataset,
                              5,
                              info,
                              {},
                              BitsetView(filtered),
                              DataType::VECTOR_FLOAT,
                              vector_getter,
                              pk_getter);
    next.NextBatch(info, result);
    ASSERT_EQ(result.seg_offsets_, (std::vector<int64_t>{3, -1}));
    ASSERT_EQ(result.distances_[0], 4);
}

TEST(CachedSearchIteratorStrictPkTest,
     SparseKeepsZeroNegativeAndEmptyQueryScores) {
    using Row = knowhere::sparse::SparseRow<SparseValueType>;
    auto packed = [](uint32_t index, float value) {
        std::string bytes(Row::element_size(), 0);
        milvus::fastmem::FastMemcpy(bytes.data(), &index, sizeof(index));
        milvus::fastmem::FastMemcpy(
            bytes.data() + sizeof(index), &value, sizeof(value));
        return bytes;
    };
    std::vector<std::string> docs{
        packed(2, -1), packed(3, 1), "", packed(2, 1)};
    std::vector<int64_t> pks{3, 6, 2, 5};
    auto vector_getter = [&](const std::vector<int64_t>& offsets) {
        auto data = std::make_unique<DataArray>();
        auto* sparse = data->mutable_vectors()->mutable_sparse_float_vector();
        for (auto offset : offsets) sparse->add_contents(docs[offset]);
        return data;
    };
    auto pk_getter = [&](const std::vector<int64_t>& offsets) {
        std::vector<PkType> values;
        for (auto offset : offsets) values.emplace_back(pks[offset]);
        return values;
    };
    Row query(1);
    const auto bytes = packed(2, 1);
    milvus::fastmem::FastMemcpy(query.data(), bytes.data(), bytes.size());
    Row empty(0);
    for (bool empty_query : {false, true}) {
        SearchInfo info;
        info.metric_type_ = knowhere::metric::IP;
        info.topk_ = 1;
        info.round_decimal_ = -1;
        SearchIteratorV2Info cursor;
        cursor.batch_size = 1;
        cursor.cursor_version = 2;
        info.iterator_v2_info_ = cursor;
        dataset::SearchDataset dataset{
            info.metric_type_, 1, 1, -1, 0, empty_query ? &empty : &query};
        std::vector<int64_t> actual;
        std::vector<float> scores;
        for (int page = 0; page < 5; ++page) {
            CachedSearchIterator iterator(dataset,
                                          4,
                                          info,
                                          {},
                                          BitsetView{},
                                          DataType::VECTOR_SPARSE_U32_F32,
                                          vector_getter,
                                          pk_getter);
            SearchResult result;
            iterator.NextBatch(info, result);
            if (result.seg_offsets_[0] < 0)
                break;
            actual.push_back(pks[result.seg_offsets_[0]]);
            scores.push_back(result.distances_[0]);
            info.iterator_v2_info_->last_bound = result.distances_[0];
            info.iterator_v2_info_->last_pk = PkType(actual.back());
        }
        ASSERT_EQ(actual,
                  (empty_query ? std::vector<int64_t>{2, 3, 5, 6}
                               : std::vector<int64_t>{5, 2, 6, 3}));
        ASSERT_EQ(scores,
                  (empty_query ? std::vector<float>{0, 0, 0, 0}
                               : std::vector<float>{1, 0, 0, -1}));
    }
}

TEST(CachedSearchIteratorStrictPkTest, SealedHnswRawIndexReadsEveryLogicalRow) {
    constexpr int64_t rows = 257;
    constexpr int64_t dim = 4;
    constexpr int64_t batch = 17;
    for (auto pk_type : {DataType::INT64, DataType::VARCHAR}) {
        for (const auto& metric : kMetricTypes) {
            SCOPED_TRACE(::testing::Message()
                         << metric << " pk=" << static_cast<int>(pk_type));
            auto schema = std::make_shared<Schema>();
            auto pk_field = schema->AddDebugField("pk", pk_type);
            schema->set_primary_field_id(pk_field);
            auto vector_field = schema->AddDebugField(
                "vector", DataType::VECTOR_FLOAT, dim, metric);
            auto generated = DataGen(schema, rows);
            for (auto& field : *generated.raw_->mutable_fields_data()) {
                if (field.field_id() == pk_field.get()) {
                    for (int64_t i = 0; i < rows; ++i) {
                        if (pk_type == DataType::INT64) {
                            field.mutable_scalars()
                                ->mutable_long_data()
                                ->set_data(i, rows - i);
                        } else {
                            const auto value = std::to_string(rows - i);
                            field.mutable_scalars()
                                ->mutable_string_data()
                                ->set_data(
                                    i,
                                    std::string(4 - value.size(), '0') + value);
                        }
                    }
                } else if (field.field_id() == vector_field.get()) {
                    for (int64_t i = 0; i < rows * dim; ++i) {
                        field.mutable_vectors()
                            ->mutable_float_vector()
                            ->set_data(i, 1);
                    }
                }
            }
            auto segment = CreateSealedWithFieldDataLoaded(
                schema, generated, false, {vector_field.get()});
            CreateIndexInfo index_info;
            index_info.field_type = DataType::VECTOR_FLOAT;
            index_info.metric_type = metric;
            index_info.index_type = knowhere::IndexEnum::INDEX_HNSW;
            index_info.index_engine_version =
                knowhere::Version::GetCurrentVersion().VersionNumber();
            auto index = IndexFactory::GetInstance().CreateIndex(
                index_info, storage::FileManagerContext());
            std::vector<float> vectors(rows * dim, 1);
            index->BuildWithDataset(
                knowhere::GenDataSet(rows, dim, vectors.data()),
                {{knowhere::meta::METRIC_TYPE, metric},
                 {knowhere::meta::DIM, std::to_string(dim)},
                 {knowhere::indexparam::M, "16"},
                 {knowhere::indexparam::EFCONSTRUCTION, "128"}});
            LoadIndexInfo load_info;
            load_info.field_id = vector_field.get();
            load_info.index_params = GenIndexParams(index.get());
            load_info.cache_index = CreateTestCacheIndex(
                "strict-pk-sealed-" + metric +
                    std::to_string(static_cast<int>(pk_type)),
                std::move(index));
            segment->LoadIndex(load_info);
            ASSERT_TRUE(segment->HasIndex(vector_field));
            ASSERT_FALSE(segment->HasFieldData(vector_field));
            SearchInfo info;
            info.field_id_ = vector_field;
            info.metric_type_ = metric;
            info.topk_ = batch;
            info.round_decimal_ = -1;
            info.search_params_ = {{knowhere::indexparam::EF, "64"}};
            SearchIteratorV2Info cursor;
            cursor.batch_size = batch;
            cursor.cursor_version = 2;
            info.iterator_v2_info_ = cursor;
            std::vector<float> query(dim, 1);
            std::vector<PkType> actual;
            for (int page = 0; page < 20; ++page) {
                SearchResult result;
                segment->vector_search(info,
                                       query.data(),
                                       nullptr,
                                       1,
                                       std::numeric_limits<Timestamp>::max(),
                                       BitsetView{},
                                       nullptr,
                                       result);
                ASSERT_TRUE(result.iterator_pk_cursor_executed_);
                auto get_pks = CachedSearchIterator::MakePrimaryKeyGetter(
                    *segment, nullptr, result);
                std::vector<int64_t> offsets;
                for (auto offset : result.seg_offsets_) {
                    if (offset < 0)
                        break;
                    offsets.push_back(offset);
                }
                if (offsets.empty())
                    break;
                auto page_pks = get_pks(offsets);
                actual.insert(actual.end(), page_pks.begin(), page_pks.end());
                info.iterator_v2_info_->last_bound =
                    result.distances_[offsets.size() - 1];
                info.iterator_v2_info_->last_pk = actual.back();
            }
            ASSERT_EQ(actual.size(), rows);
            for (int64_t i = 0; i < rows; ++i) {
                if (pk_type == DataType::INT64) {
                    EXPECT_EQ(actual[i], PkType(i + 1));
                } else {
                    const auto value = std::to_string(i + 1);
                    EXPECT_EQ(
                        actual[i],
                        PkType(std::string(4 - value.size(), '0') + value));
                }
            }
        }
    }
}

TEST(CachedSearchIteratorStrictPkTest,
     MissingRawDataIsSystemErrorWithoutFallback) {
    auto schema = std::make_shared<Schema>();
    auto pk = schema->AddDebugField("pk", DataType::INT64);
    schema->set_primary_field_id(pk);
    auto vector =
        schema->AddDebugField("vector", DataType::VECTOR_FLOAT, 4, "L2");
    auto generated = DataGen(schema, 2);
    auto segment = CreateSealedWithFieldDataLoaded(
        schema, generated, false, {vector.get()});
    SearchInfo info;
    info.field_id_ = vector;
    info.metric_type_ = knowhere::metric::L2;
    info.round_decimal_ = -1;
    info.topk_ = 1;
    SearchIteratorV2Info cursor;
    cursor.batch_size = 1;
    cursor.cursor_version = 2;
    info.iterator_v2_info_ = cursor;
    float query[4] = {};
    SearchResult result;
    try {
        segment->vector_search(info,
                               query,
                               nullptr,
                               1,
                               std::numeric_limits<Timestamp>::max(),
                               BitsetView{},
                               nullptr,
                               result);
        FAIL() << "Strict scan must fail when no raw vector source exists";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
        EXPECT_NE(
            std::string(error.what()).find("must exist when getting raw data"),
            std::string::npos);
    }
    EXPECT_FALSE(result.iterator_pk_cursor_executed_);
}

TEST(CachedSearchIteratorStrictPkTest,
     DuplicateEqualScoreRowsDoNotHideOtherKeys) {
    // Page reduction deduplicates PKs. Nonempty short pages must still advance
    // the composite cursor, even when duplicates occupy a whole core batch.
    const std::vector<int64_t> pks{1, 4, 1, 2, 1, 3, 1, 2, 1, 3, 1, 2, 1, 1};
    auto vectors = [](const std::vector<int64_t>& offsets) {
        auto data = std::make_unique<DataArray>();
        data->mutable_vectors()->set_dim(1);
        data->mutable_vectors()->mutable_float_vector()->mutable_data()->Resize(
            offsets.size(), 1);
        return data;
    };
    auto keys = [&](const std::vector<int64_t>& offsets) {
        std::vector<PkType> values;
        for (auto offset : offsets) values.emplace_back(pks[offset]);
        return values;
    };
    SearchInfo info;
    info.metric_type_ = knowhere::metric::L2;
    info.round_decimal_ = -1;
    info.topk_ = 2;
    SearchIteratorV2Info cursor;
    cursor.batch_size = 2;
    cursor.cursor_version = 2;
    info.iterator_v2_info_ = cursor;
    float query = 0;
    dataset::SearchDataset dataset{info.metric_type_, 1, 2, -1, 1, &query};
    std::vector<int64_t> actual;
    for (int page = 0; page < 8; ++page) {
        CachedSearchIterator iterator(dataset,
                                      pks.size(),
                                      info,
                                      {},
                                      BitsetView{},
                                      DataType::VECTOR_FLOAT,
                                      vectors,
                                      keys);
        SearchResult result;
        iterator.NextBatch(info, result);
        std::vector<int64_t> reduced;
        size_t count = 0;
        for (auto offset : result.seg_offsets_) {
            if (offset < 0)
                break;
            const auto pk = pks[offset];
            if (reduced.empty() || reduced.back() != pk)
                reduced.push_back(pk);
            ++count;
        }
        if (reduced.empty())
            break;
        actual.insert(actual.end(), reduced.begin(), reduced.end());
        info.iterator_v2_info_->last_bound = result.distances_[count - 1];
        info.iterator_v2_info_->last_pk = PkType(reduced.back());
    }
    EXPECT_EQ(actual, (std::vector<int64_t>{1, 2, 3, 4}));
}

TEST(CachedSearchIteratorStrictPkTest,
     LiveBm25StatisticsAreRejectedWithoutReads) {
    SearchInfo info;
    info.metric_type_ = knowhere::metric::BM25;
    info.round_decimal_ = -1;
    info.topk_ = 1;
    SearchIteratorV2Info cursor;
    cursor.batch_size = 1;
    cursor.cursor_version = 2;
    info.iterator_v2_info_ = cursor;
    using Row = knowhere::sparse::SparseRow<SparseValueType>;
    Row query(0);
    dataset::SearchDataset dataset{info.metric_type_, 1, 1, -1, 0, &query};
    bool read = false;
    auto vectors = [&](const std::vector<int64_t>&) {
        read = true;
        return std::make_unique<DataArray>();
    };
    auto keys = [](const std::vector<int64_t>&) {
        return std::vector<PkType>{};
    };
    try {
        CachedSearchIterator iterator(dataset,
                                      1,
                                      info,
                                      {},
                                      BitsetView{},
                                      DataType::VECTOR_SPARSE_U32_F32,
                                      vectors,
                                      keys);
        FAIL() << "Live BM25 statistics cannot provide a stable score cursor";
    } catch (const SegcoreError& error) {
        EXPECT_EQ(error.get_error_code(), ErrorCode::UnexpectedError);
    }
    EXPECT_FALSE(read);
}

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
