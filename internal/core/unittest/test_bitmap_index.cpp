// Copyright(C) 2019 - 2020 Zilliz.All rights reserved.
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
#include <functional>
#include <boost/filesystem.hpp>
#include <unordered_set>
#include <memory>

#include "common/Tracer.h"
#include "index/BitmapIndex.h"
#include "storage/Util.h"
#include "storage/InsertData.h"
#include "indexbuilder/IndexFactory.h"
#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "index/Meta.h"

using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

template <typename T>
static std::vector<T>
GenerateData(const size_t size, const size_t cardinality) {
    std::vector<T> result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(rand() % cardinality);
    }
    return result;
}

template <>
std::vector<bool>
GenerateData<bool>(const size_t size, const size_t cardinality) {
    std::vector<bool> result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(rand() % 2 == 0);
    }
    return result;
}

template <>
std::vector<std::string>
GenerateData<std::string>(const size_t size, const size_t cardinality) {
    std::vector<std::string> result;
    for (size_t i = 0; i < size; ++i) {
        result.push_back(std::to_string(rand() % cardinality));
    }
    return result;
}

template <typename T>
class BitmapIndexTest : public testing::Test {
 protected:
    void
    Init(int64_t collection_id,
         int64_t partition_id,
         int64_t segment_id,
         int64_t field_id,
         int64_t index_build_id,
         int64_t index_version) {
        proto::schema::FieldSchema field_schema;
        if constexpr (std::is_same_v<int8_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int8);
        } else if constexpr (std::is_same_v<int16_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int16);
        } else if constexpr (std::is_same_v<int32_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int32);
        } else if constexpr (std::is_same_v<int64_t, T>) {
            field_schema.set_data_type(proto::schema::DataType::Int64);
        } else if constexpr (std::is_same_v<float, T>) {
            field_schema.set_data_type(proto::schema::DataType::Float);
        } else if constexpr (std::is_same_v<double, T>) {
            field_schema.set_data_type(proto::schema::DataType::Double);
        } else if constexpr (std::is_same_v<std::string, T>) {
            field_schema.set_data_type(proto::schema::DataType::String);
        }
        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        std::vector<T> data_gen;
        data_gen = GenerateData<T>(nb_, cardinality_);
        for (auto x : data_gen) {
            data_.push_back(x);
        }

        auto field_data = storage::CreateFieldData(type_);
        field_data->FillFieldData(data_.data(), data_.size());
        storage::InsertData insert_data(field_data);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    "/tmp/test-bitmap-index/",
                                    collection_id,
                                    partition_id,
                                    segment_id,
                                    field_id,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager_);
        std::vector<std::string> index_files;

        Config config;
        config["index_type"] = milvus::index::BITMAP_INDEX_TYPE;
        config["insert_files"] = std::vector<std::string>{log_path};

        auto build_index =
            indexbuilder::IndexFactory::GetInstance().CreateIndex(
                type_, config, ctx);
        build_index->Build();

        auto binary_set = build_index->Upload();
        for (const auto& [key, _] : binary_set.binary_map_) {
            index_files.push_back(key);
        }

        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::BITMAP_INDEX_TYPE;
        index_info.field_type = type_;

        config["index_files"] = index_files;

        if (is_mmap_) {
            config["enable_mmap"] = "true";
            config["mmap_filepath"] = fmt::format("/{}/{}/{}/{}/{}",
                                                  "/tmp/test-bitmap-index/",
                                                  collection_id,
                                                  1,
                                                  segment_id,
                                                  field_id);
            ;
        }
        index_ =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index_->Load(milvus::tracer::TraceContext{}, config);
    }

    virtual void
    SetParam() {
        nb_ = 10000;
        cardinality_ = 30;
    }
    void
    SetUp() override {
        SetParam();

        if constexpr (std::is_same_v<T, int8_t>) {
            type_ = DataType::INT8;
        } else if constexpr (std::is_same_v<T, int16_t>) {
            type_ = DataType::INT16;
        } else if constexpr (std::is_same_v<T, int32_t>) {
            type_ = DataType::INT32;
        } else if constexpr (std::is_same_v<T, int64_t>) {
            type_ = DataType::INT64;
        } else if constexpr (std::is_same_v<T, std::string>) {
            type_ = DataType::VARCHAR;
        }
        int64_t collection_id = 1;
        int64_t partition_id = 2;
        int64_t segment_id = 3;
        int64_t field_id = 101;
        int64_t index_build_id = 1000;
        int64_t index_version = 10000;
        std::string root_path = "/tmp/test-bitmap-index/";

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager_ = storage::CreateChunkManager(storage_config);

        Init(collection_id,
             partition_id,
             segment_id,
             field_id,
             index_build_id,
             index_version);
    }

    virtual ~BitmapIndexTest() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

 public:
    void
    TestInFunc() {
        boost::container::vector<T> test_data;
        std::unordered_set<T> s;
        size_t nq = 10;
        for (size_t i = 0; i < nq; i++) {
            test_data.push_back(data_[i]);
            s.insert(data_[i]);
        }
        auto index_ptr = dynamic_cast<index::BitmapIndex<T>*>(index_.get());
        auto bitset = index_ptr->In(test_data.size(), test_data.data());
        for (size_t i = 0; i < bitset.size(); i++) {
            ASSERT_EQ(bitset[i], s.find(data_[i]) != s.end());
        }
    }

    void
    TestNotInFunc() {
        boost::container::vector<T> test_data;
        std::unordered_set<T> s;
        size_t nq = 10;
        for (size_t i = 0; i < nq; i++) {
            test_data.push_back(data_[i]);
            s.insert(data_[i]);
        }
        auto index_ptr = dynamic_cast<index::BitmapIndex<T>*>(index_.get());
        auto bitset = index_ptr->NotIn(test_data.size(), test_data.data());
        for (size_t i = 0; i < bitset.size(); i++) {
            ASSERT_EQ(bitset[i], s.find(data_[i]) == s.end());
        }
    }

    void
    TestCompareValueFunc() {
        if constexpr (!std::is_same_v<T, std::string>) {
            using RefFunc = std::function<bool(int64_t)>;
            std::vector<std::tuple<T, OpType, RefFunc>> test_cases{
                {10,
                 OpType::GreaterThan,
                 [&](int64_t i) -> bool { return data_[i] > 10; }},
                {10,
                 OpType::GreaterEqual,
                 [&](int64_t i) -> bool { return data_[i] >= 10; }},
                {10,
                 OpType::LessThan,
                 [&](int64_t i) -> bool { return data_[i] < 10; }},
                {10,
                 OpType::LessEqual,
                 [&](int64_t i) -> bool { return data_[i] <= 10; }},
            };
            for (const auto& [test_value, op, ref] : test_cases) {
                auto index_ptr =
                    dynamic_cast<index::BitmapIndex<T>*>(index_.get());
                auto bitset = index_ptr->Range(test_value, op);
                for (size_t i = 0; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = ref(i);
                    ASSERT_EQ(ans, should)
                        << "op: " << op << ", @" << i << ", ans: " << ans
                        << ", ref: " << should << "|" << data_[i];
                }
            }
        }
    }

    void
    TestRangeCompareFunc() {
        if constexpr (!std::is_same_v<T, std::string>) {
            using RefFunc = std::function<bool(int64_t)>;
            struct TestParam {
                int64_t lower_val;
                int64_t upper_val;
                bool lower_inclusive;
                bool upper_inclusive;
                RefFunc ref;
            };
            std::vector<TestParam> test_cases = {
                {
                    10,
                    30,
                    false,
                    false,
                    [&](int64_t i) { return 10 < data_[i] && data_[i] < 30; },
                },
                {
                    10,
                    30,
                    true,
                    false,
                    [&](int64_t i) { return 10 <= data_[i] && data_[i] < 30; },
                },
                {
                    10,
                    30,
                    true,
                    true,
                    [&](int64_t i) { return 10 <= data_[i] && data_[i] <= 30; },
                },
                {
                    10,
                    30,
                    false,
                    true,
                    [&](int64_t i) { return 10 < data_[i] && data_[i] <= 30; },
                }};

            for (const auto& test_case : test_cases) {
                auto index_ptr =
                    dynamic_cast<index::BitmapIndex<T>*>(index_.get());
                auto bitset = index_ptr->Range(test_case.lower_val,
                                               test_case.lower_inclusive,
                                               test_case.upper_val,
                                               test_case.upper_inclusive);
                for (size_t i = 0; i < bitset.size(); i++) {
                    auto ans = bitset[i];
                    auto should = test_case.ref(i);
                    ASSERT_EQ(ans, should)
                        << "lower:" << test_case.lower_val
                        << "upper:" << test_case.upper_val << ", @" << i
                        << ", ans: " << ans << ", ref: " << should;
                }
            }
        }
    }

 public:
    IndexBasePtr index_;
    DataType type_;
    size_t nb_;
    size_t cardinality_;
    bool is_mmap_ = false;
    boost::container::vector<T> data_;
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
};

TYPED_TEST_SUITE_P(BitmapIndexTest);

TYPED_TEST_P(BitmapIndexTest, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(BitmapIndexTest, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(BitmapIndexTest, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(BitmapIndexTest, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

using BitmapType =
    testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(BitmapIndexTest,
                            CountFuncTest,
                            INFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(BitmapE2ECheck, BitmapIndexTest, BitmapType);

template <typename T>
class BitmapIndexTestV2 : public BitmapIndexTest<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 2000;
    }

    virtual ~BitmapIndexTestV2() {
    }
};

TYPED_TEST_SUITE_P(BitmapIndexTestV2);

TYPED_TEST_P(BitmapIndexTestV2, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(BitmapIndexTestV2, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(BitmapIndexTestV2, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(BitmapIndexTestV2, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(BitmapIndexTestV2, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

using BitmapType =
    testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(BitmapIndexTestV2,
                            CountFuncTest,
                            INFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(BitmapIndexE2ECheck_HighCardinality,
                               BitmapIndexTestV2,
                               BitmapType);

template <typename T>
class BitmapIndexTestV3 : public BitmapIndexTest<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 2000;
        this->is_mmap_ = true;
    }

    virtual ~BitmapIndexTestV3() {
    }
};

TYPED_TEST_SUITE_P(BitmapIndexTestV3);

TYPED_TEST_P(BitmapIndexTestV3, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(BitmapIndexTestV3, INFuncTest) {
    this->TestInFunc();
}

TYPED_TEST_P(BitmapIndexTestV3, NotINFuncTest) {
    this->TestNotInFunc();
}

TYPED_TEST_P(BitmapIndexTestV3, CompareValFuncTest) {
    this->TestCompareValueFunc();
}

TYPED_TEST_P(BitmapIndexTestV3, TestRangeCompareFuncTest) {
    this->TestRangeCompareFunc();
}

using BitmapType =
    testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(BitmapIndexTestV3,
                            CountFuncTest,
                            INFuncTest,
                            NotINFuncTest,
                            CompareValFuncTest,
                            TestRangeCompareFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(BitmapIndexE2ECheck_Mmap,
                               BitmapIndexTestV3,
                               BitmapType);