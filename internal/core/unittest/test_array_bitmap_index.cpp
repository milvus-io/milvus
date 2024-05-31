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
#include "pb/schema.pb.h"

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

std::vector<milvus::Array>
GenerateArrayData(proto::schema::DataType element_type,
                  int cardinality,
                  int size,
                  int array_len) {
    std::vector<ScalarArray> data(size);
    switch (element_type) {
        case proto::schema::DataType::Bool: {
            for (int i = 0; i < size; i++) {
                milvus::proto::schema::ScalarField field_data;
                for (int j = 0; j < array_len; j++) {
                    field_data.mutable_bool_data()->add_data(
                        static_cast<bool>(random()));
                }
                data[i] = field_data;
            }
            break;
        }
        case proto::schema::DataType::Int8:
        case proto::schema::DataType::Int16:
        case proto::schema::DataType::Int32: {
            for (int i = 0; i < size; i++) {
                milvus::proto::schema::ScalarField field_data;

                for (int j = 0; j < array_len; j++) {
                    field_data.mutable_int_data()->add_data(
                        static_cast<int>(random() % cardinality));
                }
                data[i] = field_data;
            }
            break;
        }
        case proto::schema::DataType::Int64: {
            for (int i = 0; i < size; i++) {
                milvus::proto::schema::ScalarField field_data;
                for (int j = 0; j < array_len; j++) {
                    field_data.mutable_long_data()->add_data(
                        static_cast<int64_t>(random() % cardinality));
                }
                data[i] = field_data;
            }
            break;
        }
        case proto::schema::DataType::String: {
            for (int i = 0; i < size; i++) {
                milvus::proto::schema::ScalarField field_data;

                for (int j = 0; j < array_len; j++) {
                    field_data.mutable_string_data()->add_data(
                        std::to_string(random() % cardinality));
                }
                data[i] = field_data;
            }
            break;
        }
        case proto::schema::DataType::Float: {
            for (int i = 0; i < size; i++) {
                milvus::proto::schema::ScalarField field_data;

                for (int j = 0; j < array_len; j++) {
                    field_data.mutable_float_data()->add_data(
                        static_cast<float>(random() % cardinality));
                }
                data[i] = field_data;
            }
            break;
        }
        case proto::schema::DataType::Double: {
            for (int i = 0; i < size; i++) {
                milvus::proto::schema::ScalarField field_data;

                for (int j = 0; j < array_len; j++) {
                    field_data.mutable_double_data()->add_data(
                        static_cast<double>(random() % cardinality));
                }
                data[i] = field_data;
            }
            break;
        }
        default: {
            throw std::runtime_error("unsupported data type");
        }
    }
    std::vector<milvus::Array> res;
    for (int i = 0; i < size; i++) {
        res.push_back(milvus::Array(data[i]));
    }
    return res;
}

template <typename T>
class ArrayBitmapIndexTest : public testing::Test {
 protected:
    void
    Init(int64_t collection_id,
         int64_t partition_id,
         int64_t segment_id,
         int64_t field_id,
         int64_t index_build_id,
         int64_t index_version) {
        proto::schema::FieldSchema field_schema;
        field_schema.set_data_type(proto::schema::DataType::Array);
        proto::schema::DataType element_type;
        if constexpr (std::is_same_v<int8_t, T>) {
            element_type = proto::schema::DataType::Int8;
        } else if constexpr (std::is_same_v<int16_t, T>) {
            element_type = proto::schema::DataType::Int16;
        } else if constexpr (std::is_same_v<int32_t, T>) {
            element_type = proto::schema::DataType::Int32;
        } else if constexpr (std::is_same_v<int64_t, T>) {
            element_type = proto::schema::DataType::Int64;
        } else if constexpr (std::is_same_v<float, T>) {
            element_type = proto::schema::DataType::Float;
        } else if constexpr (std::is_same_v<double, T>) {
            element_type = proto::schema::DataType::Double;
        } else if constexpr (std::is_same_v<std::string, T>) {
            element_type = proto::schema::DataType::String;
        }
        field_schema.set_element_type(element_type);
        auto field_meta = storage::FieldDataMeta{
            collection_id, partition_id, segment_id, field_id, field_schema};
        auto index_meta = storage::IndexMeta{
            segment_id, field_id, index_build_id, index_version};

        data_ = GenerateArrayData(element_type, cardinality_, nb_, 10);

        auto field_data = storage::CreateFieldData(DataType::ARRAY);
        field_data->FillFieldData(data_.data(), data_.size());
        storage::InsertData insert_data(field_data);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("{}/{}/{}/{}/{}/{}",
                                    "test_array_bitmap",
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
        config["bitmap_cardinality_limit"] = "1000";

        auto build_index =
            indexbuilder::IndexFactory::GetInstance().CreateIndex(
                DataType::ARRAY, config, ctx);
        build_index->Build();

        auto binary_set = build_index->Upload();
        for (const auto& [key, _] : binary_set.binary_map_) {
            index_files.push_back(key);
        }

        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::BITMAP_INDEX_TYPE;
        index_info.field_type = DataType::ARRAY;

        config["index_files"] = index_files;

        index_ =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index_->Load(milvus::tracer::TraceContext{}, config);
    }

    void
    SetUp() override {
        nb_ = 10000;
        cardinality_ = 30;

        // if constexpr (std::is_same_v<T, int8_t>) {
        //     type_ = DataType::INT8;
        // } else if constexpr (std::is_same_v<T, int16_t>) {
        //     type_ = DataType::INT16;
        // } else if constexpr (std::is_same_v<T, int32_t>) {
        //     type_ = DataType::INT32;
        // } else if constexpr (std::is_same_v<T, int64_t>) {
        //     type_ = DataType::INT64;
        // } else if constexpr (std::is_same_v<T, std::string>) {
        //     type_ = DataType::VARCHAR;
        // }
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

    virtual ~ArrayBitmapIndexTest() override {
        boost::filesystem::remove_all(chunk_manager_->GetRootPath());
    }

 public:
    void
    TestInFunc() {
        // boost::container::vector<T> test_data;
        // std::unordered_set<T> s;
        // size_t nq = 10;
        // for (size_t i = 0; i < nq; i++) {
        //     test_data.push_back(data_[i]);
        //     s.insert(data_[i]);
        // }
        // auto index_ptr = dynamic_cast<index::ScalarIndex<T>*>(index_.get());
        // auto bitset = index_ptr->In(test_data.size(), test_data.data());
        // for (size_t i = 0; i < bitset.size(); i++) {
        //     ASSERT_EQ(bitset[i], s.find(data_[i]) != s.end());
        // }
    }

 private:
    std::shared_ptr<storage::ChunkManager> chunk_manager_;

 public:
    DataType type_;
    IndexBasePtr index_;
    size_t nb_;
    size_t cardinality_;
    std::vector<milvus::Array> data_;
};

TYPED_TEST_SUITE_P(ArrayBitmapIndexTest);

TYPED_TEST_P(ArrayBitmapIndexTest, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

TYPED_TEST_P(ArrayBitmapIndexTest, INFuncTest) {
    // this->TestInFunc();
}

TYPED_TEST_P(ArrayBitmapIndexTest, NotINFuncTest) {
    //this->TestNotInFunc();
}

using BitmapType =
    testing::Types<int8_t, int16_t, int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(ArrayBitmapIndexTest,
                            CountFuncTest,
                            INFuncTest,
                            NotINFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(ArrayBitmapE2ECheck,
                               ArrayBitmapIndexTest,
                               BitmapType);
