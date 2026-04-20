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

#include <boost/container/vector.hpp>
#include <boost/filesystem/operations.hpp>
#include <fmt/core.h>
#include <folly/FBVector.h>
#include <gtest/gtest.h>
#include <nlohmann/json.hpp>
#include <stdint.h>
#include <stdlib.h>
#include <iosfwd>
#include <memory>
#include <stdexcept>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/Array.h"
#include "common/Consts.h"
#include "common/FieldDataInterface.h"
#include "common/Tracer.h"
#include "common/TracerBase.h"
#include "common/Types.h"
#include "common/protobuf_utils.h"
#include "gtest/gtest.h"
#include "index/BitmapIndex.h"
#include "index/Index.h"
#include "index/IndexFactory.h"
#include "index/IndexInfo.h"
#include "index/IndexStats.h"
#include "index/Meta.h"
#include "index/ScalarIndex.h"
#include "indexbuilder/IndexCreatorBase.h"
#include "indexbuilder/IndexFactory.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"

using namespace milvus::index;
using namespace milvus::indexbuilder;
using namespace milvus;
using namespace milvus::index;

std::vector<milvus::Array>
GenerateArrayData(proto::schema::DataType element_type,
                  int cardinality,
                  int size,
                  int array_len) {
    std::vector<ScalarFieldProto> data(size);
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
        field_schema.set_nullable(nullable_);
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
        auto field_data = storage::CreateFieldData(
            DataType::ARRAY, DataType::NONE, nullable_);
        if (nullable_) {
            valid_data_.reserve(nb_);
            uint8_t* ptr = new uint8_t[(nb_ + 7) / 8];
            for (int i = 0; i < nb_; i++) {
                int byteIndex = i / 8;
                int bitIndex = i % 8;
                if (i % 2 == 0) {
                    valid_data_.push_back(true);
                    ptr[byteIndex] |= (1 << bitIndex);
                } else {
                    valid_data_.push_back(false);
                    ptr[byteIndex] &= ~(1 << bitIndex);
                }
            }
            field_data->FillFieldData(data_.data(), ptr, data_.size(), 0);
            delete[] ptr;
        } else {
            field_data->FillFieldData(data_.data(), data_.size());
        }
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);

        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    TestLocalPath,
                                    collection_id,
                                    partition_id,
                                    segment_id,
                                    field_id,
                                    0);
        chunk_manager_->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        storage::FileManagerContext ctx(
            field_meta, index_meta, chunk_manager_, fs_);
        std::vector<std::string> index_files;

        Config config;
        config["index_type"] = milvus::index::HYBRID_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        config["bitmap_cardinality_limit"] = "100";
        config[INDEX_NUM_ROWS_KEY] = nb_;
        config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] = 3;
        if (has_lack_binlog_row_) {
            config[INDEX_NUM_ROWS_KEY] = nb_ + lack_binlog_row_;
        }

        {
            auto build_index =
                indexbuilder::IndexFactory::GetInstance().CreateIndex(
                    DataType::ARRAY, config, ctx);
            build_index->Build();

            auto create_index_result = build_index->Upload();
            auto memSize = create_index_result->GetMemSize();
            auto serializedSize = create_index_result->GetSerializedSize();
            ASSERT_GT(memSize, 0);
            ASSERT_GT(serializedSize, 0);
            index_files = create_index_result->GetIndexFiles();
        }

        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::HYBRID_INDEX_TYPE;
        index_info.field_type = DataType::ARRAY;

        config["index_files"] = index_files;
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        ctx.set_for_loading_index(true);
        index_ =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        index_->LoadUnified(config);
    }

    virtual void
    SetParam() {
        nb_ = 10000;
        cardinality_ = 30;
        nullable_ = false;
        index_build_id_ = 2001;
        index_version_ = 2001;
    }

    void
    SetUp() override {
        SetParam();
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
        std::string root_path = TestLocalPath;

        storage::StorageConfig storage_config;
        storage_config.storage_type = "local";
        storage_config.root_path = root_path;
        chunk_manager_ = storage::CreateChunkManager(storage_config);
        fs_ = storage::InitArrowFileSystem(storage_config);

        Init(collection_id,
             partition_id,
             segment_id,
             field_id,
             index_build_id_,
             index_version_);
    }

    virtual ~ArrayBitmapIndexTest() override {
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
        auto index_ptr = dynamic_cast<index::ScalarIndex<T>*>(index_.get());
        auto bitset = index_ptr->In(test_data.size(), test_data.data());
        size_t start = 0;
        if (has_lack_binlog_row_) {
            // all null here
            for (int i = 0; i < lack_binlog_row_; i++) {
                ASSERT_EQ(bitset[i], false);
            }
            start += lack_binlog_row_;
        }
        for (size_t i = start; i < bitset.size(); i++) {
            auto ref = [&]() -> bool {
                milvus::Array array = data_[i - start];
                if (nullable_ && !valid_data_[i - start]) {
                    return false;
                }
                for (size_t j = 0; j < array.length(); ++j) {
                    auto val = array.template get_data<T>(j);
                    if (s.find(val) != s.end()) {
                        return true;
                    }
                }
                return false;
            };
            ASSERT_EQ(bitset[i], ref());
        }
    }

 private:
    std::shared_ptr<storage::ChunkManager> chunk_manager_;
    milvus_storage::ArrowFileSystemPtr fs_;

 public:
    DataType type_;
    IndexBasePtr index_;
    size_t nb_;
    size_t cardinality_;
    bool nullable_;
    std::vector<milvus::Array> data_;
    FixedVector<bool> valid_data_;
    int index_version_;
    int index_build_id_;
    bool has_lack_binlog_row_{false};
    size_t lack_binlog_row_{100};
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

template <typename T>
class ArrayBitmapIndexTestV1 : public ArrayBitmapIndexTest<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 200;
        this->nullable_ = false;
        this->index_build_id_ = 2002;
        this->index_version_ = 2002;
    }

    virtual ~ArrayBitmapIndexTestV1() {
    }
};

TYPED_TEST_SUITE_P(ArrayBitmapIndexTestV1);

TYPED_TEST_P(ArrayBitmapIndexTestV1, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

template <typename T>
class ArrayBitmapIndexTestNullable : public ArrayBitmapIndexTest<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 30;
        this->nullable_ = true;
        this->index_version_ = 2003;
        this->index_build_id_ = 2003;
    }

    virtual ~ArrayBitmapIndexTestNullable() {
    }
};

TYPED_TEST_SUITE_P(ArrayBitmapIndexTestNullable);

TYPED_TEST_P(ArrayBitmapIndexTestNullable, CountFuncTest) {
    auto count = this->index_->Count();
    EXPECT_EQ(count, this->nb_);
}

template <typename T>
class ArrayBitmapIndexTestV2 : public ArrayBitmapIndexTest<T> {
 public:
    virtual void
    SetParam() override {
        this->nb_ = 10000;
        this->cardinality_ = 30;
        this->nullable_ = true;
        this->index_version_ = 2003;
        this->index_build_id_ = 2003;
        this->has_lack_binlog_row_ = true;
    }

    virtual ~ArrayBitmapIndexTestV2() {
    }
};

TYPED_TEST_SUITE_P(ArrayBitmapIndexTestV2);

TYPED_TEST_P(ArrayBitmapIndexTestV2, CountFuncTest) {
    auto count = this->index_->Count();
    if (this->has_lack_binlog_row_) {
        EXPECT_EQ(count, this->nb_ + this->lack_binlog_row_);
    } else {
        EXPECT_EQ(count, this->nb_);
    }
}

using BitmapTypeV1 = testing::Types<int32_t, int64_t, std::string>;

REGISTER_TYPED_TEST_SUITE_P(ArrayBitmapIndexTestV1, CountFuncTest);
REGISTER_TYPED_TEST_SUITE_P(ArrayBitmapIndexTestNullable, CountFuncTest);
REGISTER_TYPED_TEST_SUITE_P(ArrayBitmapIndexTestV2, CountFuncTest);

INSTANTIATE_TYPED_TEST_SUITE_P(ArrayBitmapE2ECheckV1,
                               ArrayBitmapIndexTestV1,
                               BitmapTypeV1);

INSTANTIATE_TYPED_TEST_SUITE_P(ArrayBitmapE2ECheckV1,
                               ArrayBitmapIndexTestNullable,
                               BitmapTypeV1);

INSTANTIATE_TYPED_TEST_SUITE_P(ArrayBitmapE2ECheckV1,
                               ArrayBitmapIndexTestV2,
                               BitmapTypeV1);

struct BitmapIndexArrayRegressionParam {
    proto::schema::DataType element_type;
    bool use_v3;
    bool use_mmap;
};

class BitmapIndexArrayRegressionTest
    : public testing::TestWithParam<BitmapIndexArrayRegressionParam> {
 protected:
    static constexpr int64_t collection_id_ = 1;
    static constexpr int64_t partition_id_ = 2;
    static constexpr int64_t segment_id_ = 3;
    static constexpr int64_t field_id_ = 101;
    static constexpr int64_t num_rows_ = 4;

    std::string
    ElementTypeName(proto::schema::DataType element_type) const {
        switch (element_type) {
            case proto::schema::DataType::Int32:
                return "int32";
            case proto::schema::DataType::String:
                return "string";
            default:
                return "unknown";
        }
    }

    std::vector<milvus::Array>
    BuildArrayData(proto::schema::DataType element_type, bool use_mmap) const {
        auto posting_count =
            use_mmap ? DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND + 50 : 2;

        std::vector<ScalarFieldProto> scalar_arrays(num_rows_);
        switch (element_type) {
            case proto::schema::DataType::Int32: {
                for (int i = 0; i < posting_count; ++i) {
                    scalar_arrays[1].mutable_int_data()->add_data(i);
                }
                scalar_arrays[2].mutable_int_data()->add_data(-1);
                break;
            }
            case proto::schema::DataType::String: {
                for (int i = 0; i < posting_count; ++i) {
                    scalar_arrays[1].mutable_string_data()->add_data(
                        fmt::format("s{}", i));
                }
                scalar_arrays[2].mutable_string_data()->add_data("ignored");
                break;
            }
            default:
                ThrowInfo(ErrorCode::DataTypeInvalid,
                          "unsupported element type {} in regression test",
                          proto::schema::DataType_Name(element_type));
        }

        std::vector<milvus::Array> array_data;
        array_data.reserve(num_rows_);
        for (const auto& scalar_array : scalar_arrays) {
            array_data.emplace_back(scalar_array);
        }
        return array_data;
    }

    FieldDataPtr
    BuildFieldData(proto::schema::DataType element_type, bool use_mmap) const {
        auto array_data = BuildArrayData(element_type, use_mmap);
        auto field_data =
            storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
        uint8_t valid_data = 0x0B;  // rows 0,1,3 valid; row 2 null.
        field_data->FillFieldData(
            array_data.data(), &valid_data, array_data.size(), 0);
        return field_data;
    }

    void
    AssertNullSemantics(index::IndexBase* loaded_index,
                        proto::schema::DataType element_type) const {
        switch (element_type) {
            case proto::schema::DataType::Int32: {
                auto index_ptr =
                    dynamic_cast<index::ScalarIndex<int32_t>*>(loaded_index);
                ASSERT_NE(index_ptr, nullptr);
                AssertNullSemanticsImpl(index_ptr);
                break;
            }
            case proto::schema::DataType::String: {
                auto index_ptr = dynamic_cast<index::ScalarIndex<std::string>*>(
                    loaded_index);
                ASSERT_NE(index_ptr, nullptr);
                AssertNullSemanticsImpl(index_ptr);
                break;
            }
            default:
                FAIL() << "unsupported element type "
                       << proto::schema::DataType_Name(element_type);
        }
    }

    template <typename T>
    void
    AssertNullSemanticsImpl(index::ScalarIndex<T>* index_ptr) const {
        auto is_null = index_ptr->IsNull();
        auto is_not_null = index_ptr->IsNotNull();

        ASSERT_EQ(is_null.size(), num_rows_);
        ASSERT_EQ(is_not_null.size(), num_rows_);

        EXPECT_FALSE(is_null[0]) << "empty array row should stay non-null";
        EXPECT_TRUE(is_not_null[0]) << "empty array row should stay non-null";

        EXPECT_FALSE(is_null[1]) << "non-empty array row should stay non-null";
        EXPECT_TRUE(is_not_null[1])
            << "non-empty array row should stay non-null";

        EXPECT_TRUE(is_null[2]) << "null array row should stay null";
        EXPECT_FALSE(is_not_null[2]) << "null array row should stay null";

        EXPECT_FALSE(is_null[3]) << "empty array row should stay non-null";
        EXPECT_TRUE(is_not_null[3]) << "empty array row should stay non-null";
    }
};

TEST_P(BitmapIndexArrayRegressionTest,
       NullableEmptyArrayPreservesValidityAfterReload) {
    auto param = GetParam();
    auto index_build_id = param.use_v3 ? 3001 : 3002;
    auto index_version = index_build_id;

    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(param.element_type);
    field_schema.set_nullable(true);

    auto field_meta = storage::FieldDataMeta{
        collection_id_, partition_id_, segment_id_, field_id_, field_schema};
    auto index_meta = storage::IndexMeta{
        segment_id_, field_id_, index_build_id, index_version};

    auto field_data = BuildFieldData(param.element_type, param.use_mmap);

    std::string root_path =
        fmt::format("{}/bitmap_empty_array_regression_{}_{}_{}",
                    TestLocalPath,
                    ElementTypeName(param.element_type),
                    param.use_v3 ? "v3" : "binaryset",
                    param.use_mmap ? "mmap" : "memory");
    boost::filesystem::remove_all(root_path);

    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto mmap_path = fmt::format("{}/bitmap_index.mmap", root_path);

    if (param.use_v3) {
        auto payload_reader =
            std::make_shared<milvus::storage::PayloadReader>(field_data);
        storage::InsertData insert_data(payload_reader);
        insert_data.SetFieldDataMeta(field_meta);
        insert_data.SetTimestamps(0, 100);

        auto serialized_bytes = insert_data.Serialize(storage::Remote);
        auto log_path = fmt::format("/{}/{}/{}/{}/{}/{}",
                                    TestLocalPath,
                                    collection_id_,
                                    partition_id_,
                                    segment_id_,
                                    field_id_,
                                    0);
        chunk_manager->Write(
            log_path, serialized_bytes.data(), serialized_bytes.size());

        Config config;
        config["index_type"] = milvus::index::BITMAP_INDEX_TYPE;
        config[INSERT_FILES_KEY] = std::vector<std::string>{log_path};
        config[INDEX_NUM_ROWS_KEY] = num_rows_;
        config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] = 3;

        auto build_index =
            indexbuilder::IndexFactory::GetInstance().CreateIndex(
                DataType::ARRAY, config, ctx);
        build_index->Build();

        auto create_index_result = build_index->Upload();
        ASSERT_GT(create_index_result->GetMemSize(), 0);
        ASSERT_GT(create_index_result->GetSerializedSize(), 0);

        index::CreateIndexInfo index_info{};
        index_info.index_type = milvus::index::BITMAP_INDEX_TYPE;
        index_info.field_type = DataType::ARRAY;

        config["index_files"] = create_index_result->GetIndexFiles();
        config[milvus::LOAD_PRIORITY] =
            milvus::proto::common::LoadPriority::HIGH;
        if (param.use_mmap) {
            config[milvus::index::MMAP_FILE_PATH] = mmap_path;
        }
        ctx.set_for_loading_index(true);

        auto loaded_index =
            index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
        loaded_index->LoadUnified(config);
        AssertNullSemantics(loaded_index.get(), param.element_type);
    } else {
        Config load_config;
        if (param.use_mmap) {
            load_config[milvus::index::MMAP_FILE_PATH] = mmap_path;
        }

        switch (param.element_type) {
            case proto::schema::DataType::Int32: {
                auto index = std::make_unique<index::BitmapIndex<int32_t>>(ctx);
                index->BuildWithFieldData(
                    std::vector<FieldDataPtr>{field_data});
                auto binary_set = index->Serialize({});

                auto loaded_index =
                    std::make_unique<index::BitmapIndex<int32_t>>(ctx);
                loaded_index->Load(binary_set, load_config);
                AssertNullSemantics(loaded_index.get(), param.element_type);
                break;
            }
            case proto::schema::DataType::String: {
                auto index =
                    std::make_unique<index::BitmapIndex<std::string>>(ctx);
                index->BuildWithFieldData(
                    std::vector<FieldDataPtr>{field_data});
                auto binary_set = index->Serialize({});

                auto loaded_index =
                    std::make_unique<index::BitmapIndex<std::string>>(ctx);
                loaded_index->Load(binary_set, load_config);
                AssertNullSemantics(loaded_index.get(), param.element_type);
                break;
            }
            default:
                FAIL() << "unsupported element type "
                       << proto::schema::DataType_Name(param.element_type);
        }
    }

    boost::filesystem::remove_all(root_path);
}

INSTANTIATE_TEST_SUITE_P(
    BitmapIndexArrayRegression,
    BitmapIndexArrayRegressionTest,
    testing::Values(
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::Int32, true, false},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::Int32, true, true},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::Int32, false, false},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::Int32, false, true},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::String, true, false},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::String, true, true},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::String, false, false},
        BitmapIndexArrayRegressionParam{
            proto::schema::DataType::String, false, true}),
    [](const testing::TestParamInfo<BitmapIndexArrayRegressionParam>& info) {
        auto element_type =
            info.param.element_type == proto::schema::DataType::Int32
                ? "Int32"
                : "String";
        return fmt::format("{}_{}_{}",
                           element_type,
                           info.param.use_v3 ? "V3" : "BinarySet",
                           info.param.use_mmap ? "MMap" : "Memory");
    });
