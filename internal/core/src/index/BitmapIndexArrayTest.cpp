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
#include <folly/ScopeGuard.h>
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
#include "index/ScalarIndexSort.h"
#include "index/StringIndexSort.h"
#include "common/ArrayOffsets.h"
#include "indexbuilder/IndexCreatorBase.h"
#include "indexbuilder/IndexFactory.h"
#include "milvus-storage/filesystem/fs.h"
#include "pb/common.pb.h"
#include "pb/schema.pb.h"
#include "storage/ChunkManager.h"
#include "storage/EntryStreamUtils.h"
#include "storage/FileManager.h"
#include "storage/InsertData.h"
#include "storage/PayloadReader.h"
#include "storage/PluginLoader.h"
#include "storage/ThreadPools.h"
#include "storage/Types.h"
#include "storage/Util.h"
#include "test_utils/Constants.h"
#include "test_utils/PlannerCipherPlugin.h"

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
    // Collect up to `max_vals` distinct ELEMENT VALUES (not Array objects) from
    // the generated arrays to drive an In/NotIn query. For nested array bitmap
    // indexes, In/NotIn operate on element values: a row matches In when its
    // array contains at least one of the queried values.
    std::vector<T>
    CollectQueryValues(size_t max_vals, std::unordered_set<T>& s) {
        std::vector<T> test_data;
        for (size_t i = 0; i < data_.size() && s.size() < max_vals; i++) {
            auto& array = data_[i];
            for (size_t j = 0; j < array.length() && s.size() < max_vals; ++j) {
                auto val = array.template get_data<T>(j);
                if (s.insert(val).second) {
                    test_data.push_back(val);
                }
            }
        }
        return test_data;
    }

    void
    TestInFunc() {
        std::unordered_set<T> s;
        auto test_data = CollectQueryValues(10, s);
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
                milvus::Array& array = data_[i - start];
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

    void
    TestNotInFunc() {
        std::unordered_set<T> s;
        auto test_data = CollectQueryValues(10, s);
        auto index_ptr = dynamic_cast<index::ScalarIndex<T>*>(index_.get());
        auto bitset = index_ptr->NotIn(test_data.size(), test_data.data());
        size_t start = 0;
        if (has_lack_binlog_row_) {
            // all null here -> NotIn masks out null rows
            for (int i = 0; i < lack_binlog_row_; i++) {
                ASSERT_EQ(bitset[i], false);
            }
            start += lack_binlog_row_;
        }
        for (size_t i = start; i < bitset.size(); i++) {
            auto ref = [&]() -> bool {
                milvus::Array& array = data_[i - start];
                if (nullable_ && !valid_data_[i - start]) {
                    // NotIn(null) is false, masked by IsNotNull
                    return false;
                }
                for (size_t j = 0; j < array.length(); ++j) {
                    auto val = array.template get_data<T>(j);
                    if (s.find(val) != s.end()) {
                        // contains a queried value -> excluded from NotIn
                        return false;
                    }
                }
                return true;
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
    this->TestInFunc();
}

TYPED_TEST_P(ArrayBitmapIndexTest, NotINFuncTest) {
    this->TestNotInFunc();
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

TEST(BitmapIndexArrayNestedTest, BuildAndLoadElementLevelBitmap) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int32);
    field_schema.set_nullable(true);

    auto field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
    auto index_meta = storage::IndexMeta{3, 101, 3001, 3001};

    std::vector<ScalarFieldProto> scalar_arrays(4);
    scalar_arrays[0].mutable_int_data()->add_data(1);
    scalar_arrays[0].mutable_int_data()->add_data(2);
    // row 1 is an empty, non-null array.
    // row 2 is null and should not produce any nested element.
    scalar_arrays[3].mutable_int_data()->add_data(2);
    scalar_arrays[3].mutable_int_data()->add_data(3);

    std::vector<milvus::Array> array_data;
    array_data.reserve(scalar_arrays.size());
    for (const auto& scalar_array : scalar_arrays) {
        array_data.emplace_back(scalar_array);
    }

    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    uint8_t valid_data = 0x0B;  // rows 0, 1, and 3 are valid; row 2 is null.
    field_data->FillFieldData(
        array_data.data(), &valid_data, array_data.size(), 0);

    auto root_path = fmt::format("{}/bitmap_nested_array", TestLocalPath);
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto index = std::make_unique<index::BitmapIndex<int32_t>>(ctx, true);
    index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_TRUE(index->IsNestedIndex());
    ASSERT_TRUE(index->HasRawData());
    ASSERT_EQ(index->Count(), 4);

    auto binary_set = index->Serialize({});
    auto loaded_index =
        std::make_unique<index::BitmapIndex<int32_t>>(ctx, false);
    loaded_index->Load(binary_set, {});
    ASSERT_TRUE(loaded_index->IsNestedIndex());
    ASSERT_EQ(loaded_index->Count(), 4);

    int32_t value = 2;
    auto in_result = loaded_index->In(1, &value);
    ASSERT_EQ(in_result.size(), 4);
    EXPECT_FALSE(in_result[0]);
    EXPECT_TRUE(in_result[1]);
    EXPECT_TRUE(in_result[2]);
    EXPECT_FALSE(in_result[3]);

    auto not_in_result = loaded_index->NotIn(1, &value);
    ASSERT_EQ(not_in_result.size(), 4);
    EXPECT_TRUE(not_in_result[0]);
    EXPECT_FALSE(not_in_result[1]);
    EXPECT_FALSE(not_in_result[2]);
    EXPECT_TRUE(not_in_result[3]);

    auto is_null = loaded_index->IsNull();
    auto is_not_null = loaded_index->IsNotNull();
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(is_null[i]);
        EXPECT_TRUE(is_not_null[i]);
    }

    auto element = loaded_index->Reverse_Lookup(3);
    ASSERT_TRUE(element.has_value());
    EXPECT_EQ(element.value(), 3);

    boost::filesystem::remove_all(root_path);
}

TEST(BitmapIndexArrayNestedTest, UnifiedLoadRestoresNestedBitmapMeta) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_name("array[0]");
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int32);
    field_schema.set_nullable(true);

    auto field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
    auto index_meta = storage::IndexMeta{3, 101, 3002, 3002};

    std::vector<ScalarFieldProto> scalar_arrays(4);
    scalar_arrays[0].mutable_int_data()->add_data(1);
    scalar_arrays[0].mutable_int_data()->add_data(2);
    scalar_arrays[3].mutable_int_data()->add_data(2);
    scalar_arrays[3].mutable_int_data()->add_data(3);

    std::vector<milvus::Array> array_data;
    array_data.reserve(scalar_arrays.size());
    for (const auto& scalar_array : scalar_arrays) {
        array_data.emplace_back(scalar_array);
    }

    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    uint8_t valid_data = 0x0B;  // rows 0, 1, and 3 are valid; row 2 is null.
    field_data->FillFieldData(
        array_data.data(), &valid_data, array_data.size(), 0);

    auto root_path = fmt::format("{}/bitmap_nested_array_v3", TestLocalPath);
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto build_index = std::make_unique<index::BitmapIndex<int32_t>>(ctx, true);
    build_index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});

    auto create_index_result = build_index->UploadUnified({});
    ASSERT_EQ(create_index_result->GetIndexFiles().size(), 1);

    Config config;
    config["index_files"] = create_index_result->GetIndexFiles();
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;

    ctx.set_for_loading_index(true);
    auto loaded_index =
        std::make_unique<index::BitmapIndex<int32_t>>(ctx, false);
    ASSERT_FALSE(loaded_index->IsNestedIndex());

    loaded_index->LoadUnified(config);
    ASSERT_TRUE(loaded_index->IsNestedIndex());
    ASSERT_EQ(loaded_index->Count(), 4);

    int32_t value = 2;
    auto in_result = loaded_index->In(1, &value);
    ASSERT_EQ(in_result.size(), 4);
    EXPECT_FALSE(in_result[0]);
    EXPECT_TRUE(in_result[1]);
    EXPECT_TRUE(in_result[2]);
    EXPECT_FALSE(in_result[3]);

    auto is_null = loaded_index->IsNull();
    auto is_not_null = loaded_index->IsNotNull();
    for (size_t i = 0; i < 4; ++i) {
        EXPECT_FALSE(is_null[i]);
        EXPECT_TRUE(is_not_null[i]);
    }

    boost::filesystem::remove_all(root_path);
}

TEST(BitmapIndexArrayNestedTest, UnifiedMmapLoadRestoresNestedBitmapMeta) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_name("array[0]");
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int32);
    field_schema.set_nullable(true);

    auto field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
    auto index_meta = storage::IndexMeta{3, 101, 3003, 3003};

    const int32_t posting_count = DEFAULT_BITMAP_INDEX_BUILD_MODE_BOUND + 50;
    const int32_t target_value = posting_count + 7;
    std::vector<ScalarFieldProto> scalar_arrays(4);
    for (int32_t i = 0; i < posting_count; ++i) {
        scalar_arrays[0].mutable_int_data()->add_data(i);
    }
    scalar_arrays[3].mutable_int_data()->add_data(target_value);

    std::vector<milvus::Array> array_data;
    array_data.reserve(scalar_arrays.size());
    for (const auto& scalar_array : scalar_arrays) {
        array_data.emplace_back(scalar_array);
    }

    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, true);
    uint8_t valid_data = 0x0B;  // rows 0, 1, and 3 are valid; row 2 is null.
    field_data->FillFieldData(
        array_data.data(), &valid_data, array_data.size(), 0);

    auto root_path =
        fmt::format("{}/bitmap_nested_array_v3_mmap", TestLocalPath);
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    auto build_index = std::make_unique<index::BitmapIndex<int32_t>>(ctx, true);
    build_index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});

    auto create_index_result = build_index->UploadUnified({});
    ASSERT_EQ(create_index_result->GetIndexFiles().size(), 1);

    Config config;
    config["index_files"] = create_index_result->GetIndexFiles();
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;
    config[milvus::index::MMAP_FILE_PATH] =
        fmt::format("{}/bitmap_index.mmap", root_path);

    ctx.set_for_loading_index(true);
    auto loaded_index =
        std::make_unique<index::BitmapIndex<int32_t>>(ctx, false);
    ASSERT_FALSE(loaded_index->IsNestedIndex());

    loaded_index->LoadUnified(config);
    ASSERT_TRUE(loaded_index->IsNestedIndex());
    ASSERT_TRUE(loaded_index->is_mmap_);
    ASSERT_EQ(loaded_index->Count(), posting_count + 1);

    auto in_result = loaded_index->In(1, &target_value);
    ASSERT_EQ(in_result.size(), posting_count + 1);
    for (int32_t i = 0; i < posting_count; ++i) {
        EXPECT_FALSE(in_result[i]);
    }
    EXPECT_TRUE(in_result[posting_count]);

    auto is_null = loaded_index->IsNull();
    auto is_not_null = loaded_index->IsNotNull();
    for (size_t i = 0; i < is_null.size(); ++i) {
        EXPECT_FALSE(is_null[i]);
        EXPECT_TRUE(is_not_null[i]);
    }

    boost::filesystem::remove_all(root_path);
}

TEST(BitmapIndexArrayNestedTest, FactoryCreatesNestedBitmapForStructSubField) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_name("array");
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(proto::schema::DataType::Int32);

    auto field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
    auto index_meta = storage::IndexMeta{3, 101, 3001, 3001};

    auto root_path =
        fmt::format("{}/bitmap_nested_array_factory", TestLocalPath);
    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    storage::FileManagerContext ctx(field_meta, index_meta, chunk_manager, fs);

    index::CreateIndexInfo index_info{};
    index_info.index_type = milvus::index::BITMAP_INDEX_TYPE;
    index_info.field_type = DataType::ARRAY;
    index_info.field_name = "array[0]";

    auto index =
        index::IndexFactory::GetInstance().CreateIndex(index_info, ctx);
    ASSERT_TRUE(index->IsNestedIndex());
    ASSERT_NE(dynamic_cast<index::BitmapIndex<int32_t>*>(index.get()), nullptr);

    boost::filesystem::remove_all(root_path);
}

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

// ============================================================================
// Regression tests for struct-array nested-index bug fixes.
//
// These cover the compact-buffer / stride / byte-size / array-offsets fixes on
// branch fix/struct-array-nested-bugs. All nested scalar indexes are
// element-indexed: In()/Range()/Count() operate over the flattened, valid-only
// element stream (NULL rows and empty arrays contribute no element), which is
// exactly the path the bugs corrupted.
// ============================================================================

namespace {

// Build an ARRAY FieldData from per-row int proto arrays plus an optional
// validity bitmap. NULL rows are represented by an (ignored) placeholder Array
// entry; the validity bitmap marks them so the FieldData stores valid rows
// compactly -- the exact condition under which the old Data()[i] build overran.
storage::FileManagerContext
MakeNestedCtx(const std::string& root_path,
              proto::schema::DataType element_type,
              bool nullable,
              int64_t index_id) {
    proto::schema::FieldSchema field_schema;
    field_schema.set_name("struct_field[sub]");
    field_schema.set_data_type(proto::schema::DataType::Array);
    field_schema.set_element_type(element_type);
    field_schema.set_nullable(nullable);

    auto field_meta = storage::FieldDataMeta{1, 2, 3, 101, field_schema};
    auto index_meta = storage::IndexMeta{3, 101, index_id, index_id};

    boost::filesystem::remove_all(root_path);
    storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = root_path;
    auto chunk_manager = storage::CreateChunkManager(storage_config);
    auto fs = storage::InitArrowFileSystem(storage_config);
    return storage::FileManagerContext(
        field_meta, index_meta, chunk_manager, fs);
}

FieldDataPtr
MakeIntArrayFieldData(const std::vector<ScalarFieldProto>& scalar_arrays,
                      bool nullable,
                      const uint8_t* valid_data) {
    std::vector<milvus::Array> array_data;
    array_data.reserve(scalar_arrays.size());
    for (const auto& scalar_array : scalar_arrays) {
        array_data.emplace_back(scalar_array);
    }
    auto field_data =
        storage::CreateFieldData(DataType::ARRAY, DataType::NONE, nullable);
    if (nullable) {
        field_data->FillFieldData(
            array_data.data(), valid_data, array_data.size(), 0);
    } else {
        field_data->FillFieldData(array_data.data(), array_data.size());
    }
    return field_data;
}

}  // namespace

// Bug #1: nullable nested build with NULL rows *before* valid rows. Under the
// old code, the compact FieldData (valid rows only) was indexed with the
// logical row id, so any NULL preceding a valid row shifted every later row and
// eventually ran past the buffer. Verified through Serialize/Load and the
// V3 UploadUnified/LoadUnified round-trips.
TEST(BitmapIndexArrayNestedTest, NullableNullsBeforeValidElementBitmap) {
    // Logical rows: row0 NULL, row1 {10,20}, row2 NULL, row3 {} (empty,
    // non-null), row4 {20,30}, row5 {30}. Flattened valid elements:
    //   e0=10 e1=20 (row1) | e2=20 e3=30 (row4) | e4=30 (row5)  => 5 elements.
    std::vector<ScalarFieldProto> scalar_arrays(6);
    scalar_arrays[1].mutable_int_data()->add_data(10);
    scalar_arrays[1].mutable_int_data()->add_data(20);
    // row3 empty non-null array -> no elements.
    scalar_arrays[4].mutable_int_data()->add_data(20);
    scalar_arrays[4].mutable_int_data()->add_data(30);
    scalar_arrays[5].mutable_int_data()->add_data(30);

    // bit i == 1 means row i is valid. rows 1,3,4,5 valid; rows 0,2 null.
    uint8_t valid_data = 0b00111010;  // 0x3A

    auto root_path =
        fmt::format("{}/bitmap_nested_nulls_before", TestLocalPath);
    auto ctx =
        MakeNestedCtx(root_path, proto::schema::DataType::Int32, true, 3101);
    auto field_data = MakeIntArrayFieldData(scalar_arrays, true, &valid_data);

    auto index = std::make_unique<index::BitmapIndex<int32_t>>(ctx, true);
    index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_TRUE(index->IsNestedIndex());
    ASSERT_EQ(index->Count(), 5);

    auto check = [](index::BitmapIndex<int32_t>* idx) {
        // In(10) -> only e0.
        int32_t v10 = 10;
        auto r10 = idx->In(1, &v10);
        ASSERT_EQ(r10.size(), 5);
        EXPECT_TRUE(r10[0]);
        EXPECT_FALSE(r10[1]);
        EXPECT_FALSE(r10[2]);
        EXPECT_FALSE(r10[3]);
        EXPECT_FALSE(r10[4]);

        // In(20) -> e1 (row1) and e2 (row4).
        int32_t v20 = 20;
        auto r20 = idx->In(1, &v20);
        EXPECT_FALSE(r20[0]);
        EXPECT_TRUE(r20[1]);
        EXPECT_TRUE(r20[2]);
        EXPECT_FALSE(r20[3]);
        EXPECT_FALSE(r20[4]);

        // In(30) -> e3 (row4) and e4 (row5).
        int32_t v30 = 30;
        auto r30 = idx->In(1, &v30);
        EXPECT_FALSE(r30[0]);
        EXPECT_FALSE(r30[1]);
        EXPECT_FALSE(r30[2]);
        EXPECT_TRUE(r30[3]);
        EXPECT_TRUE(r30[4]);

        // Range > 15 -> all the 20s and 30s but not the 10.
        auto rg = idx->Range(15, OpType::GreaterThan);
        ASSERT_EQ(rg.size(), 5);
        EXPECT_FALSE(rg[0]);
        EXPECT_TRUE(rg[1]);
        EXPECT_TRUE(rg[2]);
        EXPECT_TRUE(rg[3]);
        EXPECT_TRUE(rg[4]);

        // Reverse_Lookup maps element id -> value.
        EXPECT_EQ(idx->Reverse_Lookup(0).value(), 10);
        EXPECT_EQ(idx->Reverse_Lookup(4).value(), 30);
    };

    // Note: a freshly built BitmapIndex finalizes its queryable structures on
    // Serialize/Load, so In()/Range()/Reverse_Lookup() are validated on the
    // loaded index (the production path), not the build-time index.

    // BinarySet serialize -> load round-trip.
    auto binary_set = index->Serialize({});
    auto loaded = std::make_unique<index::BitmapIndex<int32_t>>(ctx, false);
    loaded->Load(binary_set, {});
    ASSERT_TRUE(loaded->IsNestedIndex());
    ASSERT_EQ(loaded->Count(), 5);
    check(loaded.get());

    boost::filesystem::remove_all(root_path);
}

TEST(BitmapIndexArrayNestedTest, NullableNullsBeforeValidUnifiedLoad) {
    std::vector<ScalarFieldProto> scalar_arrays(6);
    scalar_arrays[1].mutable_int_data()->add_data(10);
    scalar_arrays[1].mutable_int_data()->add_data(20);
    scalar_arrays[4].mutable_int_data()->add_data(20);
    scalar_arrays[4].mutable_int_data()->add_data(30);
    scalar_arrays[5].mutable_int_data()->add_data(30);
    uint8_t valid_data = 0b00111010;

    auto root_path =
        fmt::format("{}/bitmap_nested_nulls_before_v3", TestLocalPath);
    auto ctx =
        MakeNestedCtx(root_path, proto::schema::DataType::Int32, true, 3102);
    auto field_data = MakeIntArrayFieldData(scalar_arrays, true, &valid_data);

    auto build_index = std::make_unique<index::BitmapIndex<int32_t>>(ctx, true);
    build_index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    auto create_index_result = build_index->UploadUnified({});
    ASSERT_EQ(create_index_result->GetIndexFiles().size(), 1);

    Config config;
    config["index_files"] = create_index_result->GetIndexFiles();
    config[milvus::LOAD_PRIORITY] = milvus::proto::common::LoadPriority::HIGH;

    ctx.set_for_loading_index(true);
    auto loaded = std::make_unique<index::BitmapIndex<int32_t>>(ctx, false);
    loaded->LoadUnified(config);
    ASSERT_TRUE(loaded->IsNestedIndex());
    ASSERT_EQ(loaded->Count(), 5);

    int32_t v30 = 30;
    auto r30 = loaded->In(1, &v30);
    ASSERT_EQ(r30.size(), 5);
    EXPECT_FALSE(r30[0]);
    EXPECT_FALSE(r30[1]);
    EXPECT_FALSE(r30[2]);
    EXPECT_TRUE(r30[3]);
    EXPECT_TRUE(r30[4]);

    boost::filesystem::remove_all(root_path);
}

// Bug #2: int8/int16 elements are physically stored as int32. get_data<T> must
// take the 4-byte-stride-then-narrow branch; the old code fell through to a
// 1-byte reinterpret for int8_t/int16_t and produced garbage values during the
// nested build. Build a typed nested bitmap and confirm element values index
// correctly (and identically to the int32 baseline).
template <typename T>
static void
RunNarrowIntNestedBitmapTest(proto::schema::DataType element_type,
                             const std::string& tag,
                             int64_t index_id) {
    // row0 {5,-3}, row1 {-3,100}, row2 {100}. Flattened: e0=5 e1=-3 e2=-3
    // e3=100 e4=100 => 5 elements. Values chosen inside int8 range.
    std::vector<ScalarFieldProto> scalar_arrays(3);
    scalar_arrays[0].mutable_int_data()->add_data(5);
    scalar_arrays[0].mutable_int_data()->add_data(-3);
    scalar_arrays[1].mutable_int_data()->add_data(-3);
    scalar_arrays[1].mutable_int_data()->add_data(100);
    scalar_arrays[2].mutable_int_data()->add_data(100);

    auto root_path =
        fmt::format("{}/bitmap_nested_narrow_{}", TestLocalPath, tag);
    auto ctx = MakeNestedCtx(root_path, element_type, false, index_id);
    auto field_data = MakeIntArrayFieldData(scalar_arrays, false, nullptr);

    auto build_index = std::make_unique<index::BitmapIndex<T>>(ctx, true);
    build_index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_TRUE(build_index->IsNestedIndex());
    ASSERT_EQ(build_index->Count(), 5) << tag;

    // Query the loaded index (queryable structures are finalized on load).
    auto binary_set = build_index->Serialize({});
    auto index = std::make_unique<index::BitmapIndex<T>>(ctx, false);
    index->Load(binary_set, {});
    ASSERT_TRUE(index->IsNestedIndex());
    ASSERT_EQ(index->Count(), 5) << tag;

    T v_neg = static_cast<T>(-3);
    auto r_neg = index->In(1, &v_neg);
    ASSERT_EQ(r_neg.size(), 5);
    EXPECT_FALSE(r_neg[0]) << tag;
    EXPECT_TRUE(r_neg[1]) << tag;
    EXPECT_TRUE(r_neg[2]) << tag;
    EXPECT_FALSE(r_neg[3]) << tag;
    EXPECT_FALSE(r_neg[4]) << tag;

    T v100 = static_cast<T>(100);
    auto r100 = index->In(1, &v100);
    EXPECT_FALSE(r100[0]) << tag;
    EXPECT_FALSE(r100[1]) << tag;
    EXPECT_FALSE(r100[2]) << tag;
    EXPECT_TRUE(r100[3]) << tag;
    EXPECT_TRUE(r100[4]) << tag;

    // Range > 0 -> e0(5), e3(100), e4(100).
    auto rg = index->Range(static_cast<T>(0), OpType::GreaterThan);
    ASSERT_EQ(rg.size(), 5);
    EXPECT_TRUE(rg[0]) << tag;
    EXPECT_FALSE(rg[1]) << tag;
    EXPECT_FALSE(rg[2]) << tag;
    EXPECT_TRUE(rg[3]) << tag;
    EXPECT_TRUE(rg[4]) << tag;

    EXPECT_EQ(index->Reverse_Lookup(0).value(), static_cast<T>(5)) << tag;
    EXPECT_EQ(index->Reverse_Lookup(1).value(), static_cast<T>(-3)) << tag;

    boost::filesystem::remove_all(root_path);
}

TEST(BitmapIndexArrayNestedTest, Int8NestedElementBitmapStride) {
    RunNarrowIntNestedBitmapTest<int8_t>(
        proto::schema::DataType::Int8, "int8", 3110);
}

TEST(BitmapIndexArrayNestedTest, Int16NestedElementBitmapStride) {
    RunNarrowIntNestedBitmapTest<int16_t>(
        proto::schema::DataType::Int16, "int16", 3111);
    // int16 also covers values outside the int8 range.
    std::vector<ScalarFieldProto> scalar_arrays(2);
    scalar_arrays[0].mutable_int_data()->add_data(300);
    scalar_arrays[0].mutable_int_data()->add_data(-300);
    scalar_arrays[1].mutable_int_data()->add_data(300);

    auto root_path = fmt::format("{}/bitmap_nested_int16_wide", TestLocalPath);
    auto ctx =
        MakeNestedCtx(root_path, proto::schema::DataType::Int16, false, 3112);
    auto field_data = MakeIntArrayFieldData(scalar_arrays, false, nullptr);
    auto build_index = std::make_unique<index::BitmapIndex<int16_t>>(ctx, true);
    build_index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_EQ(build_index->Count(), 3);
    auto binary_set = build_index->Serialize({});
    auto index = std::make_unique<index::BitmapIndex<int16_t>>(ctx, false);
    index->Load(binary_set, {});
    ASSERT_EQ(index->Count(), 3);
    int16_t v300 = 300;
    auto r = index->In(1, &v300);
    EXPECT_TRUE(r[0]);
    EXPECT_FALSE(r[1]);
    EXPECT_TRUE(r[2]);
    boost::filesystem::remove_all(root_path);
}

// Bug #3: ScalarIndexSort::BuildWithArrayDataNested previously skipped the
// final ComputeByteSize(), so ByteSize() under-reported (stayed 0) for nested
// numeric sort indexes. Also confirms the nested STL-sort build reads compact
// nullable FieldData correctly (same RawValue fix as bug #1).
TEST(ScalarIndexSortArrayNestedTest, NestedBuildByteSizeAndQuery) {
    std::vector<ScalarFieldProto> scalar_arrays(6);
    scalar_arrays[1].mutable_int_data()->add_data(10);
    scalar_arrays[1].mutable_int_data()->add_data(20);
    scalar_arrays[4].mutable_int_data()->add_data(20);
    scalar_arrays[4].mutable_int_data()->add_data(30);
    scalar_arrays[5].mutable_int_data()->add_data(30);
    uint8_t valid_data = 0b00111010;  // rows 1,3,4,5 valid; 0,2 null

    auto root_path = fmt::format("{}/stlsort_nested_array", TestLocalPath);
    auto ctx =
        MakeNestedCtx(root_path, proto::schema::DataType::Int32, true, 3120);
    auto field_data = MakeIntArrayFieldData(scalar_arrays, true, &valid_data);

    auto index = std::make_unique<index::ScalarIndexSort<int32_t>>(ctx, true);
    index->BuildWithFieldData(std::vector<FieldDataPtr>{field_data});
    ASSERT_EQ(index->Count(), 5);

    // Bug #3 regression: byte size must be accounted (was 0 before the fix).
    EXPECT_GT(index->ByteSize(), 0);

    // Element-indexed Range query over the flattened valid elements.
    auto rg = index->Range(15, OpType::GreaterThan);
    ASSERT_EQ(rg.size(), 5);
    EXPECT_FALSE(rg[0]);  // e0 = 10
    EXPECT_TRUE(rg[1]);   // e1 = 20
    EXPECT_TRUE(rg[2]);   // e2 = 20
    EXPECT_TRUE(rg[3]);   // e3 = 30
    EXPECT_TRUE(rg[4]);   // e4 = 30

    int32_t v30 = 30;
    auto in30 = index->In(1, &v30);
    ASSERT_EQ(in30.size(), 5);
    EXPECT_FALSE(in30[0]);
    EXPECT_FALSE(in30[1]);
    EXPECT_FALSE(in30[2]);
    EXPECT_TRUE(in30[3]);
    EXPECT_TRUE(in30[4]);

    boost::filesystem::remove_all(root_path);
}

TEST(ScalarIndexSortArrayNestedTest, ArraySortIndexDoesNotExposeRawArrayData) {
    auto numeric_root_path =
        fmt::format("{}/stlsort_array_has_raw_data", TestLocalPath);
    auto numeric_ctx = MakeNestedCtx(
        numeric_root_path, proto::schema::DataType::Int32, true, 3121);
    auto numeric_index =
        std::make_unique<index::ScalarIndexSort<int32_t>>(numeric_ctx, true);
    EXPECT_TRUE(numeric_index->IsNestedIndex());
    EXPECT_FALSE(numeric_index->HasRawData());
    auto numeric_index_from_schema =
        std::make_unique<index::ScalarIndexSort<int32_t>>(numeric_ctx, false);
    EXPECT_FALSE(numeric_index_from_schema->IsNestedIndex());
    EXPECT_FALSE(numeric_index_from_schema->HasRawData());

    auto string_root_path =
        fmt::format("{}/stringsort_array_has_raw_data", TestLocalPath);
    auto string_ctx = MakeNestedCtx(
        string_root_path, proto::schema::DataType::VarChar, true, 3122);
    auto string_index =
        std::make_unique<index::StringIndexSort>(string_ctx, true);
    EXPECT_TRUE(string_index->IsNestedIndex());
    EXPECT_FALSE(string_index->HasRawData());
    auto string_index_from_schema =
        std::make_unique<index::StringIndexSort>(string_ctx, false);
    EXPECT_FALSE(string_index_from_schema->IsNestedIndex());
    EXPECT_FALSE(string_index_from_schema->HasRawData());

    std::map<std::string, std::string> index_params{
        {index::INDEX_TYPE, index::ASCENDING_SORT}};
    auto request = index::IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::ARRAY, 0, 1024, index_params, false, 10);
    EXPECT_FALSE(request.has_raw_data);

    boost::filesystem::remove_all(numeric_root_path);
    boost::filesystem::remove_all(string_root_path);
}

TEST(BitmapIndexLoadResourceTest,
     EncryptedNonMmapIncludesTargetAndStreamMemory) {
    auto& budget = storage::TransientMemoryBudget::GetLoadTransientBudget();
    auto old_capacity = budget.CapacityBytes();
    auto& plugin_loader = storage::PluginLoader::GetInstance();
    auto cleanup = folly::makeGuard([&]() {
        budget.SetCapacityBytes(old_capacity);
        plugin_loader.unload("CipherPlugin");
    });
    budget.SetCapacityBytes(0);
    plugin_loader.addPluginForTest(
        std::make_shared<milvus::test::PlannerCipherPlugin>());

    constexpr uint64_t index_size = 32 * 1024 * 1024;
    std::map<std::string, std::string> index_params{
        {index::INDEX_TYPE, index::BITMAP_INDEX_TYPE},
        {index::SCALAR_INDEX_ENGINE_VERSION, "3"}};
    auto request = index::IndexFactory::GetInstance().ScalarIndexLoadResource(
        DataType::INT64, 0, index_size, index_params, false, 1024);

    EXPECT_EQ(request.final_memory_cost, index_size);
    EXPECT_EQ(request.max_memory_cost, 4 * index_size);
    EXPECT_EQ(request.final_disk_cost, 0);
    EXPECT_EQ(request.max_disk_cost, 0);
}

// Bug #4: ArrayOffsetsSealed::BuildAllZeros is used in the add-field /
// schema-evolution path to materialize empty (all-zeros) offsets for old rows.
// It must charge the caching layer so the destructor's refund is balanced, and
// must present every old row as an empty array (so MATCH/element_filter treats
// them as zero-element rows rather than crashing on missing offsets).
TEST(ArrayOffsetsSealedTest, BuildAllZerosEmptyArraysAndBalancedResource) {
    constexpr int64_t kRowCount = 1000;
    auto offsets = milvus::ArrayOffsetsSealed::BuildAllZeros(kRowCount);
    ASSERT_NE(offsets, nullptr);

    EXPECT_EQ(offsets->GetRowCount(), kRowCount);
    // Every row is an empty array -> zero total elements.
    EXPECT_EQ(offsets->GetTotalElementCount(), 0);

    // Each old row maps to an empty element range [x, x).
    for (int32_t row : {0, 1, 499, 999}) {
        auto range = offsets->ElementIDRangeOfRow(row);
        EXPECT_EQ(range.first, range.second)
            << "row " << row << " should be an empty array";
        EXPECT_EQ(range.first, 0);
    }

    // Repeated build/destroy must charge and refund symmetrically (the old code
    // left this memory untracked); an underflowing refund would assert/crash in
    // the caching layer dlist. Surviving the loop guards the charge/refund pair.
    for (int i = 0; i < 256; ++i) {
        auto tmp = milvus::ArrayOffsetsSealed::BuildAllZeros(500);
        ASSERT_EQ(tmp->GetRowCount(), 500);
        ASSERT_EQ(tmp->GetTotalElementCount(), 0);
    }
}
