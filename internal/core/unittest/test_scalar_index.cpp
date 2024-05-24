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

#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <gtest/gtest.h>

#include "gtest/gtest-typed-test.h"
#include "index/IndexFactory.h"
#include "common/CDataType.h"
#include "knowhere/comp/index_param.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"
#include <boost/filesystem.hpp>
#include "test_utils/storage_test_utils.h"
#include "test_utils/TmpPath.h"

constexpr int64_t nb = 100;
namespace indexcgo = milvus::proto::indexcgo;
namespace schemapb = milvus::proto::schema;
using milvus::index::ScalarIndexPtr;
using milvus::segcore::GeneratedData;
template <typename T>
class TypedScalarIndexTest : public ::testing::Test {
 protected:
    // void
    // SetUp() override {
    // }

    // void
    // TearDown() override {
    // }
};

TYPED_TEST_SUITE_P(TypedScalarIndexTest);

TYPED_TEST_P(TypedScalarIndexTest, Dummy) {
    using T = TypeParam;
    std::cout << typeid(T()).name() << std::endl;
    std::cout << milvus::GetDType<T>() << std::endl;
}

TYPED_TEST_P(TypedScalarIndexTest, Constructor) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Count) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        ASSERT_EQ(nb, scalar_index->Count());
    }
}

TYPED_TEST_P(TypedScalarIndexTest, HasRawData) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        ASSERT_EQ(nb, scalar_index->Count());
        ASSERT_TRUE(scalar_index->HasRawData());
    }
}

TYPED_TEST_P(TypedScalarIndexTest, In) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_in<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, NotIn) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_not_in<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Reverse) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_reverse<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Range) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());
        assert_range<T>(scalar_index, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexTest, Codec) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypes<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());

        auto binary_set = index->Serialize(nullptr);
        auto copy_index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info);
        copy_index->Load(binary_set);

        auto copy_scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(copy_index.get());
        ASSERT_EQ(nb, copy_scalar_index->Count());
        assert_in<T>(copy_scalar_index, arr);
        assert_not_in<T>(copy_scalar_index, arr);
        assert_range<T>(copy_scalar_index, arr);
    }
}

// TODO: it's easy to overflow for int8_t. Design more reasonable ut.
using ScalarT =
    ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double>;

REGISTER_TYPED_TEST_SUITE_P(TypedScalarIndexTest,
                            Dummy,
                            Constructor,
                            Count,
                            In,
                            NotIn,
                            Range,
                            Codec,
                            Reverse,
                            HasRawData);

INSTANTIATE_TYPED_TEST_SUITE_P(ArithmeticCheck, TypedScalarIndexTest, ScalarT);

template <typename T>
class TypedScalarIndexTestV2 : public ::testing::Test {
 public:
    struct Helper {};

 protected:
};

static std::unordered_map<std::type_index,
                          const std::shared_ptr<arrow::DataType>>
    m_fields = {{typeid(int8_t), arrow::int8()},
                {typeid(int16_t), arrow::int16()},
                {typeid(int32_t), arrow::int32()},
                {typeid(int64_t), arrow::int64()},
                {typeid(float), arrow::float32()},
                {typeid(double), arrow::float64()}};

template <typename T>
std::shared_ptr<arrow::Schema>
TestSchema(int vec_size) {
    arrow::FieldVector fields;
    fields.push_back(arrow::field("pk", arrow::int64()));
    fields.push_back(arrow::field("ts", arrow::int64()));
    fields.push_back(arrow::field("scalar", m_fields[typeid(T)]));
    fields.push_back(arrow::field("vec", arrow::fixed_size_binary(vec_size)));
    return std::make_shared<arrow::Schema>(fields);
}

template <typename T>
std::shared_ptr<arrow::RecordBatchReader>
TestRecords(int vec_size, GeneratedData& dataset, std::vector<T>& scalars) {
    arrow::Int64Builder pk_builder;
    arrow::Int64Builder ts_builder;
    arrow::NumericBuilder<typename TypedScalarIndexTestV2<T>::Helper::C>
        scalar_builder;
    arrow::FixedSizeBinaryBuilder vec_builder(
        arrow::fixed_size_binary(vec_size));
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));
    auto data = reinterpret_cast<char*>(xb_data.data());
    for (auto i = 0; i < nb; ++i) {
        EXPECT_TRUE(pk_builder.Append(i).ok());
        EXPECT_TRUE(ts_builder.Append(i).ok());
        EXPECT_TRUE(vec_builder.Append(data + i * vec_size).ok());
    }
    for (auto& v : scalars) {
        EXPECT_TRUE(scalar_builder.Append(v).ok());
    }
    std::shared_ptr<arrow::Array> pk_array;
    EXPECT_TRUE(pk_builder.Finish(&pk_array).ok());
    std::shared_ptr<arrow::Array> ts_array;
    EXPECT_TRUE(ts_builder.Finish(&ts_array).ok());
    std::shared_ptr<arrow::Array> scalar_array;
    EXPECT_TRUE(scalar_builder.Finish(&scalar_array).ok());
    std::shared_ptr<arrow::Array> vec_array;
    EXPECT_TRUE(vec_builder.Finish(&vec_array).ok());
    auto schema = TestSchema<T>(vec_size);
    auto rec_batch = arrow::RecordBatch::Make(
        schema, nb, {pk_array, ts_array, scalar_array, vec_array});
    auto reader =
        arrow::RecordBatchReader::Make({rec_batch}, schema).ValueOrDie();
    return reader;
}

template <typename T>
std::shared_ptr<milvus_storage::Space>
TestSpace(boost::filesystem::path& temp_path,
          int vec_size,
          GeneratedData& dataset,
          std::vector<T>& scalars) {
    auto arrow_schema = TestSchema<T>(vec_size);
    milvus_storage::SchemaOptions schema_options{
        .primary_column = "pk", .version_column = "ts", .vector_column = "vec"};
    auto schema =
        std::make_shared<milvus_storage::Schema>(arrow_schema, schema_options);
    EXPECT_TRUE(schema->Validate().ok());

    auto space_res = milvus_storage::Space::Open(
        "file://" + boost::filesystem::canonical(temp_path).string(),
        milvus_storage::Options{schema});
    EXPECT_TRUE(space_res.has_value());

    auto space = std::move(space_res.value());
    auto rec = TestRecords<T>(vec_size, dataset, scalars);
    auto write_opt = milvus_storage::WriteOption{nb};
    space->Write(*rec, write_opt);
    return std::move(space);
}

template <>
struct TypedScalarIndexTestV2<int8_t>::Helper {
    using C = arrow::Int8Type;
};

template <>
struct TypedScalarIndexTestV2<int16_t>::Helper {
    using C = arrow::Int16Type;
};

template <>
struct TypedScalarIndexTestV2<int32_t>::Helper {
    using C = arrow::Int32Type;
};

template <>
struct TypedScalarIndexTestV2<int64_t>::Helper {
    using C = arrow::Int64Type;
};

template <>
struct TypedScalarIndexTestV2<float>::Helper {
    using C = arrow::FloatType;
};

template <>
struct TypedScalarIndexTestV2<double>::Helper {
    using C = arrow::DoubleType;
};

TYPED_TEST_SUITE_P(TypedScalarIndexTestV2);

TYPED_TEST_P(TypedScalarIndexTestV2, Base) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    auto index_types = GetIndexTypesV2<T>();
    for (const auto& index_type : index_types) {
        milvus::index::CreateIndexInfo create_index_info;
        create_index_info.field_type = milvus::DataType(dtype);
        create_index_info.index_type = index_type;
        create_index_info.field_name = "scalar";

        auto storage_config = get_default_local_storage_config();
        auto chunk_manager =
            milvus::storage::CreateChunkManager(storage_config);

        milvus::test::TmpPath tmp_path;
        auto temp_path = tmp_path.get();
        auto vec_size = DIM * 4;
        auto dataset = GenDataset(nb, knowhere::metric::L2, false);
        auto scalars = GenSortedArr<T>(nb);
        auto space = TestSpace<T>(temp_path, vec_size, dataset, scalars);
        milvus::storage::FileManagerContext file_manager_context(
            {}, {.field_name = "scalar"}, chunk_manager, space);
        auto index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, file_manager_context, space);
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        scalar_index->BuildV2();
        scalar_index->UploadV2();

        auto new_index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, file_manager_context, space);
        auto new_scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(new_index.get());
        new_scalar_index->LoadV2();
        ASSERT_EQ(nb, new_scalar_index->Count());
    }
}

REGISTER_TYPED_TEST_SUITE_P(TypedScalarIndexTestV2, Base);

INSTANTIATE_TYPED_TEST_SUITE_P(ArithmeticCheck,
                               TypedScalarIndexTestV2,
                               ScalarT);
