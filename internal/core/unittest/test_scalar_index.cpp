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
#include "index/BitmapIndex.h"
#include "index/InvertedIndexTantivy.h"
#include "index/ScalarIndex.h"
#include "common/CDataType.h"
#include "common/Types.h"
#include "knowhere/comp/index_param.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/AssertUtils.h"
#include "test_utils/DataGen.h"
#include <boost/filesystem.hpp>
#include "test_utils/storage_test_utils.h"
#include "test_utils/TmpPath.h"
#include "storage/Util.h"

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

auto
GetTempFileManagerCtx(CDataType data_type) {
    milvus::storage::StorageConfig storage_config;
    storage_config.storage_type = "local";
    storage_config.root_path = "/tmp/local/";
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    auto ctx = milvus::storage::FileManagerContext(chunk_manager);
    ctx.fieldDataMeta.field_schema.set_data_type(
        static_cast<milvus::proto::schema::DataType>(data_type));
    return ctx;
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
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
                create_index_info, GetTempFileManagerCtx(dtype));
        auto scalar_index =
            dynamic_cast<milvus::index::ScalarIndex<T>*>(index.get());
        auto arr = GenSortedArr<T>(nb);
        scalar_index->Build(nb, arr.data());

        auto binary_set = index->Serialize(nullptr);
        auto copy_index =
            milvus::index::IndexFactory::GetInstance().CreateScalarIndex(
                create_index_info, GetTempFileManagerCtx(dtype));
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

using namespace milvus::index;
template <typename T>
std::vector<T>
GenerateRawData(int N, int cardinality) {
    using std::vector;
    std::default_random_engine random(60);
    std::normal_distribution<> distr(0, 1);
    vector<T> data(N);
    for (auto& x : data) {
        x = random() % (cardinality);
    }
    return data;
}

template <>
std::vector<std::string>
GenerateRawData(int N, int cardinality) {
    using std::vector;
    std::default_random_engine random(60);
    std::normal_distribution<> distr(0, 1);
    vector<std::string> data(N);
    for (auto& x : data) {
        x = std::to_string(random() % (cardinality));
    }
    return data;
}

template <typename T>
IndexBasePtr
TestBuildIndex(int N, int cardinality, int index_type) {
    auto raw_data = GenerateRawData<T>(N, cardinality);
    if (index_type == 0) {
        auto index = std::make_unique<milvus::index::BitmapIndex<T>>();
        index->Build(N, raw_data.data());
        return std::move(index);
    } else if (index_type == 1) {
        if constexpr (std::is_same_v<T, std::string>) {
            auto index = std::make_unique<milvus::index::StringIndexMarisa>();
            index->Build(N, raw_data.data());
            return std::move(index);
        }
        auto index = milvus::index::CreateScalarIndexSort<T>();
        index->Build(N, raw_data.data());
        return std::move(index);
    }
}

template <typename T>
void
TestIndexSearchIn() {
    // low data cardinality
    {
        int N = 1000;
        std::vector<int> data_cardinality = {10, 20, 100};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());
            std::vector<T> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(static_cast<T>(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }

    // high data cardinality
    {
        int N = 10000;
        std::vector<int> data_cardinality = {1001, 2000};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());
            std::vector<T> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(static_cast<T>(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
}

template <>
void
TestIndexSearchIn<std::string>() {
    // low data cardinality
    {
        int N = 1000;
        std::vector<int> data_cardinality = {10, 20, 100};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<std::string>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<std::string>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(sort_index.get());
            std::vector<std::string> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(std::to_string(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
    // high data cardinality
    {
        int N = 10000;
        std::vector<int> data_cardinality = {1001, 2000};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<std::string>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<std::string>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<std::string>*>(sort_index.get());
            std::vector<std::string> terms;
            for (int i = 0; i < 10; i++) {
                terms.push_back(std::to_string(i));
            }
            auto final1 = bitmap_index_ptr->In(10, terms.data());
            auto final2 = sort_index_ptr->In(10, terms.data());
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->NotIn(10, terms.data());
            auto final4 = sort_index_ptr->NotIn(10, terms.data());
            EXPECT_EQ(final4.size(), final3.size());
            for (int i = 0; i < final3.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
}

TEST(ScalarTest, test_function_In) {
    TestIndexSearchIn<int8_t>();
    TestIndexSearchIn<int16_t>();
    TestIndexSearchIn<int32_t>();
    TestIndexSearchIn<int64_t>();
    TestIndexSearchIn<float>();
    TestIndexSearchIn<double>();
    TestIndexSearchIn<std::string>();
}

template <typename T>
void
TestIndexSearchRange() {
    // low data cordinality
    {
        int N = 1000;
        std::vector<int> data_cardinality = {10, 20, 100};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());

            auto final1 = bitmap_index_ptr->Range(10, milvus::OpType::LessThan);
            auto final2 = sort_index_ptr->Range(10, milvus::OpType::LessThan);
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->Range(10, true, 100, false);
            auto final4 = sort_index_ptr->Range(10, true, 100, false);
            EXPECT_EQ(final3.size(), final4.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }

    // high data cordinality
    {
        int N = 10000;
        std::vector<int> data_cardinality = {1001, 2000};
        for (auto& card : data_cardinality) {
            auto bitmap_index = TestBuildIndex<T>(N, card, 0);
            auto bitmap_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(bitmap_index.get());
            auto sort_index = TestBuildIndex<T>(N, card, 1);
            auto sort_index_ptr =
                dynamic_cast<ScalarIndex<T>*>(sort_index.get());

            auto final1 = bitmap_index_ptr->Range(10, milvus::OpType::LessThan);
            auto final2 = sort_index_ptr->Range(10, milvus::OpType::LessThan);
            EXPECT_EQ(final1.size(), final2.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final1[i], final2[i]);
            }

            auto final3 = bitmap_index_ptr->Range(10, true, 100, false);
            auto final4 = sort_index_ptr->Range(10, true, 100, false);
            EXPECT_EQ(final3.size(), final4.size());
            for (int i = 0; i < final1.size(); i++) {
                EXPECT_EQ(final3[i], final4[i]);
            }
        }
    }
}

TEST(ScalarTest, test_function_range) {
    TestIndexSearchRange<int8_t>();
    TestIndexSearchRange<int16_t>();
    TestIndexSearchRange<int32_t>();
    TestIndexSearchRange<int64_t>();
    TestIndexSearchRange<float>();
    TestIndexSearchRange<double>();
}
