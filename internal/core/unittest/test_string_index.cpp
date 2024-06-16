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

#include <arrow/array/builder_binary.h>
#include <arrow/type.h>
#include <gtest/gtest.h>

#include "index/Index.h"
#include "index/ScalarIndex.h"
#include "index/StringIndexMarisa.h"

#include "index/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/AssertUtils.h"
#include <boost/filesystem.hpp>
#include "test_utils/storage_test_utils.h"

constexpr int64_t nb = 100;
namespace schemapb = milvus::proto::schema;

namespace milvus {
namespace index {
class StringIndexBaseTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        strs = GenStrArr(nb);
        *str_arr.mutable_data() = {strs.begin(), strs.end()};
    }

 protected:
    std::vector<std::string> strs;
    schemapb::StringArray str_arr;
};

class StringIndexMarisaTest : public StringIndexBaseTest {};

TEST_F(StringIndexMarisaTest, Constructor) {
    auto index = milvus::index::CreateStringIndexMarisa();
}

TEST_F(StringIndexMarisaTest, Build) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(strs.size(), strs.data());
}

TEST_F(StringIndexMarisaTest, HasRawData) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    ASSERT_TRUE(index->HasRawData());
}

TEST_F(StringIndexMarisaTest, Count) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    ASSERT_EQ(strs.size(), index->Count());
}

TEST_F(StringIndexMarisaTest, In) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    auto bitset = index->In(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(Any(bitset));
}

TEST_F(StringIndexMarisaTest, NotIn) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());
    auto bitset = index->NotIn(strs.size(), strs.data());
    ASSERT_EQ(bitset.size(), strs.size());
    ASSERT_TRUE(BitSetNone(bitset));
}

TEST_F(StringIndexMarisaTest, Range) {
    auto index = milvus::index::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }
    index->Build(nb, strings.data());

    {
        auto bitset = index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = index->Range("0", true, "9", true);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = index->Range("0", true, "90", false);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }
}

TEST_F(StringIndexMarisaTest, Reverse) {
    auto index_types = GetIndexTypes<std::string>();
    for (const auto& index_type : index_types) {
        auto index = milvus::index::IndexFactory::GetInstance()
                         .CreatePrimitiveScalarIndex<std::string>(index_type);
        index->Build(nb, strs.data());
        assert_reverse<std::string>(index.get(), strs);
    }
}

TEST_F(StringIndexMarisaTest, PrefixMatch) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());

    for (size_t i = 0; i < strs.size(); i++) {
        auto str = strs[i];
        auto bitset = index->PrefixMatch(str);
        ASSERT_EQ(bitset.size(), strs.size());
        ASSERT_TRUE(bitset[i]);
    }
}

TEST_F(StringIndexMarisaTest, Query) {
    auto index = milvus::index::CreateStringIndexMarisa();
    index->Build(nb, strs.data());

    {
        auto ds = knowhere::GenDataSet(strs.size(), 8, strs.data());
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::In);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(Any(bitset));
    }

    {
        auto ds = knowhere::GenDataSet(strs.size(), 8, strs.data());
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::NotIn);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto ds = std::make_shared<knowhere::DataSet>();
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::GreaterEqual);
        ds->Set<std::string>(milvus::index::RANGE_VALUE, "0");
        auto bitset = index->Query(ds);
        ASSERT_EQ(bitset.size(), strs.size());
        ASSERT_EQ(Count(bitset), strs.size());
    }

    {
        auto ds = std::make_shared<knowhere::DataSet>();
        ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                milvus::OpType::Range);
        ds->Set<std::string>(milvus::index::LOWER_BOUND_VALUE, "0");
        ds->Set<std::string>(milvus::index::UPPER_BOUND_VALUE, "range");
        ds->Set<bool>(milvus::index::LOWER_BOUND_INCLUSIVE, true);
        ds->Set<bool>(milvus::index::UPPER_BOUND_INCLUSIVE, true);
        auto bitset = index->Query(ds);
        ASSERT_TRUE(Any(bitset));
    }

    {
        for (size_t i = 0; i < strs.size(); i++) {
            auto ds = std::make_shared<knowhere::DataSet>();
            ds->Set<milvus::OpType>(milvus::index::OPERATOR_TYPE,
                                    milvus::OpType::PrefixMatch);
            ds->Set<std::string>(milvus::index::PREFIX_VALUE,
                                 std::move(strs[i]));
            auto bitset = index->Query(ds);
            ASSERT_EQ(bitset.size(), strs.size());
            ASSERT_TRUE(bitset[i]);
        }
    }
}

TEST_F(StringIndexMarisaTest, Codec) {
    auto index = milvus::index::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }

    index->Build(nb, strings.data());

    std::vector<std::string> invalid_strings = {std::to_string(nb)};
    auto copy_index = milvus::index::CreateStringIndexMarisa();

    {
        auto binary_set = index->Serialize(nullptr);
        copy_index->Load(binary_set);
    }

    {
        auto bitset = copy_index->In(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(Any(bitset));
    }

    {
        auto bitset = copy_index->In(1, invalid_strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->NotIn(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "9", true);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "90", false);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        for (size_t i = 0; i < nb; i++) {
            auto str = strings[i];
            auto bitset = copy_index->PrefixMatch(str);
            ASSERT_EQ(bitset.size(), nb);
            ASSERT_TRUE(bitset[i]);
        }
    }
}

TEST_F(StringIndexMarisaTest, BaseIndexCodec) {
    milvus::index::IndexBasePtr index =
        milvus::index::CreateStringIndexMarisa();
    std::vector<std::string> strings(nb);
    for (int i = 0; i < nb; ++i) {
        strings[i] = std::to_string(std::rand() % 10);
    }
    *str_arr.mutable_data() = {strings.begin(), strings.end()};
    std::vector<uint8_t> data(str_arr.ByteSizeLong(), 0);
    str_arr.SerializeToArray(data.data(), str_arr.ByteSizeLong());
    index->BuildWithRawData(str_arr.ByteSizeLong(), data.data());

    std::vector<std::string> invalid_strings = {std::to_string(nb)};
    auto copy_index = milvus::index::CreateStringIndexMarisa();

    {
        auto binary_set = index->Serialize(nullptr);
        copy_index->Load(binary_set);
    }

    {
        auto bitset = copy_index->In(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(Any(bitset));
    }

    {
        auto bitset = copy_index->In(1, invalid_strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->NotIn(nb, strings.data());
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_TRUE(BitSetNone(bitset));
    }

    {
        auto bitset = copy_index->Range("0", milvus::OpType::GreaterEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("90", milvus::OpType::LessThan);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("9", milvus::OpType::LessEqual);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "9", true);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        auto bitset = copy_index->Range("0", true, "90", false);
        ASSERT_EQ(bitset.size(), nb);
        ASSERT_EQ(Count(bitset), nb);
    }

    {
        for (size_t i = 0; i < nb; i++) {
            auto str = strings[i];
            auto bitset = copy_index->PrefixMatch(str);
            ASSERT_EQ(bitset.size(), nb);
            ASSERT_TRUE(bitset[i]);
        }
    }
}

using milvus::segcore::GeneratedData;
class StringIndexMarisaTestV2 : public StringIndexBaseTest {
    std::shared_ptr<arrow::Schema>
    TestSchema(int vec_size) {
        arrow::FieldVector fields;
        fields.push_back(arrow::field("pk", arrow::int64()));
        fields.push_back(arrow::field("ts", arrow::int64()));
        fields.push_back(arrow::field("scalar", arrow::utf8()));
        fields.push_back(
            arrow::field("vec", arrow::fixed_size_binary(vec_size)));
        return std::make_shared<arrow::Schema>(fields);
    }

    std::shared_ptr<arrow::RecordBatchReader>
    TestRecords(int vec_size,
                GeneratedData& dataset,
                std::vector<std::string>& scalars) {
        arrow::Int64Builder pk_builder;
        arrow::Int64Builder ts_builder;
        arrow::StringBuilder scalar_builder;
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
        auto schema = TestSchema(vec_size);
        auto rec_batch = arrow::RecordBatch::Make(
            schema, nb, {pk_array, ts_array, scalar_array, vec_array});
        auto reader =
            arrow::RecordBatchReader::Make({rec_batch}, schema).ValueOrDie();
        return reader;
    }

    std::shared_ptr<milvus_storage::Space>
    TestSpace(int vec_size,
              GeneratedData& dataset,
              std::vector<std::string>& scalars) {
        auto arrow_schema = TestSchema(vec_size);
        milvus_storage::SchemaOptions schema_options{.primary_column = "pk",
                                                     .version_column = "ts",
                                                     .vector_column = "vec"};
        auto schema = std::make_shared<milvus_storage::Schema>(arrow_schema,
                                                               schema_options);
        EXPECT_TRUE(schema->Validate().ok());

        auto space_res = milvus_storage::Space::Open(
            "file://" + boost::filesystem::canonical(temp_path).string(),
            milvus_storage::Options{schema});
        EXPECT_TRUE(space_res.has_value());

        auto space = std::move(space_res.value());
        auto rec = TestRecords(vec_size, dataset, scalars);
        auto write_opt = milvus_storage::WriteOption{nb};
        space->Write(*rec, write_opt);
        return std::move(space);
    }
    void
    SetUp() override {
        StringIndexBaseTest::SetUp();
        temp_path = boost::filesystem::temp_directory_path() /
                    boost::filesystem::unique_path();
        boost::filesystem::create_directory(temp_path);

        auto vec_size = DIM * 4;
        auto vec_field_data_type = milvus::DataType::VECTOR_FLOAT;
        auto dataset = ::GenDataset(nb, knowhere::metric::L2, false);

        space = TestSpace(vec_size, dataset, strs);
    }
    void
    TearDown() override {
        boost::filesystem::remove_all(temp_path);
    }

 protected:
    boost::filesystem::path temp_path;
    std::shared_ptr<milvus_storage::Space> space;
};

TEST_F(StringIndexMarisaTestV2, Base) {
    auto storage_config = get_default_local_storage_config();
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    milvus::storage::FileManagerContext file_manager_context(
        {}, {.field_name = "scalar"}, chunk_manager, space);
    auto index =
        milvus::index::CreateStringIndexMarisa(file_manager_context, space);
    index->BuildV2();
    index->UploadV2();

    auto new_index =
        milvus::index::CreateStringIndexMarisa(file_manager_context, space);
    new_index->LoadV2();
    ASSERT_EQ(strs.size(), index->Count());
}

}  // namespace index
}  // namespace milvus
