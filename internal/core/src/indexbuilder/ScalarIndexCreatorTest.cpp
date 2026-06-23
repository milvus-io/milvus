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
#include <knowhere/comp/index_param.h>

#include "common/CDataType.h"
#include "common/Consts.h"
#include "index/Utils.h"
#include "storage/Util.h"
#include "indexbuilder/IndexFactory.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "test_utils/storage_test_utils.h"

#define private public
#include "indexbuilder/ScalarIndexCreator.h"

constexpr int64_t nb = 100;
namespace schemapb = milvus::proto::schema;
using milvus::indexbuilder::ScalarIndexCreatorPtr;
using ScalarTestParams = std::pair<MapParams, MapParams>;

namespace {
template <typename T,
          typename = std::enable_if_t<std::is_arithmetic_v<T> |
                                      std::is_same_v<T, std::string>>>
inline void
build_index(const ScalarIndexCreatorPtr& creator, const std::vector<T>& arr) {
    const int64_t dim = 8;  // not important here
    auto dataset = knowhere::GenDataSet(arr.size(), dim, arr.data());
    creator->Build(dataset);
}

template <>
inline void
build_index(const ScalarIndexCreatorPtr& creator,
            const std::vector<bool>& arr) {
    schemapb::BoolArray pbarr;
    for (auto b : arr) {
        pbarr.add_data(b);
    }
    auto ds = GenDsFromPB(pbarr);

    creator->Build(ds);

    delete[] (char*)(ds->GetTensor());
}

template <>
inline void
build_index(const ScalarIndexCreatorPtr& creator,
            const std::vector<std::string>& arr) {
    schemapb::StringArray pbarr;
    *(pbarr.mutable_data()) = {arr.begin(), arr.end()};
    auto ds = GenDsFromPB(pbarr);

    creator->Build(ds);

    delete[] (char*)(ds->GetTensor());
}

}  // namespace

template <typename T>
class TypedScalarIndexCreatorTest : public ::testing::Test {
 protected:
    // void
    // SetUp() override {
    // }

    // void
    // TearDown() override {
    // }
};

using ScalarT = ::testing::
    Types<bool, int8_t, int16_t, int32_t, int64_t, float, double, std::string>;

TYPED_TEST_SUITE_P(TypedScalarIndexCreatorTest);

TYPED_TEST_P(TypedScalarIndexCreatorTest, Dummy) {
    using T = TypeParam;
    std::cout << typeid(T()).name() << std::endl;
    PrintMapParams(GenParams<T>());
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, Constructor) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;

        milvus::Config config;
        for (auto iter = index_params.begin(); iter != index_params.end();
             ++iter) {
            config[iter->first] = iter->second;
        }
        for (auto iter = type_params.begin(); iter != type_params.end();
             ++iter) {
            config[iter->first] = iter->second;
        }

        auto creator = milvus::indexbuilder::CreateScalarIndex(
            milvus::DataType(dtype),
            config,
            milvus::storage::FileManagerContext());
    }
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, Codec) {
    using T = TypeParam;
    auto dtype = milvus::GetDType<T>();
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;

        milvus::Config config;
        for (auto iter = index_params.begin(); iter != index_params.end();
             ++iter) {
            config[iter->first] = iter->second;
        }
        for (auto iter = type_params.begin(); iter != type_params.end();
             ++iter) {
            config[iter->first] = iter->second;
        }
        auto creator = milvus::indexbuilder::CreateScalarIndex(
            milvus::DataType(dtype),
            config,
            milvus::storage::FileManagerContext());
        auto arr = GenSortedArr<T>(nb);
        build_index<T>(creator, arr);
        auto binary_set = creator->Serialize();
        auto copy_creator = milvus::indexbuilder::CreateScalarIndex(
            milvus::DataType(dtype),
            config,
            milvus::storage::FileManagerContext());
        copy_creator->Load(binary_set);
    }
}

REGISTER_TYPED_TEST_SUITE_P(TypedScalarIndexCreatorTest,
                            Dummy,
                            Constructor,
                            Codec);

INSTANTIATE_TYPED_TEST_SUITE_P(ArithmeticCheck,
                               TypedScalarIndexCreatorTest,
                               ScalarT);

TEST(ScalarIndexCreatorTest, CreateTextMatchIndexForTextField) {
    auto storage_config = get_default_local_storage_config();
    auto chunk_manager = milvus::storage::CreateChunkManager(storage_config);
    auto fs = milvus::storage::InitArrowFileSystem(storage_config);

    milvus::storage::FieldDataMeta field_meta{1, 2, 3, 101};
    field_meta.field_schema.set_data_type(
        milvus::proto::schema::DataType::Text);
    field_meta.field_schema.set_fieldid(101);
    field_meta.field_schema.set_name("text");
    field_meta.field_schema.add_type_params()->set_key("enable_analyzer");
    field_meta.field_schema.mutable_type_params(0)->set_value("true");
    field_meta.field_schema.add_type_params()->set_key("analyzer_params");
    field_meta.field_schema.mutable_type_params(1)->set_value(
        R"({"tokenizer":"standard"})");

    milvus::storage::IndexMeta index_meta{3, 101, 1000, 0};
    milvus::storage::FileManagerContext ctx(
        field_meta, index_meta, chunk_manager, fs);

    milvus::Config config;
    config[milvus::index::INDEX_TYPE] = milvus::index::INVERTED_INDEX_TYPE;
    config["is_text_match"] = "true";
    config[milvus::index::SCALAR_INDEX_ENGINE_VERSION] = 3;
    config[milvus::index::TANTIVY_INDEX_VERSION] =
        milvus::index::TANTIVY_INDEX_LATEST_VERSION;

    auto creator =
        milvus::indexbuilder::IndexFactory::GetInstance().CreateIndex(
            milvus::DataType::TEXT, config, ctx);
    ASSERT_NE(creator, nullptr);
}
