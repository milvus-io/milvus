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

#include "indexbuilder/index_c.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"

#define private public
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/IndexFactory.h"

TEST(Dummy, Aha) {
    std::cout << "aha" << std::endl;
}

constexpr int64_t nb = 100;
namespace indexcgo = milvus::proto::indexcgo;
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

    delete[](char*)(ds->GetTensor());
}

template <>
inline void
build_index(const ScalarIndexCreatorPtr& creator,
            const std::vector<std::string>& arr) {
    schemapb::StringArray pbarr;
    *(pbarr.mutable_data()) = {arr.begin(), arr.end()};
    auto ds = GenDsFromPB(pbarr);

    creator->Build(ds);

    delete[](char*)(ds->GetTensor());
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

TYPED_TEST_CASE_P(TypedScalarIndexCreatorTest);

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
            milvus::DataType(dtype), config, nullptr);
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
            milvus::DataType(dtype), config, nullptr);
        auto arr = GenArr<T>(nb);
        build_index<T>(creator, arr);
        auto binary_set = creator->Serialize();
        auto copy_creator = milvus::indexbuilder::CreateScalarIndex(
            milvus::DataType(dtype), config, nullptr);
        copy_creator->Load(binary_set);
    }
}

REGISTER_TYPED_TEST_CASE_P(TypedScalarIndexCreatorTest,
                           Dummy,
                           Constructor,
                           Codec);

INSTANTIATE_TYPED_TEST_CASE_P(ArithmeticCheck,
                              TypedScalarIndexCreatorTest,
                              ScalarT);
