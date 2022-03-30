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

#include <google/protobuf/text_format.h>
#include <gtest/gtest.h>
#include <map>
#include <tuple>
#include <knowhere/index/vector_index/helpers/IndexParameter.h>
#include <knowhere/index/vector_index/adapter/VectorAdapter.h>
#include <knowhere/index/vector_index/ConfAdapterMgr.h>
#include <knowhere/archive/KnowhereConfig.h>
#include "pb/index_cgo_msg.pb.h"

#define private public

#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/IndexFactory.h"

TEST(Dummy, Aha) {
    std::cout << "aha" << std::endl;
}

constexpr int64_t nb = 100;
namespace indexcgo = milvus::proto::indexcgo;
namespace schemapb = milvus::proto::schema;
using knowhere::scalar::OperatorType;
using milvus::indexbuilder::MapParams;
using milvus::indexbuilder::ScalarIndexCreator;
using ScalarTestParams = std::pair<MapParams, MapParams>;

namespace {
template <typename T>
inline void
assert_in(const std::unique_ptr<ScalarIndexCreator<T>>& creator, const std::vector<T>& arr) {
    // hard to compare floating point value.
    if (std::is_floating_point_v<T>) {
        return;
    }

    auto bitset1 = creator->index_->In(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());
    auto test = std::make_unique<T>(arr[arr.size() - 1] + 1);
    auto bitset2 = creator->index_->In(1, test.get());
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->none());
}

template <typename T>
inline void
assert_not_in(const std::unique_ptr<ScalarIndexCreator<T>>& creator, const std::vector<T>& arr) {
    auto bitset1 = creator->index_->NotIn(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->none());
    auto test = std::make_unique<T>(arr[arr.size() - 1] + 1);
    auto bitset2 = creator->index_->NotIn(1, test.get());
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());
}

template <typename T>
inline void
assert_range(const std::unique_ptr<ScalarIndexCreator<T>>& creator, const std::vector<T>& arr) {
    auto test_min = arr[0];
    auto test_max = arr[arr.size() - 1];

    auto bitset1 = creator->index_->Range(test_min - 1, OperatorType::GT);
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());

    auto bitset2 = creator->index_->Range(test_min, OperatorType::GE);
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());

    auto bitset3 = creator->index_->Range(test_max + 1, OperatorType::LT);
    ASSERT_EQ(arr.size(), bitset3->size());
    ASSERT_TRUE(bitset3->any());

    auto bitset4 = creator->index_->Range(test_max, OperatorType::LE);
    ASSERT_EQ(arr.size(), bitset4->size());
    ASSERT_TRUE(bitset4->any());

    auto bitset5 = creator->index_->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5->size());
    ASSERT_TRUE(bitset5->any());
}

template <>
inline void
assert_in(const std::unique_ptr<ScalarIndexCreator<std::string>>& creator, const std::vector<std::string>& arr) {
    auto bitset1 = creator->index_->In(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->any());
}

template <>
inline void
assert_not_in(const std::unique_ptr<ScalarIndexCreator<std::string>>& creator, const std::vector<std::string>& arr) {
    auto bitset1 = creator->index_->NotIn(arr.size(), arr.data());
    ASSERT_EQ(arr.size(), bitset1->size());
    ASSERT_TRUE(bitset1->none());
}

template <>
inline void
assert_range(const std::unique_ptr<ScalarIndexCreator<std::string>>& creator, const std::vector<std::string>& arr) {
    auto test_min = arr[0];
    auto test_max = arr[arr.size() - 1];

    auto bitset2 = creator->index_->Range(test_min, OperatorType::GE);
    ASSERT_EQ(arr.size(), bitset2->size());
    ASSERT_TRUE(bitset2->any());

    auto bitset4 = creator->index_->Range(test_max, OperatorType::LE);
    ASSERT_EQ(arr.size(), bitset4->size());
    ASSERT_TRUE(bitset4->any());

    auto bitset5 = creator->index_->Range(test_min, true, test_max, true);
    ASSERT_EQ(arr.size(), bitset5->size());
    ASSERT_TRUE(bitset5->any());
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

// TODO: it's easy to overflow for int8_t. Design more reasonable ut.
using ArithmeticT = ::testing::Types<int8_t, int16_t, int32_t, int64_t, float, double>;

TYPED_TEST_CASE_P(TypedScalarIndexCreatorTest);

TYPED_TEST_P(TypedScalarIndexCreatorTest, Dummy) {
    using T = TypeParam;
    std::cout << typeid(T()).name() << std::endl;
    PrintMapParams(GenParams<T>());
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, Constructor) {
    using T = TypeParam;
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
    }
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, In) {
    using T = TypeParam;
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        auto arr = GenArr<T>(nb);
        build_index<T>(creator, arr);
        assert_in<T>(creator, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, NotIn) {
    using T = TypeParam;
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        auto arr = GenArr<T>(nb);
        build_index<T>(creator, arr);
        assert_not_in<T>(creator, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, Range) {
    using T = TypeParam;
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        auto arr = GenArr<T>(nb);
        build_index<T>(creator, arr);
        assert_range<T>(creator, arr);
    }
}

TYPED_TEST_P(TypedScalarIndexCreatorTest, Codec) {
    using T = TypeParam;
    for (const auto& tp : GenParams<T>()) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        auto arr = GenArr<T>(nb);
        const int64_t dim = 8;  // not important here
        auto dataset = knowhere::GenDataset(arr.size(), dim, arr.data());
        creator->Build(dataset);

        auto binary_set = creator->Serialize();
        auto copy_creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        copy_creator->Load(binary_set);
        assert_in<T>(copy_creator, arr);
        assert_not_in<T>(copy_creator, arr);
        assert_range<T>(copy_creator, arr);
    }
}

REGISTER_TYPED_TEST_CASE_P(TypedScalarIndexCreatorTest, Dummy, Constructor, In, NotIn, Range, Codec);

INSTANTIATE_TYPED_TEST_CASE_P(ArithmeticCheck, TypedScalarIndexCreatorTest, ArithmeticT);

class BoolIndexTest : public ::testing::Test {
 protected:
    void
    SetUp() override {
        n = 8;
        for (size_t i = 0; i < n; i++) {
            *(all_true.mutable_data()->Add()) = true;
            *(all_false.mutable_data()->Add()) = false;
            *(half.mutable_data()->Add()) = (i % 2) == 0;
        }

        all_true_ds = GenDsFromPB(all_true);
        all_false_ds = GenDsFromPB(all_false);
        half_ds = GenDsFromPB(half);

        GenTestParams();
    }

    void
    TearDown() override {
        delete[](char*)(all_true_ds->Get<const void*>(knowhere::meta::TENSOR));
        delete[](char*) all_false_ds->Get<const void*>(knowhere::meta::TENSOR);
        delete[](char*) half_ds->Get<const void*>(knowhere::meta::TENSOR);
    }

 private:
    void
    GenTestParams() {
        params = GenBoolParams();
    }

 protected:
    schemapb::BoolArray all_true;
    schemapb::BoolArray all_false;
    schemapb::BoolArray half;
    knowhere::DatasetPtr all_true_ds;
    knowhere::DatasetPtr all_false_ds;
    knowhere::DatasetPtr half_ds;
    size_t n;
    std::vector<ScalarTestParams> params;
};

TEST_F(BoolIndexTest, Constructor) {
    using T = bool;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
    }
}

TEST_F(BoolIndexTest, In) {
    using T = bool;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto true_test = std::make_unique<bool>(true);
        auto false_test = std::make_unique<bool>(false);

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(all_true_ds);

            auto bitset1 = creator->index_->In(1, true_test.get());
            ASSERT_TRUE(bitset1->any());

            auto bitset2 = creator->index_->In(1, false_test.get());
            ASSERT_TRUE(bitset2->none());
        }

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(all_false_ds);

            auto bitset1 = creator->index_->In(1, true_test.get());
            ASSERT_TRUE(bitset1->none());

            auto bitset2 = creator->index_->In(1, false_test.get());
            ASSERT_TRUE(bitset2->any());
        }

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(half_ds);

            auto bitset1 = creator->index_->In(1, true_test.get());
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(bitset1->test(i), (i % 2) == 0);
            }

            auto bitset2 = creator->index_->In(1, false_test.get());
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(bitset2->test(i), (i % 2) != 0);
            }
        }
    }
}

TEST_F(BoolIndexTest, NotIn) {
    using T = bool;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto true_test = std::make_unique<bool>(true);
        auto false_test = std::make_unique<bool>(false);

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(all_true_ds);

            auto bitset1 = creator->index_->NotIn(1, true_test.get());
            ASSERT_TRUE(bitset1->none());

            auto bitset2 = creator->index_->NotIn(1, false_test.get());
            ASSERT_TRUE(bitset2->any());
        }

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(all_false_ds);

            auto bitset1 = creator->index_->NotIn(1, true_test.get());
            ASSERT_TRUE(bitset1->any());

            auto bitset2 = creator->index_->NotIn(1, false_test.get());
            ASSERT_TRUE(bitset2->none());
        }

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(half_ds);

            auto bitset1 = creator->index_->NotIn(1, true_test.get());
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(bitset1->test(i), (i % 2) != 0);
            }

            auto bitset2 = creator->index_->NotIn(1, false_test.get());
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(bitset2->test(i), (i % 2) == 0);
            }
        }
    }
}

TEST_F(BoolIndexTest, Codec) {
    using T = bool;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto true_test = std::make_unique<bool>(true);
        auto false_test = std::make_unique<bool>(false);

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(all_true_ds);

            auto copy_creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                        serialized_index_params.c_str());
            copy_creator->Load(creator->Serialize());

            auto bitset1 = copy_creator->index_->NotIn(1, true_test.get());
            ASSERT_TRUE(bitset1->none());

            auto bitset2 = copy_creator->index_->NotIn(1, false_test.get());
            ASSERT_TRUE(bitset2->any());
        }

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(all_false_ds);

            auto copy_creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                        serialized_index_params.c_str());
            copy_creator->Load(creator->Serialize());

            auto bitset1 = copy_creator->index_->NotIn(1, true_test.get());
            ASSERT_TRUE(bitset1->any());

            auto bitset2 = copy_creator->index_->NotIn(1, false_test.get());
            ASSERT_TRUE(bitset2->none());
        }

        {
            auto creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                   serialized_index_params.c_str());

            creator->Build(half_ds);

            auto copy_creator = std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(),
                                                                        serialized_index_params.c_str());
            copy_creator->Load(creator->Serialize());

            auto bitset1 = copy_creator->index_->NotIn(1, true_test.get());
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(bitset1->test(i), (i % 2) != 0);
            }

            auto bitset2 = copy_creator->index_->NotIn(1, false_test.get());
            for (size_t i = 0; i < n; i++) {
                ASSERT_EQ(bitset2->test(i), (i % 2) == 0);
            }
        }
    }
}

class StringIndexTest : public ::testing::Test {
    void
    SetUp() override {
        size_t n = 10;
        strs = GenStrArr(n);
        *str_arr.mutable_data() = {strs.begin(), strs.end()};
        str_ds = GenDsFromPB(str_arr);

        GenTestParams();
    }

    void
    TearDown() override {
        delete[](char*)(str_ds->Get<const void*>(knowhere::meta::TENSOR));
    }

 private:
    void
    GenTestParams() {
        params = GenStringParams();
    }

 protected:
    std::vector<std::string> strs;
    schemapb::StringArray str_arr;
    knowhere::DatasetPtr str_ds;
    std::vector<ScalarTestParams> params;
};

TEST_F(StringIndexTest, Constructor) {
    using T = std::string;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);
        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
    }
}

TEST_F(StringIndexTest, In) {
    using T = std::string;
    for (const auto& tp : params) {
        PrintMapParam(tp);

        auto type_params = tp.first;
        auto index_params = tp.second;

        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        creator->Build(str_ds);
        assert_in<T>(creator, strs);
    }
}

TEST_F(StringIndexTest, NotIn) {
    using T = std::string;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        creator->Build(str_ds);
        assert_not_in<T>(creator, strs);
    }
}

TEST_F(StringIndexTest, Range) {
    using T = std::string;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        creator->Build(str_ds);
        assert_range<T>(creator, strs);
    }
}

TEST_F(StringIndexTest, Codec) {
    using T = std::string;
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto serialized_type_params = generate_type_params(type_params);
        auto serialized_index_params = generate_index_params(index_params);

        auto creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        creator->Build(str_ds);

        auto copy_creator =
            std::make_unique<ScalarIndexCreator<T>>(serialized_type_params.c_str(), serialized_index_params.c_str());
        auto binary_set = creator->Serialize();
        copy_creator->Load(binary_set);
        assert_in<std::string>(copy_creator, strs);
        assert_not_in<std::string>(copy_creator, strs);
        assert_range<std::string>(copy_creator, strs);
    }
}
