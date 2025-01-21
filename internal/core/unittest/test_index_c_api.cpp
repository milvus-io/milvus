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
#include <tuple>

#include "common/VectorTrait.h"
#include "common/type_c.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "pb/index_cgo_msg.pb.h"
#include "test_utils/indexbuilder_test_utils.h"

constexpr int NB = 10;

template <class TraitType>
void
TestVecIndex() {
    knowhere::IndexType index_type;
    knowhere::MetricType metric_type;
    if (std::is_same_v<TraitType, milvus::BinaryVector>) {
        index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
        metric_type = knowhere::metric::JACCARD;
    } else if (std::is_same_v<TraitType, milvus::SparseFloatVector>) {
        index_type = knowhere::IndexEnum::INDEX_SPARSE_INVERTED_INDEX;
        metric_type = knowhere::metric::IP;
    } else {
        index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
        metric_type = knowhere::metric::L2;
    }

    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) =
        generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok = google::protobuf::TextFormat::PrintToString(type_params,
                                                          &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params,
                                                     &index_params_str);
    assert(ok);
    auto dataset = GenFieldData(NB, metric_type, TraitType::data_type);

    CDataType dtype = TraitType::c_data_type;
    CIndex index;
    CStatus status;
    CBinarySet binary_set;
    CIndex copy_index;

    status = CreateIndexV0(
        dtype, type_params_str.c_str(), index_params_str.c_str(), &index);
    ASSERT_EQ(milvus::Success, status.error_code);

    if (std::is_same_v<TraitType, milvus::BinaryVector>) {
        auto xb_data = dataset.template get_col<uint8_t>(milvus::FieldId(100));
        status = BuildBinaryVecIndex(index, NB * DIM / 8, xb_data.data());
    } else if (std::is_same_v<TraitType, milvus::SparseFloatVector>) {
        auto xb_data =
            dataset.template get_col<knowhere::sparse::SparseRow<float>>(
                milvus::FieldId(100));
        status = BuildSparseFloatVecIndex(
            index,
            NB,
            kTestSparseDim,
            static_cast<const uint8_t*>(
                static_cast<const void*>(xb_data.data())));
    } else if (std::is_same_v<TraitType, milvus::FloatVector>) {
        auto xb_data = dataset.template get_col<float>(milvus::FieldId(100));
        status = BuildFloatVecIndex(index, NB * DIM, xb_data.data());
    } else if (std::is_same_v<TraitType, milvus::Float16Vector>) {
        auto xb_data = dataset.template get_col<uint8_t>(milvus::FieldId(100));
        status = BuildFloat16VecIndex(index, NB * DIM, xb_data.data());
    } else if (std::is_same_v<TraitType, milvus::BFloat16Vector>) {
        auto xb_data = dataset.template get_col<uint8_t>(milvus::FieldId(100));
        status = BuildBFloat16VecIndex(index, NB * DIM, xb_data.data());
    }
    ASSERT_EQ(milvus::Success, status.error_code);

    status = SerializeIndexToBinarySet(index, &binary_set);
    ASSERT_EQ(milvus::Success, status.error_code);

    status = CreateIndexV0(
        dtype, type_params_str.c_str(), index_params_str.c_str(), &copy_index);
    ASSERT_EQ(milvus::Success, status.error_code);

    status = LoadIndexFromBinarySet(copy_index, binary_set);
    ASSERT_EQ(milvus::Success, status.error_code);

    status = DeleteIndex(index);
    ASSERT_EQ(milvus::Success, status.error_code);

    status = DeleteIndex(copy_index);
    ASSERT_EQ(milvus::Success, status.error_code);

    DeleteBinarySet(binary_set);
}

TEST(VecIndex, All) {
    TestVecIndex<milvus::BinaryVector>();
    TestVecIndex<milvus::SparseFloatVector>();
    TestVecIndex<milvus::FloatVector>();
    TestVecIndex<milvus::Float16Vector>();
    TestVecIndex<milvus::BFloat16Vector>();
}

TEST(CBoolIndexTest, All) {
    schemapb::BoolArray half;
    knowhere::DataSetPtr half_ds;

    for (size_t i = 0; i < NB; i++) {
        *(half.mutable_data()->Add()) = (i % 2) == 0;
    }
    half_ds = GenDsFromPB(half);

    auto params = GenBoolParams();
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto type_params_str = generate_type_params(type_params);
        auto index_params_str = generate_index_params(index_params);

        CDataType dtype = Bool;
        CIndex index;
        CStatus status;
        CBinarySet binary_set;
        CIndex copy_index;

        {
            status = CreateIndexV0(dtype,
                                   type_params_str.c_str(),
                                   index_params_str.c_str(),
                                   &index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = BuildScalarIndex(
                index, half_ds->GetRows(), half_ds->GetTensor());
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = SerializeIndexToBinarySet(index, &binary_set);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = CreateIndexV0(dtype,
                                   type_params_str.c_str(),
                                   index_params_str.c_str(),
                                   &copy_index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = LoadIndexFromBinarySet(copy_index, binary_set);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = DeleteIndex(index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = DeleteIndex(copy_index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        { DeleteBinarySet(binary_set); }
    }

    delete[](char*)(half_ds->GetTensor());
}

// TODO: more scalar type.
TEST(CInt64IndexTest, All) {
    auto arr = GenSortedArr<int64_t>(NB);

    auto params = GenParams<int64_t>();
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto type_params_str = generate_type_params(type_params);
        auto index_params_str = generate_index_params(index_params);

        CDataType dtype = Int64;
        CIndex index;
        CStatus status;
        CBinarySet binary_set;
        CIndex copy_index;

        {
            status = CreateIndexV0(dtype,
                                   type_params_str.c_str(),
                                   index_params_str.c_str(),
                                   &index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = BuildScalarIndex(index, arr.size(), arr.data());
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = SerializeIndexToBinarySet(index, &binary_set);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = CreateIndexV0(dtype,
                                   type_params_str.c_str(),
                                   index_params_str.c_str(),
                                   &copy_index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = LoadIndexFromBinarySet(copy_index, binary_set);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = DeleteIndex(index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = DeleteIndex(copy_index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        { DeleteBinarySet(binary_set); }
    }
}

// disable this case since marisa not supported in mac
#ifdef __linux__
TEST(CStringIndexTest, All) {
    auto strs = GenStrArr(NB);
    schemapb::StringArray str_arr;
    *str_arr.mutable_data() = {strs.begin(), strs.end()};
    auto str_ds = GenDsFromPB(str_arr);

    auto params = GenStringParams();
    for (const auto& tp : params) {
        auto type_params = tp.first;
        auto index_params = tp.second;
        auto type_params_str = generate_type_params(type_params);
        auto index_params_str = generate_index_params(index_params);

        CDataType dtype = String;
        CIndex index;
        CStatus status;
        CBinarySet binary_set;
        CIndex copy_index;

        {
            status = CreateIndexV0(dtype,
                                   type_params_str.c_str(),
                                   index_params_str.c_str(),
                                   &index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = BuildScalarIndex(
                index, (str_ds->GetRows()), (str_ds->GetTensor()));
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = SerializeIndexToBinarySet(index, &binary_set);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = CreateIndexV0(dtype,
                                   type_params_str.c_str(),
                                   index_params_str.c_str(),
                                   &copy_index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = LoadIndexFromBinarySet(copy_index, binary_set);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = DeleteIndex(index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        {
            status = DeleteIndex(copy_index);
            ASSERT_EQ(milvus::Success, status.error_code);
        }
        { DeleteBinarySet(binary_set); }
    }

    delete[](char*)(str_ds->GetTensor());
}
#endif
