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

#include "indexbuilder/VecIndexCreator.h"
#include "indexbuilder/index_c.h"
#include "indexbuilder/utils.h"
#include "test_utils/DataGen.h"
#include "test_utils/indexbuilder_test_utils.h"
#include "indexbuilder/ScalarIndexCreator.h"
#include "indexbuilder/IndexFactory.h"
#include "common/type_c.h"

constexpr int NB = 10;

TEST(FloatVecIndex, All) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_IVFPQ;
    auto metric_type = knowhere::metric::L2;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(NB, metric_type, false);
    auto xb_data = dataset.get_col<float>(milvus::FieldId(100));

    CDataType dtype = FloatVector;
    CIndex index;
    CStatus status;
    CBinarySet binary_set;
    CIndex copy_index;

    {
        status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &index);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = BuildFloatVecIndex(index, NB * DIM, xb_data.data());
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = SerializeIndexToBinarySet(index, &binary_set);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &copy_index);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = LoadIndexFromBinarySet(copy_index, binary_set);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = DeleteIndex(index);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = DeleteIndex(copy_index);
        ASSERT_EQ(Success, status.error_code);
    }
}

TEST(BinaryVecIndex, All) {
    auto index_type = knowhere::IndexEnum::INDEX_FAISS_BIN_IVFFLAT;
    auto metric_type = knowhere::metric::JACCARD;
    indexcgo::TypeParams type_params;
    indexcgo::IndexParams index_params;
    std::tie(type_params, index_params) = generate_params(index_type, metric_type);
    std::string type_params_str, index_params_str;
    bool ok;
    ok = google::protobuf::TextFormat::PrintToString(type_params, &type_params_str);
    assert(ok);
    ok = google::protobuf::TextFormat::PrintToString(index_params, &index_params_str);
    assert(ok);
    auto dataset = GenDataset(NB, metric_type, true);
    auto xb_data = dataset.get_col<uint8_t>(milvus::FieldId(100));

    CDataType dtype = BinaryVector;
    CIndex index;
    CStatus status;
    CBinarySet binary_set;
    CIndex copy_index;

    {
        status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &index);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = BuildBinaryVecIndex(index, NB * DIM / 8, xb_data.data());
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = SerializeIndexToBinarySet(index, &binary_set);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &copy_index);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = LoadIndexFromBinarySet(copy_index, binary_set);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = DeleteIndex(index);
        ASSERT_EQ(Success, status.error_code);
    }
    {
        status = DeleteIndex(copy_index);
        ASSERT_EQ(Success, status.error_code);
    }
}

TEST(CBoolIndexTest, All) {
    schemapb::BoolArray half;
    knowhere::DatasetPtr half_ds;

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
            status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = BuildScalarIndex(index, knowhere::GetDatasetRows(half_ds), knowhere::GetDatasetTensor(half_ds));
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = SerializeIndexToBinarySet(index, &binary_set);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &copy_index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = LoadIndexFromBinarySet(copy_index, binary_set);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = DeleteIndex(index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = DeleteIndex(copy_index);
            ASSERT_EQ(Success, status.error_code);
        }
    }

    delete[](char*) knowhere::GetDatasetTensor(half_ds);
}

// TODO: more scalar type.
TEST(CInt64IndexTest, All) {
    auto arr = GenArr<int64_t>(NB);

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
            status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = BuildScalarIndex(index, arr.size(), arr.data());
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = SerializeIndexToBinarySet(index, &binary_set);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &copy_index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = LoadIndexFromBinarySet(copy_index, binary_set);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = DeleteIndex(index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = DeleteIndex(copy_index);
            ASSERT_EQ(Success, status.error_code);
        }
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
            status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = BuildScalarIndex(index, knowhere::GetDatasetRows(str_ds), knowhere::GetDatasetTensor(str_ds));
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = SerializeIndexToBinarySet(index, &binary_set);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = CreateIndex(dtype, type_params_str.c_str(), index_params_str.c_str(), &copy_index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = LoadIndexFromBinarySet(copy_index, binary_set);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = DeleteIndex(index);
            ASSERT_EQ(Success, status.error_code);
        }
        {
            status = DeleteIndex(copy_index);
            ASSERT_EQ(Success, status.error_code);
        }
    }

    delete[](char*) knowhere::GetDatasetTensor(str_ds);
}
#endif
