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
#include "common/Consts.h"
#include "common/Types.h"

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
    } else if (std::is_same_v<TraitType, milvus::Int8Vector>) {
        auto xb_data = dataset.template get_col<int8_t>(milvus::FieldId(100));
        status = BuildInt8VecIndex(index, NB * DIM, xb_data.data());
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
    TestVecIndex<milvus::Int8Vector>();
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

TEST(CreateIndexTest, StorageV2) {
    auto build_index_info =
        std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();

    auto* index_param = build_index_info->add_index_params();
    index_param->set_key("index_type");
    index_param->set_value("IVF_FLAT");

    auto* metric_param = build_index_info->add_index_params();
    metric_param->set_key("metric_type");
    metric_param->set_value("L2");

    auto* type_param = build_index_info->add_type_params();
    type_param->set_key("dim");
    type_param->set_value("128");

    build_index_info->add_insert_files("test_insert_files");

    auto* opt_field = build_index_info->add_opt_fields();
    opt_field->set_fieldid(100);
    opt_field->set_field_name("test_field");
    opt_field->set_field_type(static_cast<int32_t>(milvus::DataType::INT64));
    opt_field->add_data_paths("test_path");

    build_index_info->set_lack_binlog_rows(100);
    build_index_info->set_partition_key_isolation(true);
    build_index_info->set_storage_version(STORAGE_V2);
    build_index_info->set_dim(128);

    auto* field_schema = build_index_info->mutable_field_schema();
    field_schema->set_data_type(milvus::proto::schema::DataType::Int64);

    auto* segment_insert_files =
        build_index_info->mutable_segment_insert_files();
    auto* field_insert_files = segment_insert_files->add_field_insert_files();
    field_insert_files->add_file_paths("test_segment_file_path");

    auto* storage_config = build_index_info->mutable_storage_config();
    storage_config->set_storage_type("remote");
    storage_config->set_root_path("/tmp/test_storage");
    storage_config->set_address("localhost:9000");
    storage_config->set_bucket_name("test_bucket");
    storage_config->set_access_keyid("test_access_key");
    storage_config->set_secret_access_key("test_secret_key");
    storage_config->set_cloud_provider("aws");
    storage_config->set_iamendpoint("test_iam_endpoint");
    storage_config->set_usessl(false);
    storage_config->set_sslcacert("test_ca_cert");
    storage_config->set_useiam(false);
    storage_config->set_region("test_region");
    storage_config->set_use_virtual_host(false);
    storage_config->set_request_timeout_ms(1000);
    storage_config->set_gcpcredentialjson("test_credential_json");

    std::string serialized_info;
    build_index_info->SerializeToString(&serialized_info);

    CIndex index;
    CStatus status =
        CreateIndex(&index,
                    reinterpret_cast<const uint8_t*>(serialized_info.data()),
                    serialized_info.size());

    ASSERT_EQ(status.error_code, milvus::S3Error);
    free(const_cast<char*>(static_cast<const char*>(status.error_msg)));
}

TEST(VectorMemIndexTest, StorageV2) {
    auto build_index_info =
        std::make_unique<milvus::proto::indexcgo::BuildIndexInfo>();

    auto* index_param = build_index_info->add_index_params();
    index_param->set_key("index_type");
    index_param->set_value("IVF_FLAT");

    auto* metric_param = build_index_info->add_index_params();
    metric_param->set_key("metric_type");
    metric_param->set_value("L2");

    auto* type_param = build_index_info->add_type_params();
    type_param->set_key("dim");
    type_param->set_value("128");

    auto* opt_field = build_index_info->add_opt_fields();
    opt_field->set_fieldid(100);
    opt_field->set_field_name("test_field");
    opt_field->set_field_type(
        static_cast<int32_t>(milvus::DataType::VECTOR_FLOAT));
    opt_field->add_data_paths("test_path");

    build_index_info->set_lack_binlog_rows(100);
    build_index_info->set_partition_key_isolation(true);
    build_index_info->set_storage_version(STORAGE_V2);
    build_index_info->set_dim(128);

    auto* field_schema = build_index_info->mutable_field_schema();
    field_schema->set_data_type(milvus::proto::schema::DataType::FloatVector);

    auto* segment_insert_files =
        build_index_info->mutable_segment_insert_files();
    auto* field_insert_files = segment_insert_files->add_field_insert_files();
    field_insert_files->add_file_paths("test_segment_file_path");

    auto* storage_config = build_index_info->mutable_storage_config();
    storage_config->set_storage_type("local");
    storage_config->set_root_path("/tmp");

    std::string serialized_info;
    build_index_info->SerializeToString(&serialized_info);

    CIndex index;
    CStatus status =
        CreateIndex(&index,
                    reinterpret_cast<const uint8_t*>(serialized_info.data()),
                    serialized_info.size());

    ASSERT_EQ(status.error_code, milvus::UnexpectedError);
    free(const_cast<char*>(static_cast<const char*>(status.error_msg)));
}
