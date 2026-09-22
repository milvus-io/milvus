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
#include <stdlib.h>

#include <cstdint>
#include <memory>
#include <string>

#include "common/Consts.h"
#include "common/Types.h"
#include "indexbuilder/index_c.h"
#include "pb/index_cgo_msg.pb.h"
#include "test_utils/Constants.h"

TEST(CreateIndexTest, StorageV2) {
    GTEST_SKIP() << "TODO: after index/stats task level fs is finished, should "
                    "fix shutdown sdk api in test";
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
    storage_config->set_root_path(TestLocalPath);
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
    storage_config->set_root_path(TestLocalPath);

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
