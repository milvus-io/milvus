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

#include "segcore/vector_index_c.h"

#include "common/Types.h"
#include "common/EasyAssert.h"
#include "knowhere/utils.h"
#include "knowhere/config.h"
#include "knowhere/version.h"
#include "index/Meta.h"
#include "index/IndexFactory.h"
#include "pb/index_cgo_msg.pb.h"

CStatus
ValidateIndexParams(const char* index_type,
                    enum CDataType data_type,
                    const uint8_t* serialized_index_params,
                    const uint64_t length) {
    try {
        auto index_params =
            std::make_unique<milvus::proto::indexcgo::IndexParams>();
        auto res =
            index_params->ParseFromArray(serialized_index_params, length);
        AssertInfo(res, "Unmarshal index params failed");

        knowhere::Json json;

        for (size_t i = 0; i < index_params->params_size(); i++) {
            auto& param = index_params->params(i);
            json[param.key()] = param.value();
        }

        milvus::DataType dataType(static_cast<milvus::DataType>(data_type));

        knowhere::Status status;
        std::string error_msg;
        if (dataType == milvus::DataType::VECTOR_BINARY) {
            status = knowhere::IndexStaticFaced<knowhere::bin1>::ConfigCheck(
                index_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                json,
                error_msg);
        } else if (dataType == milvus::DataType::VECTOR_FLOAT) {
            status = knowhere::IndexStaticFaced<knowhere::fp32>::ConfigCheck(
                index_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                json,
                error_msg);
        } else if (dataType == milvus::DataType::VECTOR_BFLOAT16) {
            status = knowhere::IndexStaticFaced<knowhere::bf16>::ConfigCheck(
                index_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                json,
                error_msg);
        } else if (dataType == milvus::DataType::VECTOR_FLOAT16) {
            status = knowhere::IndexStaticFaced<knowhere::fp16>::ConfigCheck(
                index_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                json,
                error_msg);
        } else if (dataType == milvus::DataType::VECTOR_SPARSE_FLOAT) {
            status = knowhere::IndexStaticFaced<knowhere::fp32>::ConfigCheck(
                index_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                json,
                error_msg);
        } else if (dataType == milvus::DataType::VECTOR_INT8) {
            status = knowhere::IndexStaticFaced<knowhere::int8>::ConfigCheck(
                index_type,
                knowhere::Version::GetCurrentVersion().VersionNumber(),
                json,
                error_msg);
        } else {
            status = knowhere::Status::invalid_args;
        }
        CStatus cStatus;
        if (status == knowhere::Status::success) {
            cStatus.error_code = milvus::Success;
            cStatus.error_msg = "";
        } else {
            cStatus.error_code = milvus::ConfigInvalid;
            cStatus.error_msg = strdup(error_msg.c_str());
        }
        return cStatus;
    } catch (std::exception& e) {
        auto cStatus = CStatus();
        cStatus.error_code = milvus::UnexpectedError;
        cStatus.error_msg = strdup(e.what());
        return cStatus;
    }
}

int
GetIndexListSize() {
    return knowhere::IndexFactory::Instance().GetIndexFeatures().size();
}

void
GetIndexFeatures(void* index_key_list, uint64_t* index_feature_list) {
    auto& features = knowhere::IndexFactory::Instance().GetIndexFeatures();
    int idx = 0;

    const char** index_keys = (const char**)index_key_list;
    uint64_t* index_features = (uint64_t*)index_feature_list;
    for (auto it = features.begin(); it != features.end(); ++it) {
        index_keys[idx] = it->first.c_str();
        index_features[idx] = it->second;
        idx++;
    }
}
