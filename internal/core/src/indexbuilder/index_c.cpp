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

#include <string>
#include "index/knowhere/knowhere/index/vector_index/adapter/VectorAdapter.h"
#include "indexbuilder/IndexWrapper.h"
#include "indexbuilder/index_c.h"

class CGODebugUtils {
 public:
    static int64_t
    Strlen(const char* str, int64_t size) {
        if (size == 0) {
            return size;
        } else {
            return strlen(str);
        }
    }
};

CStatus
CreateIndex(const char* serialized_type_params, const char* serialized_index_params, CIndex* res_index) {
    auto status = CStatus();
    try {
        //    std::cout << "strlen(serialized_type_params): " << CGODebugUtils::Strlen(serialized_type_params,
        //    type_params_size)
        //              << std::endl;
        //    std::cout << "type_params_size: " << type_params_size << std::endl;
        //    std::cout << "strlen(serialized_index_params): "
        //              << CGODebugUtils::Strlen(serialized_index_params, index_params_size) << std::endl;
        //    std::cout << "index_params_size: " << index_params_size << std::endl;
        auto index =
            std::make_unique<milvus::indexbuilder::IndexWrapper>(serialized_type_params, serialized_index_params);
        *res_index = index.release();
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::runtime_error& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    return status;
}

void
DeleteIndex(CIndex index) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    delete cIndex;
}

CStatus
BuildFloatVecIndexWithoutIds(CIndex index, int64_t float_value_num, const float* vectors) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = float_value_num / dim;
        auto ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        cIndex->BuildWithoutIds(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::runtime_error& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
BuildBinaryVecIndexWithoutIds(CIndex index, int64_t data_size, const uint8_t* vectors) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto dim = cIndex->dim();
        auto row_nums = (data_size * 8) / dim;
        auto ds = milvus::knowhere::GenDataset(row_nums, dim, vectors);
        cIndex->BuildWithoutIds(ds);
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::runtime_error& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    return status;
}

CStatus
SerializeToSlicedBuffer(CIndex index, int32_t* buffer_size, char** res_buffer) {
    auto status = CStatus();
    try {
        auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
        auto binary = cIndex->Serialize();
        *buffer_size = binary.size;
        *res_buffer = binary.data;
        status.error_code = Success;
        status.error_msg = "";
    } catch (std::runtime_error& e) {
        status.error_code = UnexpectedException;
        status.error_msg = strdup(e.what());
    }
    return status;
}

void
LoadFromSlicedBuffer(CIndex index, const char* serialized_sliced_blob_buffer, int32_t size) {
    auto cIndex = (milvus::indexbuilder::IndexWrapper*)index;
    cIndex->Load(serialized_sliced_blob_buffer, size);
}
