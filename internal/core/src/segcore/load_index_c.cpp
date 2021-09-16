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

#include "index/knowhere/knowhere/common/BinarySet.h"
#include "index/knowhere/knowhere/index/vector_index/VecIndexFactory.h"
#include "segcore/load_index_c.h"
#include "common/LoadInfo.h"
#include "exceptions/EasyAssert.h"

CStatus
NewLoadIndexInfo(CLoadIndexInfo* c_load_index_info) {
    try {
        auto load_index_info = std::make_unique<LoadIndexInfo>();
        *c_load_index_info = load_index_info.release();
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

void
DeleteLoadIndexInfo(CLoadIndexInfo c_load_index_info) {
    auto info = (LoadIndexInfo*)c_load_index_info;
    delete info;
}

CStatus
AppendIndexParam(CLoadIndexInfo c_load_index_info, const char* c_index_key, const char* c_index_value) {
    try {
        auto load_index_info = (LoadIndexInfo*)c_load_index_info;
        std::string index_key(c_index_key);
        std::string index_value(c_index_value);
        load_index_info->index_params[index_key] = index_value;

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
AppendFieldInfo(CLoadIndexInfo c_load_index_info, int64_t field_id) {
    try {
        auto load_index_info = (LoadIndexInfo*)c_load_index_info;
        load_index_info->field_id = field_id;

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
AppendIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    try {
        auto load_index_info = (LoadIndexInfo*)c_load_index_info;
        auto binary_set = (milvus::knowhere::BinarySet*)c_binary_set;
        auto& index_params = load_index_info->index_params;
        bool find_index_type = index_params.count("index_type") > 0 ? true : false;
        bool find_index_mode = index_params.count("index_mode") > 0 ? true : false;
        AssertInfo(find_index_type == true, "Can't find index type in index_params");
        milvus::knowhere::IndexMode mode;
        if (find_index_mode) {
            mode = index_params["index_mode"] == "CPU" ? milvus::knowhere::IndexMode::MODE_CPU
                                                       : milvus::knowhere::IndexMode::MODE_GPU;
        } else {
            mode = milvus::knowhere::IndexMode::MODE_CPU;
        }
        load_index_info->index =
            milvus::knowhere::VecIndexFactory::GetInstance().CreateVecIndex(index_params["index_type"], mode);
        load_index_info->index->Load(*binary_set);
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

CStatus
NewBinarySet(CBinarySet* c_binary_set) {
    try {
        auto binary_set = std::make_unique<milvus::knowhere::BinarySet>();
        *c_binary_set = binary_set.release();
        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}

void
DeleteBinarySet(CBinarySet c_binary_set) {
    auto binary_set = (milvus::knowhere::BinarySet*)c_binary_set;
    delete binary_set;
}

CStatus
AppendBinaryIndex(CBinarySet c_binary_set, void* index_binary, int64_t index_size, const char* c_index_key) {
    try {
        auto binary_set = (milvus::knowhere::BinarySet*)c_binary_set;
        std::string index_key(c_index_key);
        uint8_t* index = (uint8_t*)index_binary;
        std::shared_ptr<uint8_t[]> data(index, [](void*) {});
        binary_set->Append(index_key, data, index_size);

        auto status = CStatus();
        status.error_code = Success;
        status.error_msg = "";
        return status;
    } catch (std::exception& e) {
        auto status = CStatus();
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
        return status;
    }
}
