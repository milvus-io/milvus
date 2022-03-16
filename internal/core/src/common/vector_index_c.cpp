// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "knowhere/common/BinarySet.h"
#include "common/vector_index_c.h"

CStatus
NewBinarySet(CBinarySet* c_binary_set) {
    try {
        auto binary_set = std::make_unique<knowhere::BinarySet>();
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
    auto binary_set = (knowhere::BinarySet*)c_binary_set;
    delete binary_set;
}

CStatus
AppendIndexBinary(CBinarySet c_binary_set, void* index_binary, int64_t index_size, const char* c_index_key) {
    auto status = CStatus();
    try {
        auto binary_set = (knowhere::BinarySet*)c_binary_set;
        std::string index_key(c_index_key);
        uint8_t* index = (uint8_t*)index_binary;
        std::shared_ptr<uint8_t[]> data(index, [](void*) {});
        binary_set->Append(index_key, data, index_size);

        status.error_code = Success;
        status.error_msg = "";
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}

int
GetBinarySetSize(CBinarySet c_binary_set) {
    auto binary_set = (knowhere::BinarySet*)c_binary_set;
    return binary_set->binary_map_.size();
}

void
GetBinarySetKeys(CBinarySet c_binary_set, void* datas) {
    auto binary_set = (knowhere::BinarySet*)c_binary_set;
    auto& map_ = binary_set->binary_map_;
    const char** datas_ = (const char**)datas;
    std::size_t i = 0;
    for (auto it = map_.begin(); it != map_.end(); ++it, i++) {
        datas_[i] = it->first.c_str();
    }
}

int
GetBinarySetValueSize(CBinarySet c_binary_set, const char* key) {
    auto binary_set = (knowhere::BinarySet*)c_binary_set;
    int64_t ret_ = 0;
    try {
        std::string key_(key);
        auto binary = binary_set->GetByName(key_);
        ret_ = binary->size;
    } catch (std::exception& e) {
    }
    return ret_;
}

CStatus
CopyBinarySetValue(void* data, const char* key, CBinarySet c_binary_set) {
    auto status = CStatus();
    auto binary_set = (knowhere::BinarySet*)c_binary_set;
    try {
        auto binary = binary_set->GetByName(key);
        status.error_code = Success;
        status.error_msg = "";
        memcpy((uint8_t*)data, binary->data.get(), binary->size);
    } catch (std::exception& e) {
        status.error_code = UnexpectedError;
        status.error_msg = strdup(e.what());
    }
    return status;
}
