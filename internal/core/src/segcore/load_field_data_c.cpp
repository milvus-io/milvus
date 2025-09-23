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

#include "log/Log.h"
#include "common/EasyAssert.h"
#include "common/LoadInfo.h"
#include "segcore/load_field_data_c.h"
#include "monitor/scope_metric.h"

CStatus
NewLoadFieldDataInfo(CLoadFieldDataInfo* c_load_field_data_info,
                     int64_t storage_version) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_field_data_info = std::make_unique<LoadFieldDataInfo>();
        load_field_data_info->storage_version = storage_version;
        *c_load_field_data_info = load_field_data_info.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
DeleteLoadFieldDataInfo(CLoadFieldDataInfo c_load_field_data_info) {
    SCOPE_CGO_CALL_METRIC();

    auto info = static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    delete info;
}

CStatus
AppendLoadFieldInfo(CLoadFieldDataInfo c_load_field_data_info,
                    int64_t field_id,
                    int64_t row_count) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_field_data_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        auto iter = load_field_data_info->field_infos.find(field_id);
        if (iter != load_field_data_info->field_infos.end()) {
            ThrowInfo(milvus::ErrorCode::FieldAlreadyExist,
                      "append same field info multi times");
        }
        FieldBinlogInfo binlog_info;
        binlog_info.field_id = field_id;
        binlog_info.row_count = row_count;
        load_field_data_info->field_infos[field_id] = binlog_info;
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
SetLoadFieldInfoChildFields(CLoadFieldDataInfo c_load_field_data_info,
                            int64_t field_id,
                            const int64_t* child_field_ids,
                            const int64_t child_field_num) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_field_data_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        auto iter = load_field_data_info->field_infos.find(field_id);
        if (iter == load_field_data_info->field_infos.end()) {
            ThrowInfo(milvus::ErrorCode::FieldIDInvalid,
                      "please append field info first");
        }
        load_field_data_info->field_infos[field_id].child_field_ids =
            std::vector<int64_t>(child_field_ids,
                                 child_field_ids + child_field_num);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

CStatus
AppendLoadFieldDataPath(CLoadFieldDataInfo c_load_field_data_info,
                        int64_t field_id,
                        int64_t entries_num,
                        int64_t memory_size,
                        const char* c_file_path) {
    SCOPE_CGO_CALL_METRIC();

    try {
        auto load_field_data_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        auto iter = load_field_data_info->field_infos.find(field_id);
        if (iter == load_field_data_info->field_infos.end()) {
            ThrowInfo(milvus::ErrorCode::FieldIDInvalid,
                      "please append field info first");
        }
        std::string file_path(c_file_path);
        load_field_data_info->field_infos[field_id].insert_files.emplace_back(
            file_path);
        load_field_data_info->field_infos[field_id].entries_nums.emplace_back(
            entries_num);
        load_field_data_info->field_infos[field_id].memory_sizes.emplace_back(
            memory_size);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
AppendWarmupPolicy(CLoadFieldDataInfo c_load_field_data_info,
                   CacheWarmupPolicy warmup_policy) {
    auto load_field_data_info =
        static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    load_field_data_info->warmup_policy = warmup_policy;
}

void
SetStorageVersion(CLoadFieldDataInfo c_load_field_data_info,
                  int64_t storage_version) {
    SCOPE_CGO_CALL_METRIC();

    auto load_field_data_info = (LoadFieldDataInfo*)c_load_field_data_info;
    load_field_data_info->storage_version = storage_version;
}

void
EnableMmap(CLoadFieldDataInfo c_load_field_data_info,
           int64_t field_id,
           bool enabled) {
    SCOPE_CGO_CALL_METRIC();

    auto info = static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    info->field_infos[field_id].enable_mmap = enabled;
}

void
SetLoadPriority(CLoadFieldDataInfo c_load_field_data_info, int32_t priority) {
    SCOPE_CGO_CALL_METRIC();

    auto info = static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    info->load_priority = milvus::proto::common::LoadPriority(priority);
}
