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

#include "common/EasyAssert.h"
#include "common/LoadInfo.h"
#include "segcore/load_field_data_c.h"
#include "segcore/Collection.h"
#include "segcore/SegcoreConfig.h"
#include "segcore/IndexConfigGenerator.h"

CStatus
NewLoadFieldDataInfo(CLoadFieldDataInfo* c_load_field_data_info) {
    try {
        auto load_field_data_info = std::make_unique<LoadFieldDataInfo>();
        *c_load_field_data_info = load_field_data_info.release();
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
DeleteLoadFieldDataInfo(CLoadFieldDataInfo c_load_field_data_info) {
    auto info = static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    delete info;
}

CStatus
AppendLoadFieldInfo(CLoadFieldDataInfo c_load_field_data_info,
                    int64_t field_id,
                    int64_t row_count) {
    try {
        auto load_field_data_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        auto iter = load_field_data_info->field_infos.find(field_id);
        if (iter != load_field_data_info->field_infos.end()) {
            throw milvus::SegcoreError(milvus::FieldAlreadyExist,
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
AppendLoadFieldDataPath(CLoadFieldDataInfo c_load_field_data_info,
                        int64_t field_id,
                        int64_t entries_num,
                        const char* c_file_path) {
    try {
        auto load_field_data_info =
            static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
        auto iter = load_field_data_info->field_infos.find(field_id);
        if (iter == load_field_data_info->field_infos.end()) {
            throw milvus::SegcoreError(milvus::FieldIDInvalid,
                                       "please append field info first");
        }
        std::string file_path(c_file_path);
        load_field_data_info->field_infos[field_id].insert_files.emplace_back(
            file_path);
        load_field_data_info->field_infos[field_id].entries_nums.emplace_back(
            entries_num);
        return milvus::SuccessCStatus();
    } catch (std::exception& e) {
        return milvus::FailureCStatus(&e);
    }
}

void
AppendMMapDirPath(CLoadFieldDataInfo c_load_field_data_info,
                  const char* c_dir_path) {
    auto load_field_data_info =
        static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    load_field_data_info->mmap_dir_path = std::string(c_dir_path);
}

void
SetUri(CLoadFieldDataInfo c_load_field_data_info, const char* uri) {
    auto load_field_data_info = (LoadFieldDataInfo*)c_load_field_data_info;
    load_field_data_info->url = std::string(uri);
}

void
SetStorageVersion(CLoadFieldDataInfo c_load_field_data_info,
                  int64_t storage_version) {
    auto load_field_data_info = (LoadFieldDataInfo*)c_load_field_data_info;
    load_field_data_info->storage_version = storage_version;
}

void
EnableMmap(CLoadFieldDataInfo c_load_field_data_info,
           int64_t field_id,
           bool enabled) {
    auto info = static_cast<LoadFieldDataInfo*>(c_load_field_data_info);
    info->field_infos[field_id].enable_mmap = enabled;
}

float
MemOfLoadFieldDataWithBinlogIndex(CCollection c_collection,
                                  int64_t field_id,
                                  uint64_t field_data_size,
                                  uint64_t field_data_size_num,
                                  float build_expand_rate) {
    auto col = static_cast<milvus::segcore::Collection*>(c_collection);
    auto index_meta = col->get_index_meta();
    if (index_meta == nullptr) {
        // index meta is null
        return field_data_size;
    }
    milvus::FieldId f_id = milvus::FieldId(field_id);
    if (!index_meta->HasFiled(f_id))
        return field_data_size;
    auto& field_index_meta = index_meta->GetFieldIndexMeta(f_id);
    auto vec_index_config = milvus::segcore::VecIndexConfig(
        field_data_size_num,
        field_index_meta,
        milvus::segcore::SegcoreConfig::default_config(),
        SegmentType::Sealed);
    auto field_meta = col->get_schema()->operator[](f_id);
    if (!vec_index_config.IsSupportedDataType(field_meta.get_data_type())) {
        // not a float_vector type
        return field_data_size;
    } else {
        return vec_index_config.EstimateBuildBinlogIndexMemoryInBytes(
            field_data_size, build_expand_rate);
    }
}