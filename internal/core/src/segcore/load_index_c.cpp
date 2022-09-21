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

#include "common/CDataType.h"
#include "common/FieldMeta.h"
#include "common/Utils.h"
#include "index/Meta.h"
#include "index/Utils.h"
#include "index/IndexFactory.h"
#include "storage/Util.h"
#include "segcore/load_index_c.h"
#include "pb/index_cgo_msg.pb.h"

CStatus
NewLoadIndexInfo(CLoadIndexInfo* c_load_index_info) {
    try {
        auto load_index_info = std::make_unique<milvus::index::LoadIndexInfo>();
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
    auto info = (milvus::index::LoadIndexInfo*)c_load_index_info;
    delete info;
}

CStatus
AppendIndexParam(CLoadIndexInfo c_load_index_info, const char* c_index_key, const char* c_index_value) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
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
AppendFieldInfo(CLoadIndexInfo c_load_index_info,
                int64_t collection_id,
                int64_t partition_id,
                int64_t segment_id,
                int64_t field_id,
                enum CDataType field_type) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
        load_index_info->collection_id = collection_id;
        load_index_info->partition_id = partition_id;
        load_index_info->segment_id = segment_id;
        load_index_info->field_id = field_id;
        load_index_info->field_type = milvus::DataType(field_type);

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
appendVecIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
        auto binary_set = (knowhere::BinarySet*)c_binary_set;
        auto& index_params = load_index_info->index_params;

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = load_index_info->field_type;

        // get index type
        AssertInfo(index_params.find("index_type") != index_params.end(), "index type is empty");
        index_info.index_type = index_params.at("index_type");

        // get metric type
        AssertInfo(index_params.find("metric_type") != index_params.end(), "metric type is empty");
        index_info.metric_type = index_params.at("metric_type");

        // set default index mode
        index_info.index_mode = milvus::IndexMode::MODE_CPU;
        if (index_params.count("index_mode")) {
            index_info.index_mode = milvus::index::GetIndexMode(index_params["index_mode"]);
        }

        // init file manager
        milvus::storage::FieldDataMeta field_meta{load_index_info->collection_id, load_index_info->partition_id,
                                                  load_index_info->segment_id, load_index_info->field_id};
        milvus::storage::IndexMeta index_meta{load_index_info->segment_id, load_index_info->field_id,
                                              load_index_info->index_build_id, load_index_info->index_version};
        auto file_manager = milvus::storage::CreateFileManager(index_info.index_type, field_meta, index_meta);

        auto config = milvus::index::ParseConfigFromIndexParams(load_index_info->index_params);
        config["index_files"] = load_index_info->index_files;

        load_index_info->index = milvus::index::IndexFactory::GetInstance().CreateIndex(index_info, file_manager);
        load_index_info->index->Load(*binary_set, config);
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
appendScalarIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
        auto field_type = load_index_info->field_type;
        auto binary_set = (knowhere::BinarySet*)c_binary_set;
        auto& index_params = load_index_info->index_params;
        bool find_index_type = index_params.count("index_type") > 0 ? true : false;
        AssertInfo(find_index_type == true, "Can't find index type in index_params");

        milvus::index::CreateIndexInfo index_info;
        index_info.field_type = milvus::DataType(field_type);
        index_info.index_type = index_params["index_type"];
        // set default index mode
        index_info.index_mode = milvus::IndexMode::MODE_CPU;
        if (index_params.count("index_mode")) {
            index_info.index_mode = milvus::index::GetIndexMode(index_params["index_mode"]);
        }

        load_index_info->index = milvus::index::IndexFactory::GetInstance().CreateIndex(index_info, nullptr);
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
AppendIndex(CLoadIndexInfo c_load_index_info, CBinarySet c_binary_set) {
    auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
    auto field_type = load_index_info->field_type;
    if (milvus::datatype_is_vector(field_type)) {
        return appendVecIndex(c_load_index_info, c_binary_set);
    }
    return appendScalarIndex(c_load_index_info, c_binary_set);
}

CStatus
AppendIndexFilePath(CLoadIndexInfo c_load_index_info, const char* c_file_path) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
        std::string index_file_path(c_file_path);
        load_index_info->index_files.emplace_back(index_file_path);

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
AppendIndexInfo(
    CLoadIndexInfo c_load_index_info, int64_t index_id, int64_t build_id, int64_t version, const char* c_index_params) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
        load_index_info->index_id = index_id;
        load_index_info->index_build_id = build_id;
        load_index_info->index_version = version;
        milvus::proto::indexcgo::IndexParams index_params;
        milvus::index::ParseFromString(index_params, c_index_params);

        for (auto i = 0; i < index_params.params().size(); i++) {
            auto& param = index_params.params(i);
            load_index_info->index_params[param.key()] = param.value();
        }

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
CleanLoadedIndex(CLoadIndexInfo c_load_index_info) {
    try {
        auto load_index_info = (milvus::index::LoadIndexInfo*)c_load_index_info;
        auto index_file_path_prefix =
            milvus::storage::GenLocalIndexPathPrefix(load_index_info->index_build_id, load_index_info->index_version);
#ifdef BUILD_DISK_ANN
        milvus::storage::LocalChunkManager::GetInstance().RemoveDir(index_file_path_prefix);
#endif
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
