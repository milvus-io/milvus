// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#include "server/DBWrapper.h"

#include <omp.h>
#include <cmath>
#include <string>
#include <vector>

#include <faiss/utils/distances.h>

#include "config/ServerConfig.h"
#include "db/DBFactory.h"
#include "db/snapshot/OperationExecutor.h"
#include "utils/CommonUtil.h"
#include "utils/ConfigUtils.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

namespace milvus {
namespace server {

Status
DBWrapper::StartService() {
    Status s;

    // db config
    engine::DBOptions opt;
    opt.meta_.backend_uri_ = config.general.meta_uri();

    std::string path = config.storage.path();
    opt.meta_.path_ = path + "/db";

    opt.auto_flush_interval_ = config.storage.auto_flush_interval();
    opt.file_cleanup_timeout_ = config.storage.file_cleanup_timeout();
    opt.metric_enable_ = config.metric.enable();
    opt.insert_cache_immediately_ = config.cache.cache_insert_data();
    opt.insert_buffer_size_ = config.cache.insert_buffer_size();

    if (not config.cluster.enable()) {
        opt.mode_ = engine::DBOptions::MODE::SINGLE;
    } else if (config.cluster.role() == ClusterRole::RO) {
        opt.mode_ = engine::DBOptions::MODE::CLUSTER_READONLY;
    } else if (config.cluster.role() == ClusterRole::RW) {
        opt.mode_ = engine::DBOptions::MODE::CLUSTER_WRITABLE;
    } else {
        std::cerr << "Error: cluster.role is not one of rw and ro." << std::endl;
        kill(0, SIGUSR1);
    }

    opt.wal_enable_ = config.wal.enable();

    // disable wal for ci devtest
    opt.wal_enable_ = false;

    if (opt.wal_enable_) {
        opt.recovery_error_ignore_ = config.wal.recovery_error_ignore();
        int64_t wal_buffer_size = config.wal.buffer_size();
        wal_buffer_size /= (1024 * 1024);
        opt.buffer_size_ = wal_buffer_size;
        opt.mxlog_path_ = config.wal.path();
    }

    // engine config
    int64_t omp_thread = config.engine.omp_thread_num();

    if (omp_thread > 0) {
        omp_set_num_threads(omp_thread);
        LOG_SERVER_DEBUG_ << "Specify openmp thread number: " << omp_thread;
    } else {
        int64_t sys_thread_cnt = 8;
        if (GetSystemAvailableThreads(sys_thread_cnt)) {
            omp_thread = static_cast<int32_t>(ceil(sys_thread_cnt * 0.5));
            omp_set_num_threads(omp_thread);
        }
    }

    // init faiss global variable
    int64_t use_blas_threshold = config.engine.use_blas_threshold();
    faiss::distance_compute_blas_threshold = use_blas_threshold;

    // set archive config
    engine::ArchiveConf::CriteriaT criterial;
    int64_t disk = config.db.archive_disk_threshold();
    int64_t days = config.db.archive_days_threshold();

    if (disk > 0) {
        criterial[engine::ARCHIVE_CONF_DISK] = disk;
    }

    if (days > 0) {
        criterial[engine::ARCHIVE_CONF_DAYS] = days;
    }
    opt.meta_.archive_conf_.SetCriterias(criterial);

    // create db root folder
    s = CommonUtil::CreateDirectory(opt.meta_.path_);
    if (!s.ok()) {
        std::cerr << "Error: Failed to create database primary path: " << path
                  << ". Possible reason: db_config.primary_path is wrong in server_config.yaml or not available."
                  << std::endl;
        kill(0, SIGUSR1);
    }

    try {
        db_ = engine::DBFactory::BuildDB(opt);
        db_->Start();
    } catch (std::exception& ex) {
        std::cerr << "Error: failed to open database: " << ex.what()
                  << ". Possible reason: out of storage, meta schema is damaged "
                  << "or created by in-compatible Milvus version." << std::endl;
        kill(0, SIGUSR1);
    }

    // preload collection
    std::string preload_collections = config.cache.preload_collection();
    s = PreloadCollections(preload_collections);
    if (!s.ok()) {
        std::cerr << "ERROR! Failed to preload collections: " << preload_collections << std::endl;
        std::cerr << s.ToString() << std::endl;
        kill(0, SIGUSR1);
    }

    return Status::OK();
}

Status
DBWrapper::StopService() {
    if (db_) {
        db_->Stop();
    }

    // SS TODO
    /* engine::snapshot::OperationExecutor::GetInstance().Stop(); */
    return Status::OK();
}

Status
DBWrapper::PreloadCollections(const std::string& preload_collections) {
    if (preload_collections.empty()) {
        // do nothing
    } else if (preload_collections == "*") {
        // load all collections
        std::vector<std::string> names;
        auto status = db_->ListCollections(names);
        if (!status.ok()) {
            return status;
        }

        for (auto& name : names) {
            std::vector<std::string> field_names;  // input empty field names will load all fileds
            auto status = db_->LoadCollection(nullptr, name, field_names);
            if (!status.ok()) {
                return status;
            }
        }
    } else {
        std::vector<std::string> collection_names;
        StringHelpFunctions::SplitStringByDelimeter(preload_collections, ",", collection_names);
        for (auto& name : collection_names) {
            std::vector<std::string> field_names;  // input empty field names will load all fileds
            auto status = db_->LoadCollection(nullptr, name, field_names);
            if (!status.ok()) {
                return status;
            }
        }
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
