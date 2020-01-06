// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include <faiss/utils/distances.h>
#include <omp.h>
#include <cmath>
#include <string>
#include <vector>

#include "db/DBFactory.h"
#include "server/Config.h"
#include "server/DBWrapper.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

namespace milvus {
namespace server {

Status
DBWrapper::StartService() {
    Config& config = Config::GetInstance();
    Status s;

    // db config
    engine::DBOptions opt;
    s = config.GetDBConfigBackendUrl(opt.meta_.backend_uri_);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    std::string path;
    s = config.GetStorageConfigPrimaryPath(path);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    opt.meta_.path_ = path + "/db";

    std::string db_slave_path;
    s = config.GetStorageConfigSecondaryPath(db_slave_path);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    StringHelpFunctions::SplitStringByDelimeter(db_slave_path, ";", opt.meta_.slave_paths_);

    // cache config
    s = config.GetCacheConfigCacheInsertData(opt.insert_cache_immediately_);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    std::string mode;
    s = config.GetServerConfigDeployMode(mode);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    if (mode == "single") {
        opt.mode_ = engine::DBOptions::MODE::SINGLE;
    } else if (mode == "cluster_readonly") {
        opt.mode_ = engine::DBOptions::MODE::CLUSTER_READONLY;
    } else if (mode == "cluster_writable") {
        opt.mode_ = engine::DBOptions::MODE::CLUSTER_WRITABLE;
    } else {
        std::cerr << "Error: server_config.deploy_mode in server_config.yaml is not one of "
                  << "single, cluster_readonly, and cluster_writable." << std::endl;
        kill(0, SIGUSR1);
    }

    // engine config
    int64_t omp_thread;
    s = config.GetEngineConfigOmpThreadNum(omp_thread);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    if (omp_thread > 0) {
        omp_set_num_threads(omp_thread);
        SERVER_LOG_DEBUG << "Specify openmp thread number: " << omp_thread;
    } else {
        int64_t sys_thread_cnt = 8;
        if (CommonUtil::GetSystemAvailableThreads(sys_thread_cnt)) {
            omp_thread = static_cast<int32_t>(ceil(sys_thread_cnt * 0.5));
            omp_set_num_threads(omp_thread);
        }
    }

    // init faiss global variable
    int64_t use_blas_threshold;
    s = config.GetEngineConfigUseBlasThreshold(use_blas_threshold);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    faiss::distance_compute_blas_threshold = use_blas_threshold;

    // set archive config
    engine::ArchiveConf::CriteriaT criterial;
    int64_t disk, days;
    s = config.GetDBConfigArchiveDiskThreshold(disk);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    if (disk > 0) {
        criterial[engine::ARCHIVE_CONF_DISK] = disk;
    }

    s = config.GetDBConfigArchiveDaysThreshold(days);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
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

    for (auto& path : opt.meta_.slave_paths_) {
        s = CommonUtil::CreateDirectory(path);
        if (!s.ok()) {
            std::cerr << "Error: Failed to create database secondary path: " << path
                      << ". Possible reason: db_config.secondary_path is wrong in server_config.yaml or not available."
                      << std::endl;
            kill(0, SIGUSR1);
        }
    }

    // create db instance
    try {
        db_ = engine::DBFactory::Build(opt);
    } catch (std::exception& ex) {
        std::cerr << "Error: failed to open database: " << ex.what()
                  << ". Possible reason: the meta system does not work." << std::endl;
        kill(0, SIGUSR1);
    }

    db_->Start();

    // preload table
    std::string preload_tables;
    s = config.GetDBConfigPreloadTable(preload_tables);
    if (!s.ok()) {
        std::cerr << s.ToString() << std::endl;
        return s;
    }

    s = PreloadTables(preload_tables);
    if (!s.ok()) {
        std::cerr << "ERROR! Failed to preload tables: " << preload_tables << std::endl;
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

    return Status::OK();
}

Status
DBWrapper::PreloadTables(const std::string& preload_tables) {
    if (preload_tables.empty()) {
        // do nothing
    } else if (preload_tables == "*") {
        // load all tables
        std::vector<engine::meta::TableSchema> table_schema_array;
        db_->AllTables(table_schema_array);

        for (auto& schema : table_schema_array) {
            auto status = db_->PreloadTable(schema.table_id_);
            if (!status.ok()) {
                return status;
            }
        }
    } else {
        std::vector<std::string> table_names;
        StringHelpFunctions::SplitStringByDelimeter(preload_tables, ",", table_names);
        for (auto& name : table_names) {
            auto status = db_->PreloadTable(name);
            if (!status.ok()) {
                return status;
            }
        }
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
