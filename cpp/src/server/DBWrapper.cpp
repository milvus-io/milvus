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


#include "server/DBWrapper.h"
#include "Config.h"
#include "db/DBFactory.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

#include <string>
#include <omp.h>
#include <faiss/utils.h>

namespace zilliz {
namespace milvus {
namespace server {

DBWrapper::DBWrapper() {
}

Status DBWrapper::StartService() {
    Config& config = Config::GetInstance();
    Status s;
    //db config
    engine::DBOptions opt;

    s = config.GetDBConfigBackendUrl(opt.meta_.backend_uri_);
    if (!s.ok()) return s;

    std::string path;
    s = config.GetDBConfigPrimaryPath(path);
    if (!s.ok()) return s;

    opt.meta_.path_ = path + "/db";

    std::string db_slave_path;
    s = config.GetDBConfigSecondaryPath(db_slave_path);
    if (!s.ok()) return s;

    StringHelpFunctions::SplitStringByDelimeter(db_slave_path, ";", opt.meta_.slave_paths_);

    // cache config
    s = config.GetCacheConfigCacheInsertData(opt.insert_cache_immediately_);
    if (!s.ok()) return s;

    std::string mode;
    s = config.GetServerConfigDeployMode(mode);
    if (!s.ok()) return s;

    if (mode == "single") {
        opt.mode_ = engine::DBOptions::MODE::SINGLE;
    } else if (mode == "cluster_readonly") {
        opt.mode_ = engine::DBOptions::MODE::CLUSTER_READONLY;
    } else if (mode == "cluster_writable") {
        opt.mode_ = engine::DBOptions::MODE::CLUSTER_WRITABLE;
    } else {
        std::cerr <<
        "ERROR: mode specified in server_config must be ['single', 'cluster_readonly', 'cluster_writable']"
        << std::endl;
        kill(0, SIGUSR1);
    }

    // engine config
    int32_t omp_thread;
    s = config.GetEngineConfigOmpThreadNum(omp_thread);
    if (!s.ok()) return s;
    if (omp_thread > 0) {
        omp_set_num_threads(omp_thread);
        SERVER_LOG_DEBUG << "Specify openmp thread number: " << omp_thread;
    } else {
        uint32_t sys_thread_cnt = 8;
        if (CommonUtil::GetSystemAvailableThreads(sys_thread_cnt)) {
            omp_thread = (int32_t)ceil(sys_thread_cnt*0.5);
            omp_set_num_threads(omp_thread);
        }
    }

    //init faiss global variable
    int32_t blas_threshold;
    s = config.GetEngineConfigBlasThreshold(blas_threshold);
    if (!s.ok()) return s;
    faiss::distance_compute_blas_threshold = blas_threshold;

    //set archive config
    engine::ArchiveConf::CriteriaT criterial;
    int32_t disk, days;
    s = config.GetDBConfigArchiveDiskThreshold(disk);
    if (!s.ok()) return s;
    if (disk > 0) {
        criterial[engine::ARCHIVE_CONF_DISK] = disk;
    }

    s = config.GetDBConfigArchiveDaysThreshold(days);
    if (!s.ok()) return s;
    if (days > 0) {
        criterial[engine::ARCHIVE_CONF_DAYS] = days;
    }
    opt.meta_.archive_conf_.SetCriterias(criterial);

    //create db root folder
    Status status = CommonUtil::CreateDirectory(opt.meta_.path_);
    if (!status.ok()) {
        std::cerr << "ERROR! Failed to create database root path: " << opt.meta_.path_ << std::endl;
        kill(0, SIGUSR1);
    }

    for (auto& path : opt.meta_.slave_paths_) {
        status = CommonUtil::CreateDirectory(path);
        if (!status.ok()) {
            std::cerr << "ERROR! Failed to create database slave path: " << path << std::endl;
            kill(0, SIGUSR1);
        }
    }

    //create db instance
    try {
        db_ = engine::DBFactory::Build(opt);
    } catch(std::exception& ex) {
        std::cerr << "ERROR! Failed to open database: " << ex.what() << std::endl;
        kill(0, SIGUSR1);
    }

    db_->Start();

    return Status::OK();
}

Status DBWrapper::StopService() {
    if (db_) {
        db_->Stop();
    }

    return Status::OK();
}

} // namespace server
} // namespace milvus
} // namespace zilliz
