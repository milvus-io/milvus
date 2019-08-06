/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/

#include "DBWrapper.h"
#include "ServerConfig.h"
#include "utils/CommonUtil.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

#include <omp.h>

namespace zilliz {
namespace milvus {
namespace server {

DBWrapper::DBWrapper() {
    //db config
    zilliz::milvus::engine::Options opt;
    ConfigNode& db_config = ServerConfig::GetInstance().GetConfig(CONFIG_DB);
    opt.meta.backend_uri = db_config.GetValue(CONFIG_DB_URL);
    std::string db_path = db_config.GetValue(CONFIG_DB_PATH);
    opt.meta.path = db_path + "/db";

    std::string db_slave_path = db_config.GetValue(CONFIG_DB_SLAVE_PATH);
    StringHelpFunctions::SplitStringByDelimeter(db_slave_path, ";", opt.meta.slave_paths);

    int64_t index_size = db_config.GetInt64Value(CONFIG_DB_INDEX_TRIGGER_SIZE);
    if(index_size > 0) {//ensure larger than zero, unit is MB
        opt.index_trigger_size = (size_t)index_size * engine::ONE_MB;
    }
    int64_t insert_buffer_size = db_config.GetInt64Value(CONFIG_DB_INSERT_BUFFER_SIZE, 4);
    if (insert_buffer_size >= 1) {
        opt.insert_buffer_size = insert_buffer_size * engine::ONE_GB;
    }
    else {
        std::cout << "ERROR: insert_buffer_size should be at least 1 GB" << std::endl;
        kill(0, SIGUSR1);
    }

    // cache config
    ConfigNode& cache_config = ServerConfig::GetInstance().GetConfig(CONFIG_CACHE);
    opt.insert_cache_immediately_ = cache_config.GetBoolValue(CONFIG_INSERT_CACHE_IMMEDIATELY, false);

    ConfigNode& serverConfig = ServerConfig::GetInstance().GetConfig(CONFIG_SERVER);
    std::string mode = serverConfig.GetValue(CONFIG_CLUSTER_MODE, "single");
    if (mode == "single") {
        opt.mode = zilliz::milvus::engine::Options::MODE::SINGLE;
    }
    else if (mode == "cluster") {
        opt.mode = zilliz::milvus::engine::Options::MODE::CLUSTER;
    }
    else if (mode == "read_only") {
        opt.mode = zilliz::milvus::engine::Options::MODE::READ_ONLY;
    }
    else {
        std::cout << "ERROR: mode specified in server_config is not one of ['single', 'cluster', 'read_only']" << std::endl;
        kill(0, SIGUSR1);
    }

    // engine config
    ConfigNode& engine_config = ServerConfig::GetInstance().GetConfig(CONFIG_ENGINE);
    float omp_thread_rate = engine_config.GetFloatValue(CONFIG_OMP_THREAD_RATE, 0.75);
    uint32_t cpu_count = 1;
    CommonUtil::GetSystemAvailableThreads(cpu_count);
    float thread_count = omp_thread_rate*(float)cpu_count;
    int32_t omp_thread = (int32_t)round(thread_count);
    omp_set_num_threads(omp_thread);
    SERVER_LOG_DEBUG << "Openmp thread number = " << omp_thread;

    //set archive config
    engine::ArchiveConf::CriteriaT criterial;
    int64_t disk = db_config.GetInt64Value(CONFIG_DB_ARCHIVE_DISK, 0);
    int64_t days = db_config.GetInt64Value(CONFIG_DB_ARCHIVE_DAYS, 0);
    if(disk > 0) {
        criterial[engine::ARCHIVE_CONF_DISK] = disk;
    }
    if(days > 0) {
        criterial[engine::ARCHIVE_CONF_DAYS] = days;
    }
    opt.meta.archive_conf.SetCriterias(criterial);

    //create db root folder
    ServerError err = CommonUtil::CreateDirectory(opt.meta.path);
    if(err != SERVER_SUCCESS) {
        std::cout << "ERROR! Failed to create database root path: " << opt.meta.path << std::endl;
        kill(0, SIGUSR1);
    }

    for(auto& path : opt.meta.slave_paths) {
        err = CommonUtil::CreateDirectory(path);
        if(err != SERVER_SUCCESS) {
            std::cout << "ERROR! Failed to create database slave path: " << path << std::endl;
            kill(0, SIGUSR1);
        }
    }

    std::string msg = opt.meta.path;
    try {
        zilliz::milvus::engine::DB::Open(opt, &db_);
    } catch(std::exception& ex) {
        msg = ex.what();
    }

    if(db_ == nullptr) {
        std::cout << "ERROR! Failed to open database: " << msg << std::endl;
        kill(0, SIGUSR1);
    }
}

DBWrapper::~DBWrapper() {
    delete db_;
}

}
}
}