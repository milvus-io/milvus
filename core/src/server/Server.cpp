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

#include "server/Server.h"

#include <fcntl.h>
#include <string.h>
#include <unistd.h>

#include "config/Config.h"
#include "index/archive/KnowhereResource.h"
#include "metrics/Metrics.h"
#include "scheduler/SchedInst.h"
#include "server/DBWrapper.h"
#include "server/grpc_impl/GrpcServer.h"
#include "server/web_impl/WebServer.h"
#include "src/version.h"
//#include "storage/s3/S3ClientWrapper.h"
#include "tracing/TracerUtil.h"
#include "utils/Log.h"
#include "utils/LogUtil.h"
#include "utils/SignalUtil.h"
#include "utils/TimeRecorder.h"

#include "search/TaskInst.h"

namespace milvus {
namespace server {

Server&
Server::GetInstance() {
    static Server server;
    return server;
}

void
Server::Init(int64_t daemonized, const std::string& pid_filename, const std::string& config_filename,
             const std::string& log_config_file) {
    daemonized_ = daemonized;
    pid_filename_ = pid_filename;
    config_filename_ = config_filename;
    log_config_file_ = log_config_file;
}

void
Server::Daemonize() {
    if (daemonized_ == 0) {
        return;
    }

    std::cout << "Milvus server run in daemonize mode";

    //    std::string log_path(GetLogDirFullPath());
    //    log_path += "zdb_server.(INFO/WARNNING/ERROR/CRITICAL)";
    //    LOG_SERVER_INFO_ << "Log will be exported to: " + log_path);

    pid_t pid = 0;

    // Fork off the parent process
    pid = fork();

    // An error occurred
    if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    // Success: terminate parent
    if (pid > 0) {
        exit(EXIT_SUCCESS);
    }

    // On success: The child process becomes session leader
    if (setsid() < 0) {
        exit(EXIT_FAILURE);
    }

    // Ignore signal sent from child to parent process
    signal(SIGCHLD, SIG_IGN);

    // Fork off for the second time
    pid = fork();

    // An error occurred
    if (pid < 0) {
        exit(EXIT_FAILURE);
    }

    // Terminate the parent
    if (pid > 0) {
        exit(EXIT_SUCCESS);
    }

    // Set new file permissions
    umask(0);

    // Change the working directory to root
    int ret = chdir("/");
    if (ret != 0) {
        return;
    }

    // Close all open fd
    for (int64_t fd = sysconf(_SC_OPEN_MAX); fd > 0; fd--) {
        close(fd);
    }

    std::cout << "Redirect stdin/stdout/stderr to /dev/null";

    // Redirect stdin/stdout/stderr to /dev/null
    stdin = fopen("/dev/null", "r");
    stdout = fopen("/dev/null", "w+");
    stderr = fopen("/dev/null", "w+");
    // Try to write PID of daemon to lockfile
    if (!pid_filename_.empty()) {
        pid_fd_ = open(pid_filename_.c_str(), O_RDWR | O_CREAT, 0640);
        if (pid_fd_ < 0) {
            std::cerr << "Can't open filename: " + pid_filename_ + ", Error: " + strerror(errno);
            exit(EXIT_FAILURE);
        }
        if (lockf(pid_fd_, F_TLOCK, 0) < 0) {
            std::cerr << "Can't lock filename: " + pid_filename_ + ", Error: " + strerror(errno);
            exit(EXIT_FAILURE);
        }

        std::string pid_file_context = std::to_string(getpid());
        ssize_t res = write(pid_fd_, pid_file_context.c_str(), pid_file_context.size());
        if (res != 0) {
            return;
        }
    }
}

Status
Server::Start() {
    if (daemonized_ != 0) {
        Daemonize();
    }

    try {
        /* Read config file */
        Status s = LoadConfig();
        if (!s.ok()) {
            std::cerr << "ERROR: Milvus server fail to load config file" << std::endl;
            return s;
        }

        Config& config = Config::GetInstance();

        /* Init opentracing tracer from config */
        std::string tracing_config_path;
        s = config.GetTracingConfigJsonConfigPath(tracing_config_path);
        tracing_config_path.empty() ? tracing::TracerUtil::InitGlobal()
                                    : tracing::TracerUtil::InitGlobal(tracing_config_path);

        /* log path is defined in Config file, so InitLog must be called after LoadConfig */
        std::string time_zone;
        s = config.GetServerConfigTimeZone(time_zone);
        if (!s.ok()) {
            std::cerr << "Fail to get server config timezone" << std::endl;
            return s;
        }

        if (time_zone.length() == 3) {
            time_zone = "CUT";
        } else {
            int time_bias = std::stoi(time_zone.substr(3, std::string::npos));
            if (time_bias == 0) {
                time_zone = "CUT";
            } else if (time_bias > 0) {
                time_zone = "CUT" + std::to_string(-time_bias);
            } else {
                time_zone = "CUT+" + std::to_string(-time_bias);
            }
        }

        if (setenv("TZ", time_zone.c_str(), 1) != 0) {
            return Status(SERVER_UNEXPECTED_ERROR, "Fail to setenv");
        }
        tzset();

        InitLog(log_config_file_);

        // print version information
        LOG_SERVER_INFO_ << "Milvus " << BUILD_TYPE << " version: v" << MILVUS_VERSION << ", built at " << BUILD_TIME;
#ifdef MILVUS_GPU_VERSION
        LOG_SERVER_INFO_ << "GPU edition";
#else
        LOG_SERVER_INFO_ << "CPU edition";
#endif
        /* record config and hardware information into log */
        LogConfigInFile(config_filename_);
        LogCpuInfo();
        LogConfigInMem();

        server::Metrics::GetInstance().Init();
        server::SystemInfo::GetInstance().Init();

        return StartService();
    } catch (std::exception& ex) {
        std::string str = "Milvus server encounter exception: " + std::string(ex.what());
        return Status(SERVER_UNEXPECTED_ERROR, str);
    }
}

void
Server::Stop() {
    std::cerr << "Milvus server is going to shutdown ..." << std::endl;

    /* Unlock and close lockfile */
    if (pid_fd_ != -1) {
        int ret = lockf(pid_fd_, F_ULOCK, 0);
        if (ret != 0) {
            std::cerr << "ERROR: Can't lock file: " << strerror(errno) << std::endl;
            exit(0);
        }
        ret = close(pid_fd_);
        if (ret != 0) {
            std::cerr << "ERROR: Can't close file: " << strerror(errno) << std::endl;
            exit(0);
        }
    }

    /* delete lockfile */
    if (!pid_filename_.empty()) {
        int ret = unlink(pid_filename_.c_str());
        if (ret != 0) {
            std::cerr << "ERROR: Can't unlink file: " << strerror(errno) << std::endl;
            exit(0);
        }
    }

    StopService();

    std::cerr << "Milvus server exit..." << std::endl;
}

Status
Server::LoadConfig() {
    Config& config = Config::GetInstance();
    Status s = config.LoadConfigFile(config_filename_);
    if (!s.ok()) {
        std::cerr << s.message() << std::endl;
        return s;
    }

    s = config.ValidateConfig();
    if (!s.ok()) {
        std::cerr << "Config check fail: " << s.message() << std::endl;
        return s;
    }
    return milvus::Status::OK();
}

Status
Server::StartService() {
    Status stat;
    stat = engine::KnowhereResource::Initialize();
    if (!stat.ok()) {
        LOG_SERVER_ERROR_ << "KnowhereResource initialize fail: " << stat.message();
        goto FAIL;
    }

    scheduler::StartSchedulerService();

    stat = DBWrapper::GetInstance().StartService();
    if (!stat.ok()) {
        LOG_SERVER_ERROR_ << "DBWrapper start service fail: " << stat.message();
        goto FAIL;
    }

    grpc::GrpcServer::GetInstance().Start();
    web::WebServer::GetInstance().Start();

    // stat = storage::S3ClientWrapper::GetInstance().StartService();
    // if (!stat.ok()) {
    //     LOG_SERVER_ERROR_ << "S3Client start service fail: " << stat.message();
    //     goto FAIL;
    // }

    //    search::TaskInst::GetInstance().Start();

    return Status::OK();
FAIL:
    std::cerr << "Milvus initializes fail: " << stat.message() << std::endl;
    return stat;
}

void
Server::StopService() {
    //    search::TaskInst::GetInstance().Stop();
    // storage::S3ClientWrapper::GetInstance().StopService();
    web::WebServer::GetInstance().Stop();
    grpc::GrpcServer::GetInstance().Stop();
    DBWrapper::GetInstance().StopService();
    scheduler::StopSchedulerService();
    engine::KnowhereResource::Finalize();
}

}  // namespace server
}  // namespace milvus
