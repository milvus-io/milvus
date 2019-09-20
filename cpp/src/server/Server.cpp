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

#include <thread>
#include "Server.h"
#include "server/grpc_impl/GrpcServer.h"
#include "utils/Log.h"
#include "utils/LogUtil.h"
#include "utils/SignalUtil.h"
#include "utils/TimeRecorder.h"
#include "metrics/Metrics.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <csignal>
//#include <numaif.h>
#include <unistd.h>
#include <string.h>
#include <src/scheduler/SchedInst.h>
#include "src/wrapper/KnowhereResource.h"

#include "metrics/Metrics.h"
#include "DBWrapper.h"


namespace zilliz {
namespace milvus {
namespace server {

Server &
Server::Instance() {
    static Server server;
    return server;
}

Server::Server() {

}
Server::~Server() {

}

void
Server::Init(int64_t daemonized,
             const std::string &pid_filename,
             const std::string &config_filename,
             const std::string &log_config_file) {
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
//    SERVER_LOG_INFO << "Log will be exported to: " + log_path);

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
    for (long fd = sysconf(_SC_OPEN_MAX); fd > 0; fd--) {
        close(fd);
    }

    std::cout << "Redirect stdin/stdout/stderr to /dev/null";

    // Redirect stdin/stdout/stderr to /dev/null
    stdin = fopen("/dev/null", "r");
    stdout = fopen("/dev/null", "w+");
    stderr = fopen("/dev/null", "w+");
    // Try to write PID of daemon to lockfile
    if (!pid_filename_.empty()) {
        pid_fd = open(pid_filename_.c_str(), O_RDWR | O_CREAT, 0640);
        if (pid_fd < 0) {
            std::cerr << "Can't open filename: " + pid_filename_ + ", Error: " + strerror(errno);
            exit(EXIT_FAILURE);
        }
        if (lockf(pid_fd, F_TLOCK, 0) < 0) {
            std::cerr << "Can't lock filename: " + pid_filename_ + ", Error: " + strerror(errno);
            exit(EXIT_FAILURE);
        }

        std::string pid_file_context = std::to_string(getpid());
        ssize_t res = write(pid_fd, pid_file_context.c_str(), pid_file_context.size());
        if (res != 0) {
            return;
        }
    }
}

void
Server::Start() {
    if (daemonized_) {
        Daemonize();
    }

    try {
        /* Read config file */
        if (LoadConfig() != SERVER_SUCCESS) {
            std::cerr << "Milvus server fail to load config file" << std::endl;
            return;
        }

        /* log path is defined in Config file, so InitLog must be called after LoadConfig */
        ServerConfig &config = ServerConfig::GetInstance();
        ConfigNode server_config = config.GetConfig(CONFIG_SERVER);

        std::string time_zone = server_config.GetValue(CONFIG_TIME_ZONE, "UTC+8");
        if (time_zone.length() == 3) {
            time_zone = "CUT";
        } else {
            int time_bias = std::stoi(time_zone.substr(3, std::string::npos));
            if (time_bias == 0)
                time_zone = "CUT";
            else if (time_bias > 0) {
                time_zone = "CUT" + std::to_string(-time_bias);
            } else {
                time_zone = "CUT+" + std::to_string(-time_bias);
            }
        }

        if (setenv("TZ", time_zone.c_str(), 1) != 0) {
            std::cerr << "Fail to setenv" << std::endl;
            return;
        }
        tzset();

        InitLog(log_config_file_);

        server::Metrics::GetInstance().Init();
        server::SystemInfo::GetInstance().Init();

        StartService();
        std::cout << "Milvus server start successfully." << std::endl;

    } catch (std::exception &ex) {
        std::cerr << "Milvus server encounter exception: " << ex.what();
    }
}

void
Server::Stop() {
    std::cerr << "Milvus server is going to shutdown ..." << std::endl;

    /* Unlock and close lockfile */
    if (pid_fd != -1) {
        int ret = lockf(pid_fd, F_ULOCK, 0);
        if (ret != 0) {
            std::cerr << "Can't lock file: " << strerror(errno) << std::endl;
            exit(0);
        }
        ret = close(pid_fd);
        if (ret != 0) {
            std::cerr << "Can't close file: " << strerror(errno) << std::endl;
            exit(0);
        }
    }

    /* delete lockfile */
    if (!pid_filename_.empty()) {
        int ret = unlink(pid_filename_.c_str());
        if (ret != 0) {
            std::cerr << "Can't unlink file: " << strerror(errno) << std::endl;
            exit(0);
        }
    }

    StopService();

    std::cerr << "Milvus server is closed!" << std::endl;
}


ErrorCode
Server::LoadConfig() {
    ServerConfig::GetInstance().LoadConfigFile(config_filename_);
    auto status = ServerConfig::GetInstance().ValidateConfig();
    if (!status.ok()) {
        std::cerr << "Failed to load config file: " << config_filename_ << std::endl;
        exit(0);
    }

    return SERVER_SUCCESS;
}

void
Server::StartService() {
    engine::KnowhereResource::Initialize();
    engine::StartSchedulerService();
    DBWrapper::GetInstance().StartService();
    grpc::GrpcServer::GetInstance().Start();
}

void
Server::StopService() {
    grpc::GrpcServer::GetInstance().Stop();
    DBWrapper::GetInstance().StopService();
    engine::StopSchedulerService();
    engine::KnowhereResource::Finalize();
}

}
}
}
