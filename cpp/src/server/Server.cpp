////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include <thread>
#include "Server.h"
//#include "ServerConfig.h"
#ifdef MILVUS_ENABLE_THRIFT
#include "server/thrift_impl/MilvusServer.h"
#else
#include "server/grpc_impl/GrpcMilvusServer.h"
#endif

#include "utils/Log.h"
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

#include "metrics/Metrics.h"

namespace zilliz {
namespace milvus {
namespace server {

Server*
Server::Instance() {
    static Server server;
    return &server;
}

Server::Server() {

}
Server::~Server() {

}

void
Server::Init(int64_t daemonized, const std::string& pid_filename, const std::string& config_filename) {
    daemonized_ = daemonized;
    pid_filename_ = pid_filename;
    config_filename_ = config_filename;
}

void
Server::Daemonize() {
    if (daemonized_ == 0) {
        return;
    }

    SERVER_LOG_INFO << "Milvus server run in daemonize mode";

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
    if(ret != 0){
        return;
    }

    // Close all open fd
    for (long fd = sysconf(_SC_OPEN_MAX); fd > 0; fd--) {
        close(fd);
    }

    SERVER_LOG_INFO << "Redirect stdin/stdout/stderr to /dev/null";

    // Redirect stdin/stdout/stderr to /dev/null
    stdin = fopen("/dev/null", "r");
    stdout = fopen("/dev/null", "w+");
    stderr = fopen("/dev/null", "w+");
    // Try to write PID of daemon to lockfile
    if (!pid_filename_.empty()) {
        pid_fd = open(pid_filename_.c_str(), O_RDWR | O_CREAT, 0640);
        if (pid_fd < 0) {
            SERVER_LOG_INFO << "Can't open filename: " + pid_filename_ + ", Error: " + strerror(errno);
            exit(EXIT_FAILURE);
        }
        if (lockf(pid_fd, F_TLOCK, 0) < 0) {
            SERVER_LOG_INFO << "Can't lock filename: " + pid_filename_ + ", Error: " + strerror(errno);
            exit(EXIT_FAILURE);
        }

        std::string pid_file_context = std::to_string(getpid());
        ssize_t res = write(pid_fd, pid_file_context.c_str(), pid_file_context.size());
        if(res != 0){
            return;
        }
    }
}

int
Server::Start() {

    if (daemonized_) {
        Daemonize();
    }

    do {
        try {
            // Read config file
            if(LoadConfig() != SERVER_SUCCESS) {
                return 1;
            }

            //log path is defined by LoadConfig, so InitLog must be called after LoadConfig
            ServerConfig &config = ServerConfig::GetInstance();
            ConfigNode server_config = config.GetConfig(CONFIG_SERVER);

            // Handle Signal
            signal(SIGINT, SignalUtil::HandleSignal);
            signal(SIGHUP, SignalUtil::HandleSignal);
            signal(SIGTERM, SignalUtil::HandleSignal);
            server::Metrics::GetInstance().Init();
            server::SystemInfo::GetInstance().Init();
            engine::SchedServInit();
            std::cout << "Milvus server start successfully." << std::endl;
            StartService();

        } catch(std::exception& ex){
            SERVER_LOG_ERROR << "Milvus server encounter exception: " << std::string(ex.what())
                             << "Is another server instance running?";
            break;
        }
    } while(false);

    Stop();
    return 0;
}

void
Server::Stop() {
    std::cout << "Milvus server is going to shutdown ..." << std::endl;

    // Unlock and close lockfile
    if (pid_fd != -1) {
        int ret = lockf(pid_fd, F_ULOCK, 0);
        if(ret != 0){
            std::cout << "Can't lock file: " << strerror(errno) << std::endl;
            exit(0);
        }
        ret = close(pid_fd);
        if(ret != 0){
            std::cout << "Can't close file: " << strerror(errno) << std::endl;
            exit(0);
        }
    }

    // Try to delete lockfile
    if (!pid_filename_.empty()) {
        int ret = unlink(pid_filename_.c_str());
        if(ret != 0){
            std::cout << "Can't unlink file: " << strerror(errno) << std::endl;
            exit(0);
        }
    }

    running_ = 0;

    StopService();

    std::cout << "Milvus server is closed!" << std::endl;
}


ServerError
Server::LoadConfig() {
    ServerConfig::GetInstance().LoadConfigFile(config_filename_);
    ServerError err = ServerConfig::GetInstance().ValidateConfig();
    if(err != SERVER_SUCCESS){
        exit(0);
    }

    return SERVER_SUCCESS;
}

void
Server::StartService() {
#ifdef MILVUS_ENABLE_THRIFT
    MilvusServer::StartService();
#else
    grpc::GrpcMilvusServer::StartService();
#endif
}

void
Server::StopService() {
#ifdef MILVUS_ENABLE_THRIFT
    MilvusServer::StopService();
#else
    grpc::GrpcMilvusServer::StopService();
#endif
}

}
}
}
