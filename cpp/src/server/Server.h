/*******************************************************************************
 * Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 ******************************************************************************/
#pragma once

#include "ServerConfig.h"
#include "utils/Error.h"

#include <cstdint>
#include <string>

namespace zilliz {
namespace vecwise {
namespace server {

class Server {
   public:
    static Server* Instance();

    void Init(int64_t daemonized, const std::string& pid_filename, const std::string& config_filename);
    int Start();
    void Stop();

   private:
    Server();
    ~Server();

    void Daemonize();

    static void HandleSignal(int signal);
    ServerError LoadConfig();

    void StartService();
    void StopService();

   private:
    int64_t daemonized_ = 0;
    int64_t running_ = 1;
    int pid_fd = -1;
    std::string pid_filename_;
    std::string config_filename_;
    ServerConfig* opt_config_ptr_ = nullptr;
};  // Server

}   // server
}   // sql
}   // zilliz
