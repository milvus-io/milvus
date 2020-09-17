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

#pragma once

#include <string>
#include <vector>

#include "config/ServerConfig.h"
#include "utils/Status.h"

namespace milvus::server {

class Server {
 public:
    static Server&
    GetInstance();

    void
    Init(int64_t daemonized, const std::string& pid_filename, const std::string& config_filename);

    Status
    Start();
    void
    Stop();

 private:
    Server() = default;
    ~Server() = default;

    void
    Daemonize();

    Status
    StartService();
    void
    StopService();

 private:
    static std::string
    RunningMode(bool cluster_enable, ClusterRole cluster_role);

    static void
    LogConfigInFile(const std::string& path);

    static void
    LogCpuInfo();

 private:
    int64_t daemonized_ = 0;
    int pid_fd_ = -1;
    std::string pid_filename_;
    std::string config_filename_;
    /* Used for lock work directory */
    std::vector<int64_t> fd_list_;
};  // Server

}  // namespace milvus::server
