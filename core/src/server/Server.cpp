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
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <cstring>
#include <unordered_map>

#include "config/ServerConfig.h"
#include "tracing/TracerUtil.h"
#include "log/LogMgr.h"
#include <yaml-cpp/yaml.h>
#include "utils/Log.h"
#include "utils/SignalHandler.h"
#include "utils/TimeRecorder.h"

namespace milvus {
namespace server {

Server&
Server::GetInstance() {
    static Server server;
    return server;
}

void
Server::Init(int64_t daemonized, const std::string& pid_filename, const std::string& config_filename) {
    daemonized_ = daemonized;
    pid_filename_ = pid_filename;
    config_filename_ = config_filename;
}

void
Server::Daemonize() {
}

Status
Server::Start() {
}

void
Server::Stop() {
}

Status
Server::StartService() {
    Status stat;
return stat;
}

void
Server::StopService() {
}

void
Server::LogConfigInFile(const std::string& path) {
}

void
Server::LogCpuInfo() {
}

}  // namespace server
}  // namespace milvus
