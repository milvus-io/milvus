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

#include "server/init/StorageChecker.h"

#include <unistd.h>

#include <string>
#include <vector>


#include "config/ServerConfig.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

namespace milvus {
namespace server {

Status
StorageChecker::CheckStoragePermission() {
    /* Check log file write permission */
    const std::string& logs_path = config.logs.path();
    int ret = access(logs_path.c_str(), F_OK | R_OK | W_OK);
    if (0 != ret) {
        std::string err_msg =
            " Access log path " + logs_path + " fail. " + strerror(errno) + "(code: " + std::to_string(errno) + ")";
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    if (config.cluster.enable() && config.cluster.role() == ClusterRole::RO) {
        return Status::OK();
    }

    /* Check db directory write permission */
    const std::string& primary_path = config.storage.path();

    ret = access(primary_path.c_str(), F_OK | R_OK | W_OK);
    if (0 != ret) {
        std::string err_msg = " Access DB storage path " + primary_path + " fail. " + strerror(errno) +
                              "(code: " + std::to_string(errno) + ")";
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    /* Check wal directory write permission */
    if (config.wal.enable()) {
        const std::string& wal_path = config.wal.path();

        ret = access(wal_path.c_str(), F_OK | R_OK | W_OK);
        if (0 != ret) {
            std::string err_msg = " Access WAL storage path " + wal_path + " fail. " + strerror(errno) +
                                  "(code: " + std::to_string(errno) + ")";
            LOG_SERVER_FATAL_ << err_msg;
            std::cerr << err_msg << std::endl;
            return Status(SERVER_UNEXPECTED_ERROR, err_msg);
        }
    }

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
