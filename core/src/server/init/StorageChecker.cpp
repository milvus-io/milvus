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

#include <fiu-local.h>

#include "config/Config.h"
#include "utils/Log.h"
#include "utils/StringHelpFunctions.h"

namespace milvus {
namespace server {

Status
StorageChecker::CheckStoragePermission() {
    auto& config = Config::GetInstance();
    /* Check log file write permission */
    std::string logs_path;
    auto status = config.GetLogsPath(logs_path);
    if (!status.ok()) {
        return status;
    }
    int ret = access(logs_path.c_str(), F_OK | R_OK | W_OK);
    fiu_do_on("StorageChecker.CheckStoragePermission.logs_path_access_fail", ret = -1);
    if (0 != ret) {
        std::string err_msg =
            " Access log path " + logs_path + " fail. " + strerror(errno) + "(code: " + std::to_string(errno) + ")";
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

#if 1
    bool cluster_enable = false;
    std::string cluster_role;
    STATUS_CHECK(config.GetClusterConfigEnable(cluster_enable));
    STATUS_CHECK(config.GetClusterConfigRole(cluster_role));

    if (cluster_enable && cluster_role == "ro") {
        return Status::OK();
    }
#else
    std::string deploy_mode;
    status = config.GetServerConfigDeployMode(deploy_mode);
    if (!status.ok()) {
        return status;
    }

    if (deploy_mode == "cluster_readonly") {
        return Status::OK();
    }
#endif

    /* Check db directory write permission */
    std::string primary_path;
    status = config.GetStorageConfigPath(primary_path);
    if (!status.ok()) {
        return status;
    }

    ret = access(primary_path.c_str(), F_OK | R_OK | W_OK);
    fiu_do_on("StorageChecker.CheckStoragePermission.db_primary_path_access_fail", ret = -1);
    if (0 != ret) {
        std::string err_msg = " Access DB storage path " + primary_path + " fail. " + strerror(errno) +
                              "(code: " + std::to_string(errno) + ")";
        LOG_SERVER_FATAL_ << err_msg;
        std::cerr << err_msg << std::endl;
        return Status(SERVER_UNEXPECTED_ERROR, err_msg);
    }

    /* Check wal directory write permission */
    bool wal_enable = false;
    status = config.GetWalConfigEnable(wal_enable);
    if (!status.ok()) {
        return status;
    }

    if (wal_enable) {
        std::string wal_path;
        status = config.GetWalConfigWalPath(wal_path);
        if (!status.ok()) {
            return status;
        }
        ret = access(wal_path.c_str(), F_OK | R_OK | W_OK);
        fiu_do_on("StorageChecker.CheckStoragePermission.wal_path_access_fail", ret = -1);
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
