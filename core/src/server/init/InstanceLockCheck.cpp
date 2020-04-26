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

#include "server/init/InstanceLockCheck.h"
#include "utils/Log.h"

#include <errno.h>
#include <fcntl.h>
#include <unistd.h>

namespace milvus {
namespace server {

Status
InstanceLockCheck::Check(const std::string& path) {
    std::string lock_path = path + "/lock";
    auto fd = open(lock_path.c_str(), O_RDWR | O_CREAT | O_NOFOLLOW, 0640);
    if (fd < 0) {
        std::string msg;
        if (errno == EROFS) {
            // Not using locking for read-only lock file
            msg += "Lock file is read-only.";
        }
        msg += "Could not open lock file.";
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    // Acquire a write lock
    struct flock fl;
    // exclusive lock
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    if (fcntl(fd, F_SETLK, &fl) == -1) {
        std::string msg;
        if (errno == EACCES || errno == EAGAIN) {
            msg += "Permission denied. ";
        } else if (errno == ENOLCK) {
            // Not using locking for nfs mounted lock file
            msg += "Using nfs. ";
        }
        close(fd);
        msg += "Could not get lock.";
        return Status(SERVER_UNEXPECTED_ERROR, msg);
    }

    LOG_SERVER_INFO_ << "InstanceLockCheck passed.";

    return Status::OK();
}

}  // namespace server
}  // namespace milvus
