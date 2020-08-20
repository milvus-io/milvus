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

#include "server/init/Directory.h"

#include <fcntl.h>
#include <fiu/fiu-local.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <string>

#include "config/ServerConfig.h"

namespace milvus::server {
Status
Directory::Initialize(const std::string& storage_path, const std::string& wal_path) {
    try {
        init(storage_path);
        if (not wal_path.empty()) {
            init(wal_path);
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }
    return Status::OK();
}

Status
Directory::Lock(const std::string& storage_path, const std::string& wal_path) {
    try {
        lock(storage_path);
        if (not wal_path.empty()) {
            lock(wal_path);
        }
    } catch (std::exception& ex) {
        return Status(SERVER_UNEXPECTED_ERROR, ex.what());
    }
    return Status::OK();
}

void
Directory::init(const std::string& path) {
    try {
        // Returns True if a new directory was created, otherwise false.
        boost::filesystem::create_directories(path);
    } catch (std::exception& ex) {
        std::string msg = "Cannot create directory: " + path + ", reason: " + ex.what();
        throw std::runtime_error(msg);
    } catch (...) {
        std::string msg = "Cannot create directory: " + path;
        throw std::runtime_error(msg);
    }
}

void
Directory::lock(const std::string& path) {
    std::string lock_path = path + "/lock";
    auto fd = open(lock_path.c_str(), O_RDWR | O_CREAT | O_NOFOLLOW, 0640);
    fiu_do_on("Directory.lock.fd", fd = -1);
    if (fd < 0) {
        std::string msg = "Cannot lock file: " + lock_path + ", reason: ";
        if (errno == EROFS) {
            // Not using locking for read-only lock file
            msg += "Lock file is read-only.";
        } else {
            msg += strerror(errno);
        }
        throw std::runtime_error(msg);
    }

    // Acquire a write lock
    struct flock fl;
    // exclusive lock
    fl.l_type = F_WRLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start = 0;
    fl.l_len = 0;
    auto fcl = fcntl(fd, F_SETLK, &fl);
    fiu_do_on("Directory.lock.fcntl", fcl = -1);
    if (fcl == -1) {
        std::string msg = "Cannot lock file: " + lock_path + ", reason: ";
        if (errno == EACCES || errno == EAGAIN) {
            msg += "Permission denied.";
        } else if (errno == ENOLCK) {
            // Not using locking for nfs mounted lock file
            msg += "Using nfs.";
        } else {
            msg += std::string(strerror(errno)) + ".";
        }
        close(fd);
        throw std::runtime_error(msg);
    }
}

}  // namespace milvus::server
