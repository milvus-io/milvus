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

#include "utils/CommonUtil.h"
#include "cache/CpuCacheMgr.h"
#include "cache/GpuCacheMgr.h"
#include "server/Config.h"
#include "utils/Log.h"

#include <dirent.h>
#include <pwd.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/sysinfo.h>
#include <time.h>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <vector>

#include "boost/filesystem.hpp"

#if defined(__x86_64__)
#define THREAD_MULTIPLY_CPU 1
#elif defined(__powerpc64__)
#define THREAD_MULTIPLY_CPU 4
#else
#define THREAD_MULTIPLY_CPU 1
#endif

namespace milvus {
namespace server {

namespace fs = boost::filesystem;

bool
CommonUtil::GetSystemMemInfo(uint64_t& total_mem, uint64_t& free_mem) {
    struct sysinfo info;
    int ret = sysinfo(&info);
    total_mem = info.totalram;
    free_mem = info.freeram;

    return ret == 0;  // succeed 0, failed -1
}

bool
CommonUtil::GetSystemAvailableThreads(int64_t& thread_count) {
    // threadCnt = std::thread::hardware_concurrency();
    thread_count = sysconf(_SC_NPROCESSORS_CONF);
    thread_count *= THREAD_MULTIPLY_CPU;
    if (thread_count == 0) {
        thread_count = 8;
    }

    return true;
}

bool
CommonUtil::IsDirectoryExist(const std::string& path) {
    DIR* dp = nullptr;
    if ((dp = opendir(path.c_str())) == nullptr) {
        return false;
    }

    closedir(dp);
    return true;
}

Status
CommonUtil::CreateDirectory(const std::string& path) {
    if (path.empty()) {
        return Status::OK();
    }

    struct stat directory_stat;
    int status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return Status::OK();  // already exist
    }

    fs::path fs_path(path);
    fs::path parent_path = fs_path.parent_path();
    Status err_status = CreateDirectory(parent_path.string());
    if (!err_status.ok()) {
        return err_status;
    }

    status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return Status::OK();  // already exist
    }

    int makeOK = mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IROTH);
    if (makeOK != 0) {
        return Status(SERVER_UNEXPECTED_ERROR, "failed to create directory: " + path);
    }

    return Status::OK();
}

namespace {
void
RemoveDirectory(const std::string& path) {
    DIR* dir = nullptr;
    struct dirent* dmsg;
    const int32_t buf_size = 256;
    char file_name[buf_size];

    std::string folder_name = path + "/%s";
    if ((dir = opendir(path.c_str())) != nullptr) {
        while ((dmsg = readdir(dir)) != nullptr) {
            if (strcmp(dmsg->d_name, ".") != 0 && strcmp(dmsg->d_name, "..") != 0) {
                snprintf(file_name, buf_size, folder_name.c_str(), dmsg->d_name);
                std::string tmp = file_name;
                if (tmp.find(".") == std::string::npos) {
                    RemoveDirectory(file_name);
                }
                remove(file_name);
            }
        }
    }

    if (dir != nullptr) {
        closedir(dir);
    }
    remove(path.c_str());
}
}  // namespace

Status
CommonUtil::DeleteDirectory(const std::string& path) {
    if (path.empty()) {
        return Status::OK();
    }

    struct stat directory_stat;
    int statOK = stat(path.c_str(), &directory_stat);
    if (statOK != 0) {
        return Status::OK();
    }

    RemoveDirectory(path);
    return Status::OK();
}

bool
CommonUtil::IsFileExist(const std::string& path) {
    return (access(path.c_str(), F_OK) == 0);
}

uint64_t
CommonUtil::GetFileSize(const std::string& path) {
    struct stat file_info;
    if (stat(path.c_str(), &file_info) < 0) {
        return 0;
    }

    return static_cast<uint64_t>(file_info.st_size);
}

std::string
CommonUtil::GetFileName(std::string filename) {
    int pos = filename.find_last_of('/');
    return filename.substr(pos + 1);
}

std::string
CommonUtil::GetExePath() {
    const size_t buf_len = 1024;
    char buf[buf_len];
    size_t cnt = readlink("/proc/self/exe", buf, buf_len);
    if (cnt < 0 || cnt >= buf_len) {
        return "";
    }

    buf[cnt] = '\0';

    std::string exe_path = buf;
    if (exe_path.rfind('/') != exe_path.length() - 1) {
        std::string sub_str = exe_path.substr(0, exe_path.rfind('/'));
        return sub_str + "/";
    }
    return exe_path;
}

bool
CommonUtil::TimeStrToTime(const std::string& time_str, time_t& time_integer, tm& time_struct,
                          const std::string& format) {
    time_integer = 0;
    memset(&time_struct, 0, sizeof(tm));

    int ret = sscanf(time_str.c_str(), format.c_str(), &(time_struct.tm_year), &(time_struct.tm_mon),
                     &(time_struct.tm_mday), &(time_struct.tm_hour), &(time_struct.tm_min), &(time_struct.tm_sec));
    if (ret <= 0) {
        return false;
    }

    time_struct.tm_year -= 1900;
    time_struct.tm_mon--;
    time_integer = mktime(&time_struct);

    return true;
}

void
CommonUtil::ConvertTime(time_t time_integer, tm& time_struct) {
    localtime_r(&time_integer, &time_struct);
}

void
CommonUtil::ConvertTime(tm time_struct, time_t& time_integer) {
    time_integer = mktime(&time_struct);
}

void
CommonUtil::EraseFromCache(const std::string& item_key) {
    if (item_key.empty()) {
        SERVER_LOG_ERROR << "Empty key cannot be erased from cache";
        return;
    }

    cache::CpuCacheMgr::GetInstance()->EraseItem(item_key);

#ifdef MILVUS_GPU_VERSION
    server::Config& config = server::Config::GetInstance();
    std::vector<int64_t> gpus;
    Status s = config.GetGpuResourceConfigSearchResources(gpus);
    for (auto& gpu : gpus) {
        cache::GpuCacheMgr::GetInstance(gpu)->EraseItem(item_key);
    }
#endif
}

}  // namespace server
}  // namespace milvus
