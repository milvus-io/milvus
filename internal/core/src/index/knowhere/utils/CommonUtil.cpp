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

#include "utils/CommonUtil.h"
#include "utils/Log.h"

#include <dirent.h>
#include <fiu/fiu-local.h>
#include <pwd.h>
#include <sys/stat.h>
#include <unistd.h>
#include <boost/filesystem.hpp>
#include <iostream>
#include <vector>

namespace milvus {

namespace fs = boost::filesystem;

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
    fiu_do_on("CommonUtil.CreateDirectory.create_parent_fail", err_status = Status(SERVER_INVALID_ARGUMENT, ""));
    if (!err_status.ok()) {
        return err_status;
    }

    status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return Status::OK();  // already exist
    }

    int makeOK = mkdir(path.c_str(), S_IRWXU | S_IRGRP | S_IROTH);
    fiu_do_on("CommonUtil.CreateDirectory.create_dir_fail", makeOK = 1);
    if (makeOK != 0) {
        return Status(SERVER_UNEXPECTED_ERROR, "failed to create directory: " + path);
    }

    return Status::OK();
}

namespace {
void
RemoveDirectory(const std::string& path) {
    DIR* dir = nullptr;
    const int32_t buf_size = 256;
    char file_name[buf_size];

    std::string folder_name = path + "/%s";
    if ((dir = opendir(path.c_str())) != nullptr) {
        struct dirent* dmsg;
        while ((dmsg = readdir(dir)) != nullptr) {
            if (strcmp(dmsg->d_name, ".") != 0 && strcmp(dmsg->d_name, "..") != 0) {
                snprintf(file_name, buf_size, folder_name.c_str(), dmsg->d_name);
                std::string tmp = file_name;
                if (tmp.find('.') == std::string::npos) {
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
    const int64_t buf_len = 1024;
    char buf[buf_len];
    int64_t cnt = readlink("/proc/self/exe", buf, buf_len);
    fiu_do_on("CommonUtil.GetExePath.readlink_fail", cnt = -1);
    if (cnt < 0 || cnt >= buf_len) {
        return "";
    }

    buf[cnt] = '\0';

    std::string exe_path = buf;
    fiu_do_on("CommonUtil.GetExePath.exe_path_error", exe_path = "/");
    if (exe_path.rfind('/') != exe_path.length() - 1) {
        std::string sub_str = exe_path.substr(0, exe_path.rfind('/'));
        return sub_str + "/";
    }
    return exe_path;
}

bool
CommonUtil::TimeStrToTime(const std::string& time_str,
                          time_t& time_integer,
                          tm& time_struct,
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
CommonUtil::GetCurrentTimeStr(std::string& time_str) {
    auto t = std::time(nullptr);
    struct tm ltm;
    localtime_r(&t, &ltm);

    time_str = "";
    time_str += std::to_string(ltm.tm_year + 1900);
    time_str += "-";
    time_str += std::to_string(ltm.tm_mon + 1);
    time_str += "-";
    time_str += std::to_string(ltm.tm_mday);
    time_str += "_";
    time_str += std::to_string(ltm.tm_hour);
    time_str += ":";
    time_str += std::to_string(ltm.tm_min);
    time_str += ":";
    time_str += std::to_string(ltm.tm_sec);
}

void
CommonUtil::ConvertTime(time_t time_integer, tm& time_struct) {
    localtime_r(&time_integer, &time_struct);
}

void
CommonUtil::ConvertTime(tm time_struct, time_t& time_integer) {
    time_integer = mktime(&time_struct);
}

std::string
CommonUtil::ConvertSize(int64_t size) {
    const int64_t gb = 1024ll * 1024 * 1024;
    const int64_t mb = 1024ll * 1024;
    const int64_t kb = 1024ll;
    if (size % gb == 0) {
        return std::to_string(size / gb) + "GB";
    } else if (size % mb == 0) {
        return std::to_string(size / mb) + "MB";
    } else if (size % kb == 0) {
        return std::to_string(size / kb) + "KB";
    } else {
        return std::to_string(size);
    }
}

#ifdef ENABLE_CPU_PROFILING
std::string
CommonUtil::GetCurrentTimeStr() {
    time_t tt;
    time(&tt);
    tt = tt + 8 * 60;
    tm t;
    gmtime_r(&tt, &t);

    std::string str = std::to_string(t.tm_year + 1900) + "_" + std::to_string(t.tm_mon + 1) + "_" +
                      std::to_string(t.tm_mday) + "_" + std::to_string(t.tm_hour) + "_" + std::to_string(t.tm_min) +
                      "_" + std::to_string(t.tm_sec);
    return str;
}
#endif

}  // namespace milvus
