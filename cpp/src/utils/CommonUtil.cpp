////////////////////////////////////////////////////////////////////////////////
// Copyright 上海赜睿信息科技有限公司(Zilliz) - All Rights Reserved
// Unauthorized copying of this file, via any medium is strictly prohibited.
// Proprietary and confidential.
////////////////////////////////////////////////////////////////////////////////
#include "CommonUtil.h"
#include "utils/Log.h"

#include <unistd.h>
#include <sys/sysinfo.h>
#include <pwd.h>
#include <thread>
#include <sys/stat.h>
#include <dirent.h>
#include <string.h>
#include <iostream>
#include <time.h>

#include "boost/filesystem.hpp"

#if defined(__x86_64__)
#define THREAD_MULTIPLY_CPU 1
#elif defined(__powerpc64__)
#define THREAD_MULTIPLY_CPU 4
#else
#define THREAD_MULTIPLY_CPU 1
#endif

namespace zilliz {
namespace milvus {
namespace server {

namespace fs = boost::filesystem;

bool CommonUtil::GetSystemMemInfo(unsigned long &total_mem, unsigned long &free_mem) {
    struct sysinfo info;
    int ret = sysinfo(&info);
    total_mem = info.totalram;
    free_mem = info.freeram;

    return ret == 0;//succeed 0, failed -1
}

bool CommonUtil::GetSystemAvailableThreads(unsigned int &thread_count) {
    //threadCnt = std::thread::hardware_concurrency();
    thread_count = sysconf(_SC_NPROCESSORS_CONF);
    thread_count *= THREAD_MULTIPLY_CPU;
    if (thread_count == 0)
        thread_count = 8;

    return true;
}

bool CommonUtil::IsDirectoryExist(const std::string &path) {
    DIR *dp = nullptr;
    if ((dp = opendir(path.c_str())) == nullptr) {
        return false;
    }

    closedir(dp);
    return true;
}

ServerError CommonUtil::CreateDirectory(const std::string &path) {
    if(path.empty()) {
        return SERVER_SUCCESS;
    }

    struct stat directory_stat;
    int status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return SERVER_SUCCESS;//already exist
    }

    fs::path fs_path(path);
    fs::path parent_path = fs_path.parent_path();
    ServerError err = CreateDirectory(parent_path.string());
    if(err != SERVER_SUCCESS){
        return err;
    }

    status = stat(path.c_str(), &directory_stat);
    if (status == 0) {
        return SERVER_SUCCESS;//already exist
    }

    int makeOK = mkdir(path.c_str(), S_IRWXU|S_IRGRP|S_IROTH);
    if (makeOK != 0) {
        return SERVER_UNEXPECTED_ERROR;
    }

    return SERVER_SUCCESS;
}

namespace {
    void RemoveDirectory(const std::string &path) {
        DIR *dir = nullptr;
        struct dirent *dmsg;
        char file_name[256];
        char folder_name[256];

        strcpy(folder_name, path.c_str());
        strcat(folder_name, "/%s");
        if ((dir = opendir(path.c_str())) != nullptr) {
            while ((dmsg = readdir(dir)) != nullptr) {
                if (strcmp(dmsg->d_name, ".") != 0
                    && strcmp(dmsg->d_name, "..") != 0) {
                    sprintf(file_name, folder_name, dmsg->d_name);
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
}

ServerError CommonUtil::DeleteDirectory(const std::string &path) {
    if(path.empty()) {
        return SERVER_SUCCESS;
    }

    struct stat directory_stat;
    int statOK = stat(path.c_str(), &directory_stat);
    if (statOK != 0)
        return SERVER_SUCCESS;

    RemoveDirectory(path);
    return SERVER_SUCCESS;
}

bool CommonUtil::IsFileExist(const std::string &path) {
    return (access(path.c_str(), F_OK) == 0);
}

uint64_t CommonUtil::GetFileSize(const std::string &path) {
    struct stat file_info;
    if (stat(path.c_str(), &file_info) < 0) {
        return 0;
    } else {
        return (uint64_t)file_info.st_size;
    }
}

std::string CommonUtil::GetExePath() {
    const size_t buf_len = 1024;
    char buf[buf_len];
    size_t cnt = readlink("/proc/self/exe", buf, buf_len);
    if(cnt < 0|| cnt >= buf_len) {
        return "";
    }

    buf[cnt] = '\0';

    std::string exe_path = buf;
    if(exe_path.rfind('/') != exe_path.length()){
        std::string sub_str = exe_path.substr(0, exe_path.rfind('/'));
        return sub_str + "/";
    }
    return exe_path;
}

bool CommonUtil::TimeStrToTime(const std::string& time_str,
                     time_t &time_integer,
                     tm &time_struct,
                     const std::string& format) {
    time_integer = 0;
    memset(&time_struct, 0, sizeof(tm));

    int ret = sscanf(time_str.c_str(),
        format.c_str(),
        &(time_struct.tm_year),
        &(time_struct.tm_mon),
        &(time_struct.tm_mday),
        &(time_struct.tm_hour),
        &(time_struct.tm_min),
        &(time_struct.tm_sec));
    if(ret <= 0) {
        return false;
    }

    time_struct.tm_year -= 1900;
    time_struct.tm_mon--;
    time_integer = mktime(&time_struct);

    return true;
}

void CommonUtil::ConvertTime(time_t time_integer, tm &time_struct) {
    tm* t_m = localtime (&time_integer);
    memcpy(&time_struct, t_m, sizeof(tm));
}

void CommonUtil::ConvertTime(tm time_struct, time_t &time_integer) {
    time_integer = mktime(&time_struct);
}

}
}
}
