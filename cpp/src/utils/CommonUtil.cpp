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

bool CommonUtil::GetSystemMemInfo(unsigned long &totalMem, unsigned long &freeMem) {
    struct sysinfo info;
    int ret = sysinfo(&info);
    totalMem = info.totalram;
    freeMem = info.freeram;

    return ret == 0;//succeed 0, failed -1
}

bool CommonUtil::GetSystemAvailableThreads(unsigned int &threadCnt) {
    //threadCnt = std::thread::hardware_concurrency();
    threadCnt = sysconf(_SC_NPROCESSORS_CONF);
    threadCnt *= THREAD_MULTIPLY_CPU;
    if (threadCnt == 0)
        threadCnt = 8;

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

    struct stat directoryStat;
    int statOK = stat(path.c_str(), &directoryStat);
    if (statOK == 0) {
        return SERVER_SUCCESS;//already exist
    }

    fs::path fs_path(path);
    fs::path parent_path = fs_path.parent_path();
    ServerError err = CreateDirectory(parent_path.string());
    if(err != SERVER_SUCCESS){
        return err;
    }

    statOK = stat(path.c_str(), &directoryStat);
    if (statOK == 0) {
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
        DIR *pDir = NULL;
        struct dirent *dmsg;
        char szFileName[256];
        char szFolderName[256];

        strcpy(szFolderName, path.c_str());
        strcat(szFolderName, "/%s");
        if ((pDir = opendir(path.c_str())) != NULL) {
            while ((dmsg = readdir(pDir)) != NULL) {
                if (strcmp(dmsg->d_name, ".") != 0
                    && strcmp(dmsg->d_name, "..") != 0) {
                    sprintf(szFileName, szFolderName, dmsg->d_name);
                    std::string tmp = szFileName;
                    if (tmp.find(".") == std::string::npos) {
                        RemoveDirectory(szFileName);
                    }
                    remove(szFileName);
                }
            }
        }

        if (pDir != NULL) {
            closedir(pDir);
        }
        remove(path.c_str());
    }
}

ServerError CommonUtil::DeleteDirectory(const std::string &path) {
    if(path.empty()) {
        return SERVER_SUCCESS;
    }

    struct stat directoryStat;
    int statOK = stat(path.c_str(), &directoryStat);
    if (statOK != 0)
        return SERVER_SUCCESS;

    RemoveDirectory(path);
    return SERVER_SUCCESS;
}

bool CommonUtil::IsFileExist(const std::string &path) {
    return (access(path.c_str(), F_OK) == 0);
}

uint64_t CommonUtil::GetFileSize(const std::string &path) {
    struct stat fileInfo;
    if (stat(path.c_str(), &fileInfo) < 0) {
        return 0;
    } else {
        return (uint64_t)fileInfo.st_size;
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
