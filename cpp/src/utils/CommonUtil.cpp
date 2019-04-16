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

#include "boost/filesystem.hpp"

#if defined(__x86_64__)
#define THREAD_MULTIPLY_CPU 1
#elif defined(__powerpc64__)
#define THREAD_MULTIPLY_CPU 4
#else
#define THREAD_MULTIPLY_CPU 1
#endif

namespace zilliz {
namespace vecwise {
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

bool CommonUtil::IsDirectoryExit(const std::string &path)
{
    DIR *dp = nullptr;
    if ((dp = opendir(path.c_str())) == nullptr) {
        return false;
    }

    closedir(dp);
    return true;
}

ServerError CommonUtil::CreateDirectory(const std::string &path) {
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

ServerError DeleteDirectory(const std::string &path) {
    struct stat directoryStat;
    int statOK = stat(path.c_str(), &directoryStat);
    if (statOK != 0)
        return SERVER_SUCCESS;

    RemoveDirectory(path);
    return SERVER_SUCCESS;
}

bool IsFileExist(const std::string &path) {
    return (access(path.c_str(), F_OK) == 0);
}


}
}
}
