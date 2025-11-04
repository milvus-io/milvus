// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "LocalChunkManager.h"
#include "boost/algorithm/string/join.hpp"
#include "boost/filesystem/directory.hpp"
#include "log/Log.h"

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <fstream>
#include <sstream>

#include "common/EasyAssert.h"
#include "common/Exception.h"

namespace milvus::storage {

bool
LocalChunkManager::Exist(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);
    boost::system::error_code err;
    bool isExist = boost::filesystem::exists(absPath, err);
    if (err && err.value() != boost::system::errc::no_such_file_or_directory) {
        ThrowInfo(FileReadFailed,
                  fmt::format("local file {} exist interface failed, error: {}",
                              filepath,
                              err.message()));
    }
    return isExist;
}

uint64_t
LocalChunkManager::Size(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);

    if (!Exist(filepath)) {
        ThrowInfo(PathNotExist, "invalid local path:" + absPath.string());
    }
    boost::system::error_code err;
    int64_t size = boost::filesystem::file_size(absPath, err);
    if (err) {
        ThrowInfo(FileReadFailed,
                  fmt::format("get local file {} size failed, error: {}",
                              filepath,
                              err.message()));
    }
    return size;
}

void
LocalChunkManager::Remove(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);
    boost::system::error_code err;
    boost::filesystem::remove(absPath, err);
    if (err) {
        ThrowInfo(FileWriteFailed,
                  fmt::format("remove local file {} failed, error: {}",
                              filepath,
                              err.message()));
    }
}

uint64_t
LocalChunkManager::Read(const std::string& filepath, void* buf, uint64_t size) {
    return Read(filepath, 0, buf, size);
}

uint64_t
LocalChunkManager::Read(const std::string& filepath,
                        uint64_t offset,
                        void* buf,
                        uint64_t size) {
    std::ifstream infile;
    infile.open(filepath.data(), std::ios_base::binary);
    if (infile.fail()) {
        std::stringstream err_msg;
        err_msg << "Error: open local file '" << filepath << " failed, "
                << strerror(errno);
        ThrowInfo(FileOpenFailed, err_msg.str());
    }

    infile.seekg(offset, std::ios::beg);
    if (!infile.read(reinterpret_cast<char*>(buf), size)) {
        if (!infile.eof()) {
            std::stringstream err_msg;
            err_msg << "Error: read local file '" << filepath << " failed, "
                    << strerror(errno);
            ThrowInfo(FileReadFailed, err_msg.str());
        }
    }
    return infile.gcount();
}

void
LocalChunkManager::Write(const std::string& absPathStr,
                         void* buf,
                         uint64_t size) {
    boost::filesystem::path absPath(absPathStr);
    // if filepath not exists, will create this file automatically
    // ensure upper directory exist firstly
    boost::filesystem::create_directories(absPath.parent_path());

    std::ofstream outfile;
    outfile.open(absPathStr.data(), std::ios_base::binary);
    if (outfile.fail()) {
        std::stringstream err_msg;
        err_msg << "Error: open local file '" << absPathStr << " failed, "
                << strerror(errno);
        ThrowInfo(FileOpenFailed, err_msg.str());
    }
    if (!outfile.write(reinterpret_cast<char*>(buf), size)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << absPathStr << " failed, "
                << strerror(errno);
        ThrowInfo(FileWriteFailed, err_msg.str());
    }
}

void
LocalChunkManager::Write(const std::string& absPathStr,
                         uint64_t offset,
                         void* buf,
                         uint64_t size) {
    boost::filesystem::path absPath(absPathStr);
    // if filepath not exists, will create this file automatically
    // ensure upper directory exist firstly
    boost::filesystem::create_directories(absPath.parent_path());

    std::ofstream outfile;
    outfile.open(
        absPathStr.data(),
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
    if (outfile.fail()) {
        std::stringstream err_msg;
        err_msg << "Error: open local file '" << absPathStr << " failed, "
                << strerror(errno);
        ThrowInfo(FileOpenFailed, err_msg.str());
    }

    outfile.seekp(offset, std::ios::beg);
    if (!outfile.write(reinterpret_cast<char*>(buf), size)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << absPathStr << " failed, "
                << strerror(errno);
        ThrowInfo(FileWriteFailed, err_msg.str());
    }
}

std::vector<std::string>
LocalChunkManager::ListWithPrefix(const std::string& filepath) {
    ThrowInfo(NotImplemented,
              GetName() + "::ListWithPrefix" + " not implement now");
}

bool
LocalChunkManager::CreateFile(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);
    // if filepath not exists, will create this file automatically
    // ensure upper directory exist firstly
    boost::filesystem::create_directories(absPath.parent_path());
    auto absPathStr = absPath.string();
    std::ofstream file;
    file.open(absPathStr, std::ios_base::out);
    if (!file.is_open()) {
        std::stringstream err_msg;
        err_msg << "Error: create new local file '" << absPathStr << " failed, "
                << strerror(errno);
        ThrowInfo(FileCreateFailed, err_msg.str());
    }
    file.close();
    return true;
}

bool
LocalChunkManager::DirExist(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    boost::system::error_code err;
    bool isExist = boost::filesystem::exists(dirPath, err);
    if (err && err.value() != boost::system::errc::no_such_file_or_directory) {
        ThrowInfo(
            FileWriteFailed,
            fmt::format("local directory {} exist interface failed, error: {}",
                        dir,
                        err.message()));
    }
    return isExist;
}

void
LocalChunkManager::CreateDir(const std::string& dir) {
    bool isExist = DirExist(dir);
    if (isExist) {
        ThrowInfo(PathAlreadyExist, "dir:" + dir + " already exists");
    }
    boost::filesystem::path dirPath(dir);
    auto create_success = boost::filesystem::create_directories(dirPath);
    if (!create_success) {
        ThrowInfo(FileCreateFailed, "create dir:" + dir + " failed");
    }
}

void
LocalChunkManager::RemoveDir(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    boost::system::error_code err;
    boost::filesystem::remove_all(dirPath, err);
    if (err) {
        boost::filesystem::directory_iterator it(dirPath);
        std::vector<std::string> paths;
        for (; it != boost::filesystem::directory_iterator(); ++it) {
            paths.push_back(it->path().string());
        }
        std::string files = boost::algorithm::join(paths, ", ");
        ThrowInfo(FileWriteFailed,
                  fmt::format(
                      "remove local directory:{} failed, error: {}, files: {}",
                      dir,
                      err.message(),
                      files));
    }
}

// GetSizeOfDir is used to get the size of a directory, it will recursively
// get the size of all files and subdirectories in the directory.
// If file/directory doesn't exist, ignore it because it may be removed concurrently;
int64_t
LocalChunkManager::GetSizeOfDir(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    boost::system::error_code it_ec;
    boost::filesystem::directory_iterator it(dirPath, it_ec);
    if (it_ec) {
        if (it_ec.value() == boost::system::errc::no_such_file_or_directory) {
            return 0;
        }
        ThrowInfo(FileReadFailed,
                  fmt::format("iterate directory {} failed, error: {}",
                              dir,
                              it_ec.message()));
    }

    int64_t total_file_size = 0;
    boost::filesystem::directory_iterator end_it;
    for (; it != end_it; ++it) {
        boost::system::error_code status_ec;
        auto status = it->status(status_ec);
        if (status_ec) {
            if (status_ec.value() ==
                boost::system::errc::no_such_file_or_directory) {
                continue;
            }
            ThrowInfo(FileReadFailed,
                      fmt::format("get status of {} failed, error: {}",
                                  it->path().string(),
                                  status_ec.message()));
        }

        // Check if current entry is a regular file
        if (boost::filesystem::is_regular_file(status)) {
            boost::system::error_code file_size_ec;
            auto file_size =
                boost::filesystem::file_size(it->path(), file_size_ec);
            if (file_size_ec) {
                if (file_size_ec.value() ==
                    boost::system::errc::no_such_file_or_directory) {
                    continue;
                }
                ThrowInfo(FileReadFailed,
                          fmt::format("get size of file {} failed, error: {}",
                                      it->path().string(),
                                      file_size_ec.message()));
            }
            total_file_size += file_size;
            continue;
        }

        // Check if current entry is a subdirectory
        if (boost::filesystem::is_directory(status)) {
            total_file_size += GetSizeOfDir(it->path().string());
        }
    }

    return total_file_size;
}

}  // namespace milvus::storage
