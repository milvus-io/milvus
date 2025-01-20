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
        PanicInfo(FileReadFailed,
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
        PanicInfo(PathNotExist, "invalid local path:" + absPath.string());
    }
    boost::system::error_code err;
    int64_t size = boost::filesystem::file_size(absPath, err);
    if (err) {
        PanicInfo(FileReadFailed,
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
        PanicInfo(FileWriteFailed,
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
        PanicInfo(FileOpenFailed, err_msg.str());
    }

    infile.seekg(offset, std::ios::beg);
    if (!infile.read(reinterpret_cast<char*>(buf), size)) {
        if (!infile.eof()) {
            std::stringstream err_msg;
            err_msg << "Error: read local file '" << filepath << " failed, "
                    << strerror(errno);
            PanicInfo(FileReadFailed, err_msg.str());
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
        PanicInfo(FileOpenFailed, err_msg.str());
    }
    if (!outfile.write(reinterpret_cast<char*>(buf), size)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << absPathStr << " failed, "
                << strerror(errno);
        PanicInfo(FileWriteFailed, err_msg.str());
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
        PanicInfo(FileOpenFailed, err_msg.str());
    }

    outfile.seekp(offset, std::ios::beg);
    if (!outfile.write(reinterpret_cast<char*>(buf), size)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << absPathStr << " failed, "
                << strerror(errno);
        PanicInfo(FileWriteFailed, err_msg.str());
    }
}

std::vector<std::string>
LocalChunkManager::ListWithPrefix(const std::string& filepath) {
    PanicInfo(NotImplemented,
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
        PanicInfo(FileCreateFailed, err_msg.str());
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
        PanicInfo(
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
        PanicInfo(PathAlreadyExist, "dir:" + dir + " already exists");
    }
    boost::filesystem::path dirPath(dir);
    auto create_success = boost::filesystem::create_directories(dirPath);
    if (!create_success) {
        PanicInfo(FileCreateFailed, "create dir:" + dir + " failed");
    }
}

void
LocalChunkManager::RemoveDir(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    boost::system::error_code err;
    int retry_times = 10;
    // workaround for concurrent tantivy reload and remove dir
    while (retry_times > 0) {
        boost::filesystem::remove_all(dirPath, err);
        if (err) {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            retry_times--;
            continue;
        }
        break;
    }
    if (err) {
        boost::filesystem::directory_iterator it(dirPath);
        std::vector<std::string> paths;
        for (; it != boost::filesystem::directory_iterator(); ++it) {
            paths.push_back(it->path().string());
        }
        std::string files = boost::algorithm::join(paths, ", ");
        PanicInfo(FileWriteFailed,
                  fmt::format(
                      "remove local directory:{} failed, error: {}, files: {}",
                      dir,
                      err.message(),
                      files));
    }
}

int64_t
LocalChunkManager::GetSizeOfDir(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    bool is_dir = boost::filesystem::is_directory(dirPath);
    if (!is_dir) {
        PanicInfo(PathNotExist, "dir:" + dir + " not exists");
    }

    using boost::filesystem::directory_entry;
    using boost::filesystem::directory_iterator;
    std::vector<directory_entry> v;
    copy(directory_iterator(dirPath), directory_iterator(), back_inserter(v));

    int64_t total_file_size = 0;
    for (std::vector<directory_entry>::const_iterator it = v.begin();
         it != v.end();
         ++it) {
        if (boost::filesystem::is_regular_file(it->path())) {
            boost::system::error_code ec;
            auto file_size = boost::filesystem::file_size(it->path(), ec);
            if (ec) {
                // The file may be removed concurrently by other threads.
                // So the file size cannot be obtained, just ignore it.
                LOG_INFO("size of file {} cannot be obtained with error: {}",
                         it->path().string(),
                         ec.message());
                continue;
            }
            total_file_size += file_size;
        }
        if (boost::filesystem::is_directory(it->path())) {
            total_file_size += GetSizeOfDir(it->path().string());
        }
    }

    return total_file_size;
}

}  // namespace milvus::storage
