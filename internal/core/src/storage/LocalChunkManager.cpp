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
#include "log/Log.h"

#include <algorithm>
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

    boost::system::error_code exist_err;
    bool isExist = boost::filesystem::exists(absPath, exist_err);
    if (exist_err &&
        exist_err.value() != boost::system::errc::no_such_file_or_directory) {
        ThrowInfo(FileReadFailed,
                  fmt::format("local file {} exist interface failed, error: {}",
                              filepath,
                              exist_err.message()));
    }
    if (!isExist) {
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
    auto prefix = boost::filesystem::path(filepath);
    if (prefix.is_relative()) {
        prefix = boost::filesystem::path(path_prefix_) / prefix;
    }
    prefix = prefix.lexically_normal();

    const auto root = boost::filesystem::path(path_prefix_).lexically_normal();
    auto scan_root = prefix == root ? prefix : prefix.parent_path();
    boost::system::error_code ec;
    if (!boost::filesystem::exists(scan_root, ec)) {
        if (ec &&
            ec.value() != boost::system::errc::no_such_file_or_directory) {
            ThrowInfo(
                FileReadFailed,
                fmt::format("inspect local directory {} failed, error: {}",
                            scan_root.string(),
                            ec.message()));
        }
        return {};
    }

    std::vector<std::string> files;
    boost::filesystem::recursive_directory_iterator it(scan_root, ec);
    const boost::filesystem::recursive_directory_iterator end;
    for (; it != end; it.increment(ec)) {
        if (ec) {
            ThrowInfo(FileReadFailed,
                      fmt::format("list local directory {} failed, error: {}",
                                  scan_root.string(),
                                  ec.message()));
        }
        auto path = it->path().lexically_normal();
        auto path_string = path.string();
        if (boost::filesystem::is_regular_file(path, ec) &&
            path_string.starts_with(prefix.string())) {
            files.push_back(std::move(path_string));
        }
        if (ec) {
            ThrowInfo(FileReadFailed,
                      fmt::format("inspect local path {} failed, error: {}",
                                  path.string(),
                                  ec.message()));
        }
    }
    if (ec) {
        ThrowInfo(FileReadFailed,
                  fmt::format("list local directory {} failed, error: {}",
                              scan_root.string(),
                              ec.message()));
    }
    std::sort(files.begin(), files.end());
    return files;
}

}  // namespace milvus::storage
