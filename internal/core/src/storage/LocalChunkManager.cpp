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

#include <boost/filesystem.hpp>
#include <boost/system/error_code.hpp>
#include <fstream>
#include <sstream>

#include "Exception.h"

#define THROWLOCALERROR(FUNCTION)                                 \
    do {                                                          \
        std::stringstream err_msg;                                \
        err_msg << "Error:" << #FUNCTION << ":" << err.message(); \
        throw LocalChunkManagerException(err_msg.str());          \
    } while (0)

namespace milvus::storage {

bool
LocalChunkManager::Exist(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);
    boost::system::error_code err;
    bool isExist = boost::filesystem::exists(absPath, err);
    if (err && err.value() != boost::system::errc::no_such_file_or_directory) {
        THROWLOCALERROR(Exist);
    }
    return isExist;
}

uint64_t
LocalChunkManager::Size(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);

    if (!Exist(filepath)) {
        throw InvalidPathException("invalid local path:" + absPath.string());
    }
    boost::system::error_code err;
    int64_t size = boost::filesystem::file_size(absPath, err);
    if (err) {
        THROWLOCALERROR(FileSize);
    }
    return size;
}

void
LocalChunkManager::Remove(const std::string& filepath) {
    boost::filesystem::path absPath(filepath);
    boost::system::error_code err;
    boost::filesystem::remove(absPath, err);
    if (err) {
        THROWLOCALERROR(Remove);
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
        throw OpenFileException(err_msg.str());
    }

    infile.seekg(offset, std::ios::beg);
    if (!infile.read(reinterpret_cast<char*>(buf), size)) {
        if (!infile.eof()) {
            std::stringstream err_msg;
            err_msg << "Error: read local file '" << filepath << " failed, "
                    << strerror(errno);
            throw ReadFileException(err_msg.str());
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
        throw OpenFileException(err_msg.str());
    }
    if (!outfile.write(reinterpret_cast<char*>(buf), size)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << absPathStr << " failed, "
                << strerror(errno);
        throw WriteFileException(err_msg.str());
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
        throw OpenFileException(err_msg.str());
    }

    outfile.seekp(offset, std::ios::beg);
    if (!outfile.write(reinterpret_cast<char*>(buf), size)) {
        std::stringstream err_msg;
        err_msg << "Error: write local file '" << absPathStr << " failed, "
                << strerror(errno);
        throw WriteFileException(err_msg.str());
    }
}

std::vector<std::string>
LocalChunkManager::ListWithPrefix(const std::string& filepath) {
    throw NotImplementedException(GetName() + "::ListWithPrefix" +
                                  " not implement now");
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
        throw CreateFileException(err_msg.str());
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
        THROWLOCALERROR(DirExist);
    }
    return isExist;
}

void
LocalChunkManager::CreateDir(const std::string& dir) {
    bool isExist = DirExist(dir);
    if (isExist) {
        throw PathAlreadyExistException("dir:" + dir + " already exists");
    }
    boost::filesystem::path dirPath(dir);
    auto create_success = boost::filesystem::create_directories(dirPath);
    if (!create_success) {
        throw CreateFileException("create dir failed" + dir);
    }
}

void
LocalChunkManager::RemoveDir(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    boost::system::error_code err;
    boost::filesystem::remove_all(dirPath, err);
    if (err) {
        THROWLOCALERROR(RemoveDir);
    }
}

int64_t
LocalChunkManager::GetSizeOfDir(const std::string& dir) {
    boost::filesystem::path dirPath(dir);
    bool is_dir = boost::filesystem::is_directory(dirPath);
    if (!is_dir) {
        throw DirNotExistException("dir:" + dir + " not exists");
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
            total_file_size += boost::filesystem::file_size(it->path());
        }
        if (boost::filesystem::is_directory(it->path())) {
            total_file_size += GetSizeOfDir(it->path().string());
        }
    }

    return total_file_size;
}

}  // namespace milvus::storage
