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

#include <vector>
#include <string>

#include "storage/hdfs/hdfs.h"

namespace milvus {
namespace storage {

// try to use singleton in 9/9/2020
class HDFSClient {
 public:
    static HDFSClient&
    getInstance() {
        static HDFSClient instance;
        return instance;
    }

    ~HDFSClient();  // disconnect
    HDFSClient(const HDFSClient&) = delete;
    HDFSClient(HDFSClient&&) = delete;
    HDFSClient&
    operator=(const HDFSClient&) = delete;
    HDFSClient&
    operator=(HDFSClient&&) = delete;

    bool
    read_open(const char* name);

    bool
    write_open(const char* name);

    void
    write(char* ptr, int64_t size);

    void
    read(char* ptr, int64_t size);

    void
    seekg(int64_t pos);

    int64_t
    length(std::string name);

    void
    close();

    void
    CreateDirectory(char* path);

    void
    ListDirectory(std::vector<std::string>& file_paths, std::string dir_path);

    bool
    DeleteFile(const std::string& file_path);

 private:
    HDFSClient();
    struct hdfsBuilder* bld_;
    hdfsFS hdfs_fs_;
    hdfsFile hdfs_file_;
};

}  // namespace storage
}  // namespace milvus
