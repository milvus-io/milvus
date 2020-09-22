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

#include <memory>
#include <string>
#include "HDFSClient.h"
#include "core/thirdparty/hdfs/hdfs.h"
#include "storage/IOReader.h"

namespace milvus {
namespace storage {

class HDFSIOReader : public IOReader {
 public:
    HDFSIOReader() = default;
    ~HDFSIOReader() = default;
    HDFSIOReader(const HDFSIOReader&) = delete;
    HDFSIOReader(HDFSIOReader&&) = delete;

    HDFSIOReader&
    operator=(const HDFSIOReader&) = delete;
    HDFSIOReader&
    operator=(HDFSIOReader&&) = delete;

    bool
    Open(const std::string& name) override;

    // read data from hdfs to the pointed buffer
    void
    Read(void* ptr, int64_t size) override;

    void Seekg(int64_t) override;

    int64_t
    Length() override;

    void
    Close() override;

 public:
    std::string name_;
};

using HDFSIOReaderPtr = std::shared_ptr<HDFSIOReader>;

}  // namespace storage
}  // namespace milvus
