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

#include "db/transcript/ScriptFile.h"
#include "db/Types.h"

#include <limits>

namespace milvus {
namespace engine {


ScriptFile::~ScriptFile() {
    CloseFile();
}

Status
ScriptFile::OpenWrite(const std::string& path) {
    CloseFile();

    return Status::OK();
}

Status
ScriptFile::OpenRead(const std::string& path) {
    CloseFile();


    return Status::OK();
}

Status
ScriptFile::CloseFile() {


    return Status::OK();
}

Status
ScriptFile::WriteLine(const std::string& str) {
    return Status::OK();
}

Status
ScriptFile::ReadLine(std::string& str) {
    return Status::OK();
}

bool
ScriptFile::ExceedMaxSize(int64_t append_size) {
    return (file_size_ + append_size) > MAX_SCRIPT_FILE_SIZE;
}

}  // namespace engine
}  // namespace milvus
