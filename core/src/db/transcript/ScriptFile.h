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

#pragma once

#include "db/Types.h"
#include "utils/Status.h"

#include <fstream>
#include <memory>
#include <ostream>
#include <string>

namespace milvus {
namespace engine {

class ScriptFile {
 public:
    ScriptFile() = default;
    virtual ~ScriptFile();

    Status
    OpenWrite(const std::string& path);

    Status
    OpenRead(const std::string& path);

    Status
    WriteLine(const std::string& str);

    bool
    ReadLine(std::string& str);

    bool
    ExceedMaxSize(int64_t append_size);

 private:
    Status
    CloseFile();

 private:
    std::ifstream reader_;
    std::ofstream writer_;

    int64_t file_size_ = 0;
};

using ScriptFilePtr = std::shared_ptr<ScriptFile>;

}  // namespace engine
}  // namespace milvus
