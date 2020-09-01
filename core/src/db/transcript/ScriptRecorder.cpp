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

#include "db/transcript/ScriptRecorder.h"
#include "db/transcript/ScriptCodec.h"
#include "db/Utils.h"
#include "utils/CommonUtil.h"

#include <experimental/filesystem>

namespace milvus {
namespace engine {

ScriptRecorder&
ScriptRecorder::GetInstance() {
    static ScriptRecorder s_recorder;
    return s_recorder;
}

void
ScriptRecorder::SetScriptPath(const std::string& path) {
    std::experimental::filesystem::path script_path(path);

    std::string time_str;
    CommonUtil::GetCurrentTimeStr(time_str);
    script_path /= time_str;
    script_path_ = script_path.c_str();
}

ScriptFilePtr
ScriptRecorder::GetFile() {
    if (file_ == nullptr || file_->ExceedMaxSize(0)) {
        file_ = std::make_shared<ScriptFile>();
        int64_t current_time = utils::GetMicroSecTimeStamp();

        std::experimental::filesystem::path file_path(script_path_);
        file_path /= std::to_string(current_time);

        file_->OpenFile(file_path, ScriptFile::APPEND_WRITE);
    }

    return file_;
}

}  // namespace engine
}  // namespace milvus
