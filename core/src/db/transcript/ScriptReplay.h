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

#include "db/DB.h"

#include <string>

namespace milvus {
namespace engine {

class ScriptReplay {
 public:
    ScriptReplay() = default;

    Status
    Replay(const DBPtr& db, const std::string& replay_script_path);

 private:
    Status
    PerformAction(const DBPtr& db, const std::string& str_action);
};

}  // namespace engine
}  // namespace milvus
