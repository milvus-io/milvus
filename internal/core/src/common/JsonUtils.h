// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once
#include <string>
#include <vector>
#include "simdjson/dom.h"

namespace milvus {

// Parse a JSON Pointer into unescaped path segments
std::vector<std::string>
parse_json_pointer(const std::string& pointer);

// Check if a JSON Pointer path exists
bool
path_exists(const simdjson::dom::element& root,
            const std::vector<std::string>& tokens);
}  // namespace milvus