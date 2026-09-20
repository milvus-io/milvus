// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not use this file
// except in compliance with the License. You may obtain a copy of the License
// at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cstdint>
#include <string_view>

namespace milvus::index {

// The three input states the legacy JSON NGRAM build path distinguishes.
// `value` is borrowed for the complete IArtifactBuilder::Build call.
enum class JsonProjectedStringState : uint8_t {
    FieldNull,
    NoValue,
    Value,
};

struct JsonProjectedString {
    std::string_view value;
    JsonProjectedStringState state{JsonProjectedStringState::NoValue};
};

}  // namespace milvus::index
