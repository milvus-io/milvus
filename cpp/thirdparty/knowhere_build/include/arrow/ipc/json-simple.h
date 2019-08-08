// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Implement a simple JSON representation format for arrays

#ifndef ARROW_IPC_JSON_SIMPLE_H
#define ARROW_IPC_JSON_SIMPLE_H

#include <memory>
#include <string>

#include "arrow/status.h"
#include "arrow/util/string_view.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Array;
class DataType;

namespace ipc {
namespace internal {
namespace json {

ARROW_EXPORT
Status ArrayFromJSON(const std::shared_ptr<DataType>&, const std::string& json,
                     std::shared_ptr<Array>* out);

ARROW_EXPORT
Status ArrayFromJSON(const std::shared_ptr<DataType>&, const util::string_view& json,
                     std::shared_ptr<Array>* out);

ARROW_EXPORT
Status ArrayFromJSON(const std::shared_ptr<DataType>&, const char* json,
                     std::shared_ptr<Array>* out);

}  // namespace json
}  // namespace internal
}  // namespace ipc
}  // namespace arrow

#endif  // ARROW_IPC_JSON_SIMPLE_H
