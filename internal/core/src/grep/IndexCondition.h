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

#pragma once

#include <string_view>

#include "common/RegexQuery.h"

namespace milvus::grep {

// Decode the trusted Rust FFI payload once before calling an index reader.
// Rust owns size/depth limits; C++ checks parsing and tree structure.
// Returns an owning tree, independent of the payload and the Rust plan.
// Transport/structure errors throw; planning budgets are not codec limits.
RegexLiteralCondition
DecodeLiteralConditionTree(std::string_view payload);

}  // namespace milvus::grep
