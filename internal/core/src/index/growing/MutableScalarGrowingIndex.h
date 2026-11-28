// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>

#include "index/contracts/growing/IGrowingIndex.h"

namespace milvus::index {

// Construction is the only place that knows the backend. Segment and Expr use
// the existing growing and reader contracts. Accounting stays with storage,
// including allocations retained by older pins; the index reports it through
// MemoryUsage() without charging it again.
std::unique_ptr<IGrowingIndex>
CreateMutableScalarGrowingIndex(DataType type);

}  // namespace milvus::index
