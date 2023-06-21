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

#include "common/Types.h"

namespace milvus {

// used for disassemble and assemble index data
const char INDEX_FILE_SLICE_META[] = "SLICE_META";
const char META[] = "meta";
const char NAME[] = "name";
const char SLICE_NUM[] = "slice_num";
const char TOTAL_LEN[] = "total_len";

std::string
GenSlicedFileName(const std::string& prefix, size_t slice_num);

void
Assemble(BinarySet& binarySet);

void
Disassemble(BinarySet& binarySet);

void
AppendSliceMeta(BinarySet& binarySet, const Config& meta_info);

BinaryPtr
EraseSliceMeta(BinarySet& binarySet);

}  // namespace milvus
