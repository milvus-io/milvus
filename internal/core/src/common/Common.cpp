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

#include "common/Common.h"
#include "log/Log.h"

namespace milvus {

int64_t index_file_slice_size = DEFAULT_INDEX_FILE_SLICE_SIZE;
int64_t thread_core_coefficient = DEFAULT_THREAD_CORE_COEFFICIENT;

void
SetIndexSliceSize(const int64_t size) {
    index_file_slice_size = size;
    LOG_SEGCORE_DEBUG_ << "set config index slice size: " << index_file_slice_size;
}

void
SetThreadCoreCoefficient(const int64_t coefficient) {
    thread_core_coefficient = coefficient;
    LOG_SEGCORE_DEBUG_ << "set thread pool core coefficient: " << thread_core_coefficient;
}

}  // namespace milvus
