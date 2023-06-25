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

int64_t FILE_SLICE_SIZE = DEFAULT_INDEX_FILE_SLICE_SIZE;
int64_t THREAD_CORE_COEFFICIENT = DEFAULT_THREAD_CORE_COEFFICIENT;
int CPU_NUM = DEFAULT_CPU_NUM;

void
SetIndexSliceSize(const int64_t size) {
    FILE_SLICE_SIZE = size << 20;
    LOG_SEGCORE_DEBUG_ << "set config index slice size (byte): "
                       << FILE_SLICE_SIZE;
}

void
SetThreadCoreCoefficient(const int64_t coefficient) {
    THREAD_CORE_COEFFICIENT = coefficient;
    LOG_SEGCORE_DEBUG_ << "set thread pool core coefficient: "
                       << THREAD_CORE_COEFFICIENT;
}

void
SetCpuNum(const int num) {
    CPU_NUM = num;
}

}  // namespace milvus
