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

#include "segcore/engine/Memory.h"

namespace milvus::engine {

using MemoryPool = facebook::velox::memory::MemoryPool;

std::shared_ptr<facebook::velox::memory::MemoryPool>
GetDefaultMemoryPool() {
    static std::shared_ptr<facebook::velox::memory::MemoryPool> memoryPool =
        facebook::velox::memory::getDefaultMemoryPool();
    return memoryPool;
}

}  // namespace milvus::engine
