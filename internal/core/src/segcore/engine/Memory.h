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

#include <memory>
#include <string>

#include <velox/common/memory/Memory.h>

#ifdef CHECK_STRNE
#undef CHECK_STRNE
#endif
#ifdef CHECK_STREQ
#undef CHECK_STREQ
#endif
#ifdef DCHECK_STRNE
#undef DCHECK_STRNE
#endif
#ifdef DCHECK_STREQ
#undef DCHECK_STREQ
#endif

namespace milvus::engine {

std::shared_ptr<facebook::velox::memory::MemoryPool>
GetDefaultMemoryPool();

}  // namespace milvus::engine
