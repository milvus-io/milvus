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

#pragma once

#include <memory>

#include "arrow/array/builder_adaptive.h"   // IWYU pragma: export
#include "arrow/array/builder_base.h"       // IWYU pragma: export
#include "arrow/array/builder_binary.h"     // IWYU pragma: export
#include "arrow/array/builder_decimal.h"    // IWYU pragma: export
#include "arrow/array/builder_dict.h"       // IWYU pragma: export
#include "arrow/array/builder_nested.h"     // IWYU pragma: export
#include "arrow/array/builder_primitive.h"  // IWYU pragma: export
#include "arrow/array/builder_time.h"       // IWYU pragma: export
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class DataType;
class MemoryPool;

/// \brief Construct an empty ArrayBuilder corresponding to the data
/// type
/// \param[in] pool the MemoryPool to use for allocations
/// \param[in] type an instance of DictionaryType
/// \param[out] out the created ArrayBuilder
ARROW_EXPORT
Status MakeBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                   std::unique_ptr<ArrayBuilder>* out);

/// \brief Construct an empty DictionaryBuilder initialized optionally
/// with a pre-existing dictionary
/// \param[in] pool the MemoryPool to use for allocations
/// \param[in] type an instance of DictionaryType
/// \param[in] dictionary the initial dictionary, if any. May be nullptr
/// \param[out] out the created ArrayBuilder
ARROW_EXPORT
Status MakeDictionaryBuilder(MemoryPool* pool, const std::shared_ptr<DataType>& type,
                             const std::shared_ptr<Array>& dictionary,
                             std::unique_ptr<ArrayBuilder>* out);

}  // namespace arrow
