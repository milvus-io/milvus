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

// Contains declarations of time related Arrow builder types.

#pragma once

#include <memory>

#include "arrow/array.h"
#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"
#include "arrow/array/builder_primitive.h"
#include "arrow/buffer-builder.h"
#include "arrow/status.h"
#include "arrow/type_traits.h"
#include "arrow/util/macros.h"

namespace arrow {

class ARROW_EXPORT DayTimeIntervalBuilder : public ArrayBuilder {
 public:
  using DayMilliseconds = DayTimeIntervalType::DayMilliseconds;

  explicit DayTimeIntervalBuilder(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : DayTimeIntervalBuilder(day_time_interval(), pool) {}

  DayTimeIntervalBuilder(std::shared_ptr<DataType> type,
                         MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT)
      : ArrayBuilder(type, pool),
        builder_(fixed_size_binary(sizeof(DayMilliseconds)), pool) {}

  void Reset() override { builder_.Reset(); }
  Status Resize(int64_t capacity) override { return builder_.Resize(capacity); }
  Status Append(DayMilliseconds day_millis) {
    return builder_.Append(reinterpret_cast<uint8_t*>(&day_millis));
  }
  void UnsafeAppend(DayMilliseconds day_millis) {
    builder_.UnsafeAppend(reinterpret_cast<uint8_t*>(&day_millis));
  }
  using ArrayBuilder::UnsafeAppendNull;
  Status AppendNull() override { return builder_.AppendNull(); }
  Status AppendNulls(int64_t length) override { return builder_.AppendNulls(length); }
  Status FinishInternal(std::shared_ptr<ArrayData>* out) override {
    auto result = builder_.FinishInternal(out);
    if (*out != NULLPTR) {
      (*out)->type = type();
    }
    return result;
  }

 private:
  FixedSizeBinaryBuilder builder_;
};

}  // namespace arrow
