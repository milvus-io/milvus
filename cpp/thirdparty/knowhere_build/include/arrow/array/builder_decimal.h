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

#include "arrow/array/builder_base.h"
#include "arrow/array/builder_binary.h"

namespace arrow {

class Decimal128;

class ARROW_EXPORT Decimal128Builder : public FixedSizeBinaryBuilder {
 public:
  explicit Decimal128Builder(const std::shared_ptr<DataType>& type,
                             MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);

  using FixedSizeBinaryBuilder::Append;
  using FixedSizeBinaryBuilder::AppendValues;
  using FixedSizeBinaryBuilder::Reset;

  Status Append(Decimal128 val);
  void UnsafeAppend(Decimal128 val);
  void UnsafeAppend(util::string_view val);

  Status FinishInternal(std::shared_ptr<ArrayData>* out) override;

  /// \cond FALSE
  using ArrayBuilder::Finish;
  /// \endcond

  Status Finish(std::shared_ptr<Decimal128Array>* out) { return FinishTyped(out); }
};

using DecimalBuilder = Decimal128Builder;

}  // namespace arrow
