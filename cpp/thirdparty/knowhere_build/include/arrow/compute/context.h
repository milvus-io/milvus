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

#ifndef ARROW_COMPUTE_CONTEXT_H
#define ARROW_COMPUTE_CONTEXT_H

#include <cstdint>
#include <memory>

#include "arrow/memory_pool.h"
#include "arrow/status.h"
#include "arrow/util/macros.h"
#include "arrow/util/visibility.h"

namespace arrow {

class Buffer;

namespace internal {
class CpuInfo;
}  // namespace internal

namespace compute {

#define RETURN_IF_ERROR(ctx)                  \
  if (ARROW_PREDICT_FALSE(ctx->HasError())) { \
    Status s = ctx->status();                 \
    ctx->ResetStatus();                       \
    return s;                                 \
  }

/// \brief Container for variables and options used by function evaluation
class ARROW_EXPORT FunctionContext {
 public:
  explicit FunctionContext(MemoryPool* pool ARROW_MEMORY_POOL_DEFAULT);
  MemoryPool* memory_pool() const;

  /// \brief Allocate buffer from the context's memory pool
  Status Allocate(const int64_t nbytes, std::shared_ptr<Buffer>* out);

  /// \brief Indicate that an error has occurred, to be checked by a parent caller
  /// \param[in] status a Status instance
  ///
  /// \note Will not overwrite a prior set Status, so we will have the first
  /// error that occurred until FunctionContext::ResetStatus is called
  void SetStatus(const Status& status);

  /// \brief Clear any error status
  void ResetStatus();

  /// \brief Return true if an error has occurred
  bool HasError() const { return !status_.ok(); }

  /// \brief Return the current status of the context
  const Status& status() const { return status_; }

  internal::CpuInfo* cpu_info() const { return cpu_info_; }

 private:
  Status status_;
  MemoryPool* pool_;
  internal::CpuInfo* cpu_info_;
};

}  // namespace compute
}  // namespace arrow

#endif  // ARROW_COMPUTE_CONTEXT_H
