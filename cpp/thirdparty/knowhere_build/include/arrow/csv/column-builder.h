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

#ifndef ARROW_CSV_COLUMN_BUILDER_H
#define ARROW_CSV_COLUMN_BUILDER_H

#include <cstdint>
#include <memory>

#include "arrow/array.h"
#include "arrow/status.h"
#include "arrow/util/visibility.h"

namespace arrow {

class ChunkedArray;
class DataType;

namespace internal {

class TaskGroup;

}  // namespace internal

namespace csv {

class BlockParser;
struct ConvertOptions;

class ARROW_EXPORT ColumnBuilder {
 public:
  virtual ~ColumnBuilder() = default;

  /// Spawn a task that will try to convert and append the given CSV block.
  /// All calls to Append() should happen on the same thread, otherwise
  /// call Insert() instead.
  virtual void Append(const std::shared_ptr<BlockParser>& parser);

  /// Spawn a task that will try to convert and insert the given CSV block
  virtual void Insert(int64_t block_index,
                      const std::shared_ptr<BlockParser>& parser) = 0;

  /// Return the final chunked array.  The TaskGroup _must_ have finished!
  virtual Status Finish(std::shared_ptr<ChunkedArray>* out) = 0;

  /// Change the task group.  The previous TaskGroup _must_ have finished!
  void SetTaskGroup(const std::shared_ptr<internal::TaskGroup>& task_group);

  std::shared_ptr<internal::TaskGroup> task_group() { return task_group_; }

  /// Construct a strictly-typed ColumnBuilder.
  static Status Make(const std::shared_ptr<DataType>& type, int32_t col_index,
                     const ConvertOptions& options,
                     const std::shared_ptr<internal::TaskGroup>& task_group,
                     std::shared_ptr<ColumnBuilder>* out);

  /// Construct a type-inferring ColumnBuilder.
  static Status Make(int32_t col_index, const ConvertOptions& options,
                     const std::shared_ptr<internal::TaskGroup>& task_group,
                     std::shared_ptr<ColumnBuilder>* out);

 protected:
  explicit ColumnBuilder(const std::shared_ptr<internal::TaskGroup>& task_group)
      : task_group_(task_group) {}

  std::shared_ptr<internal::TaskGroup> task_group_;
  ArrayVector chunks_;
};

}  // namespace csv
}  // namespace arrow

#endif  // ARROW_CSV_COLUMN_BUILDER_H
