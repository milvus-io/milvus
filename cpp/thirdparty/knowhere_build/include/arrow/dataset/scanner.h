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
#include <string>
#include <vector>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"

namespace arrow {
namespace dataset {

/// \brief Shared state for a Scan operation
struct ARROW_DS_EXPORT ScanContext {};

// TODO(wesm): API for handling of post-materialization filters. For
// example, if the user requests [$col1 > 0, $col2 > 0] and $col1 is a
// partition key, but $col2 is not, then the filter "$col2 > 0" must
// be evaluated in-memory against the RecordBatch objects resulting
// from the Scan

class ARROW_DS_EXPORT ScanOptions {
 public:
  virtual ~ScanOptions() = default;
};

/// \brief Read record batches from a range of a single data fragment
class ARROW_DS_EXPORT ScanTask {
 public:
  virtual ~ScanTask() = default;

  /// \brief Iterate through sequence of materialized record batches
  /// resulting from the Scan. Execution semantics encapsulated in the
  /// particular ScanTask implementation
  virtual std::unique_ptr<RecordBatchIterator> Scan() = 0;
};

/// \brief Main interface for
class ARROW_DS_EXPORT Scanner {
 public:
  virtual ~Scanner() = default;

  /// \brief Return iterator yielding ScanTask instances to enable
  /// serial or parallel execution of units of scanning work
  virtual std::unique_ptr<ScanTaskIterator> GetTasks() = 0;
};

class ARROW_DS_EXPORT ScannerBuilder {
 public:
  ScannerBuilder(std::shared_ptr<Dataset> dataset,
                 std::shared_ptr<ScanContext> scan_context);

  /// \brief Set
  ScannerBuilder* Project(const std::vector<std::string>& columns);

  ScannerBuilder* AddFilter(const std::shared_ptr<Filter>& filter);

  ScannerBuilder* SetGlobalFileOptions(std::shared_ptr<FileScanOptions> options);

  /// \brief If true (default), add partition keys to the
  /// RecordBatches that the scan produces if they are not in the data
  /// otherwise
  ScannerBuilder* IncludePartitionKeys(bool include = true);

  /// \brief Return the constructed now-immutable Scanner object
  std::unique_ptr<Scanner> Finish() const;

 private:
  std::shared_ptr<Dataset> dataset_;
  std::shared_ptr<ScanContext> scan_context_;
  std::vector<std::string> project_columns_;
  FilterVector filters_;
  bool include_partition_keys_;
};

}  // namespace dataset
}  // namespace arrow
