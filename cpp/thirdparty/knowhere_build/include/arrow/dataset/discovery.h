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

/// Logic for automatically determining the structure of multi-file
/// dataset with possible partitioning according to available
/// partition schemes

#pragma once

#include <memory>
#include <string>

#include "arrow/dataset/type_fwd.h"
#include "arrow/dataset/visibility.h"
#include "arrow/util/macros.h"

namespace arrow {
namespace dataset {

struct ARROW_DS_EXPORT DiscoveryOptions {
  std::shared_ptr<FileFormat> format = NULLPTR;
  std::shared_ptr<PartitionScheme> partition_scheme = NULLPTR;
};

/// \brief Using a root directory
ARROW_DS_EXPORT
Status DiscoverSource(const std::string& path, fs::FileSystem* filesystem,
                      const DiscoveryOptions& options, std::shared_ptr<DataSource>* out);

}  // namespace dataset
}  // namespace arrow
