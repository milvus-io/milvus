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
#include <vector>

#include "arrow/dataset/visibility.h"
#include "arrow/type_fwd.h"  // IWYU pragma: export

namespace arrow {

namespace fs {

class FileSystem;

}  // namespace fs

namespace dataset {

class Dataset;
class DataFragment;
class DataSource;
struct DataSelector;
using DataFragmentIterator = Iterator<std::shared_ptr<DataFragment>>;
using DataFragmentVector = std::vector<std::shared_ptr<DataFragment>>;

struct DiscoveryOptions;

class FileBasedDataFragment;
class FileFormat;
class FileScanOptions;
class FileWriteOptions;

class Filter;
using FilterVector = std::vector<std::shared_ptr<Filter>>;

class Partition;
class PartitionKey;
class PartitionScheme;
using PartitionVector = std::vector<std::shared_ptr<Partition>>;
using PartitionIterator = Iterator<std::shared_ptr<Partition>>;

struct ScanContext;
class ScanOptions;
class Scanner;
class ScannerBuilder;
class ScanTask;
using ScanTaskIterator = Iterator<std::unique_ptr<ScanTask>>;

class DatasetWriter;
class WriteContext;
class WriteOptions;

}  // namespace dataset
}  // namespace arrow
