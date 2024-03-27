// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <memory>
#include <vector>

#include "storage/DiskFileManagerImpl.h"
#include "storage/space.h"
#include "indexbuilder/MajorCompaction.h"

namespace milvus::indexbuilder {

template <typename T>
class KmeansMajorCompaction : public MajorCompaction {
 public:
    explicit KmeansMajorCompaction(
        Config& config,
        const storage::FileManagerContext& file_manager_context =
            storage::FileManagerContext());

    void
    Train() override;

    BinarySet
    Upload() override;

 private:
    std::unique_ptr<T[]>
    Sample(const std::vector<std::string>& file_paths,
           const std::vector<uint64_t>& data_sizes,
           uint64_t train_size,
           uint64_t total_size);

    std::shared_ptr<storage::DiskFileManagerImpl> file_manager_;
    Config config_;
    std::vector<std::string> result_files_;
};

template <typename T>
using KmeansMajorCompactionPtr = std::unique_ptr<KmeansMajorCompaction<T>>;
}  // namespace milvus::indexbuilder
