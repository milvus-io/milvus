// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

#pragma once

#include <SPTAG/AnnService/inc/Core/VectorIndex.h>
#include <memory>
#include <vector>

#include "knowhere/common/Config.h"
#include "knowhere/common/Dataset.h"

namespace milvus {
namespace knowhere {

std::shared_ptr<SPTAG::VectorSet>
ConvertToVectorSet(const DatasetPtr& dataset_ptr);

std::shared_ptr<SPTAG::MetadataSet>
ConvertToMetadataSet(const DatasetPtr& dataset_ptr);

std::vector<SPTAG::QueryResult>
ConvertToQueryResult(const DatasetPtr& dataset_ptr, const Config& config);

DatasetPtr
ConvertToDataset(std::vector<SPTAG::QueryResult> query_results, std::shared_ptr<std::vector<int64_t>> uid);

}  // namespace knowhere
}  // namespace milvus
