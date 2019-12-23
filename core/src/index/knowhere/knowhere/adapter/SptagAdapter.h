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

#include <SPTAG/AnnService/inc/Core/VectorIndex.h>
#include <memory>
#include <vector>

#include "knowhere/common/Config.h"
#include "knowhere/common/Dataset.h"

namespace knowhere {

std::shared_ptr<SPTAG::VectorSet>
ConvertToVectorSet(const DatasetPtr& dataset);

std::shared_ptr<SPTAG::MetadataSet>
ConvertToMetadataSet(const DatasetPtr& dataset);

std::vector<SPTAG::QueryResult>
ConvertToQueryResult(const DatasetPtr& dataset, const Config& config);

DatasetPtr
ConvertToDataset(std::vector<SPTAG::QueryResult> query_results);

}  // namespace knowhere
