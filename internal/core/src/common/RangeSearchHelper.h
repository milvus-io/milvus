// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License

#pragma once

#include <string>
#include <common/Types.h>

namespace milvus {

DatasetPtr
ReGenRangeSearchResult(DatasetPtr data_set,
                       int64_t topk,
                       int64_t nq,
                       const std::string& metric_type);

void
CheckRangeSearchParam(float radius,
                      float range_filter,
                      const std::string& metric_type);

}  // namespace milvus
