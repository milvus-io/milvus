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

#include <memory>
#include <string>

#include "PlanImpl.h"
#include "common/Schema.h"
#include "pb/plan.pb.h"
#include "pb/segcore.pb.h"

namespace milvus::query {

// Incomplete Definition, shouldn't be instantiated
struct Plan;
struct PlaceholderGroup;
struct RetrievePlan;

// Note: serialized_expr_plan is of binary format
std::unique_ptr<Plan>
CreateSearchPlanByExpr(const Schema& schema,
                       const void* serialized_expr_plan,
                       const int64_t size);

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan,
                      const uint8_t* blob,
                      const int64_t blob_len);

// deprecated
std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan,
                      const std::string_view placeholder_group_blob);

int64_t
GetNumOfQueries(const PlaceholderGroup*);

std::unique_ptr<RetrievePlan>
CreateRetrievePlanByExpr(const Schema& schema,
                         const void* serialized_expr_plan,
                         const int64_t size);

// Query Overall TopK from Plan
// Used to alloc result memory at Go side
int64_t
GetTopK(const Plan*);

int64_t
GetFieldID(const Plan* plan);

}  // namespace milvus::query
