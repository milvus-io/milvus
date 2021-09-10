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
#include <string_view>
#include <string>
#include "common/Schema.h"
#include "pb/plan.pb.h"
#include "pb/segcore.pb.h"

namespace milvus::query {
// NOTE: APIs for C wrapper

// Incomplete Definition, shouldn't be instantiated
struct Plan;
struct PlaceholderGroup;
struct RetrievePlan;

std::unique_ptr<Plan>
CreatePlan(const Schema& schema, const std::string& dsl);

// Note: serialized_expr_plan is of binary format
std::unique_ptr<Plan>
CreatePlanByExpr(const Schema& schema, const char* serialized_expr_plan, int64_t size);

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const Plan* plan, const std::string& placeholder_group_blob);

int64_t
GetNumOfQueries(const PlaceholderGroup*);

// std::unique_ptr<RetrievePlan>
// CreateRetrievePlan(const Schema& schema, proto::segcore::RetrieveRequest&& request);

std::unique_ptr<RetrievePlan>
CreateRetrievePlanByExpr(const Schema& schema, const char* serialized_expr_plan, int size);

// Query Overall TopK from Plan
// Used to alloc result memory at Go side
int64_t
GetTopK(const Plan*);

}  // namespace milvus::query

#include "PlanImpl.h"
