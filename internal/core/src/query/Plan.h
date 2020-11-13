#pragma once
#include <memory>
#include <string_view>

namespace milvus::query {
// NOTE: APIs for C wrapper

// Incomplete Definition, shouldn't be instantiated
struct Plan;
struct PlaceholderGroup;

std::unique_ptr<Plan>
CreatePlan(const std::string& dsl);

std::unique_ptr<PlaceholderGroup>
ParsePlaceholderGroup(const std::string& placeholder_group_blob);

int64_t
GetNumOfQueries(const PlaceholderGroup*);

// Query Overall TopK from Plan
// Used to alloc result memory at Go side
int64_t
GetTopK(const Plan*);

}  // namespace milvus::query

#include "PlanImpl.h"