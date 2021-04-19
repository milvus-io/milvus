#include "plan_c.h"
#include "query/Plan.h"
#include "Collection.h"

CPlan
CreatePlan(CCollection c_col, const char* dsl) {
    auto col = (milvus::segcore::Collection*)c_col;
    auto res = milvus::query::CreatePlan(*col->get_schema(), dsl);

    return (CPlan)res.release();
}

CPlaceholderGroup
ParsePlaceholderGroup(CPlan c_plan, void* placeholder_group_blob, long int blob_size) {
    std::string blob_string((char*)placeholder_group_blob, (char*)placeholder_group_blob + blob_size);
    auto plan = (milvus::query::Plan*)c_plan;
    auto res = milvus::query::ParsePlaceholderGroup(plan, blob_string);

    return (CPlaceholderGroup)res.release();
}

long int
GetNumOfQueries(CPlaceholderGroup placeholderGroup) {
    auto res = milvus::query::GetNumOfQueries((milvus::query::PlaceholderGroup*)placeholderGroup);

    return res;
}

long int
GetTopK(CPlan plan) {
    auto res = milvus::query::GetTopK((milvus::query::Plan*)plan);

    return res;
}

void
DeletePlan(CPlan cPlan) {
    auto plan = (milvus::query::Plan*)cPlan;
    delete plan;
    std::cout << "delete plan" << std::endl;
}

void
DeletePlaceholderGroup(CPlaceholderGroup cPlaceholderGroup) {
    auto placeHolderGroup = (milvus::query::PlaceholderGroup*)cPlaceholderGroup;
    delete placeHolderGroup;
    std::cout << "delete placeholder" << std::endl;
}
