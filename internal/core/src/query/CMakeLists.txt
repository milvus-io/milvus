# Copyright (C) 2019-2020 Zilliz. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License
# is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
# or implied. See the License for the specific language governing permissions and limitations under the License

set(MILVUS_QUERY_SRCS
        deprecated/BinaryQuery.cpp
        generated/PlanNode.cpp
        generated/Expr.cpp
        visitors/ShowPlanNodeVisitor.cpp
        visitors/ShowExprVisitor.cpp
        visitors/ExecPlanNodeVisitor.cpp
        visitors/ExecExprVisitor.cpp
        visitors/VerifyPlanNodeVisitor.cpp
        visitors/VerifyExprVisitor.cpp
        visitors/ExtractInfoPlanNodeVisitor.cpp
        visitors/ExtractInfoExprVisitor.cpp
        Plan.cpp
        SearchOnGrowing.cpp
        SearchOnSealed.cpp
        SearchOnIndex.cpp
        SearchBruteForce.cpp
        SubSearchResult.cpp
        PlanProto.cpp
        )
add_library(milvus_query ${MILVUS_QUERY_SRCS})
if(USE_DYNAMIC_SIMD)
        target_link_libraries(milvus_query milvus_index milvus_simd)
else() 
        target_link_libraries(milvus_query milvus_index)
endif()
