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

#include <mutex>

#include "knowhere/index/vector_index/helpers/KDTParameterMgr.h"

namespace zilliz {
namespace knowhere {

const std::vector<KDTParameter>&
KDTParameterMgr::GetKDTParameters() {
    return kdt_parameters_;
}

KDTParameterMgr::KDTParameterMgr() {
    kdt_parameters_ = std::vector<KDTParameter>{
        {"KDTNumber", "1"},
        {"NumTopDimensionKDTSplit", "5"},
        {"NumSamplesKDTSplitConsideration", "100"},

        {"TPTNumber", "1"},
        {"TPTLeafSize", "2000"},
        {"NumTopDimensionTPTSplit", "5"},

        {"NeighborhoodSize", "32"},
        {"GraphNeighborhoodScale", "2"},
        {"GraphCEFScale", "2"},
        {"RefineIterations", "0"},
        {"CEF", "1000"},
        {"MaxCheckForRefineGraph", "10000"},

        {"NumberOfThreads", "1"},

        {"MaxCheck", "8192"},
        {"ThresholdOfNumberOfContinuousNoBetterPropagation", "3"},
        {"NumberOfInitialDynamicPivots", "50"},
        {"NumberOfOtherDynamicPivots", "4"},
    };
}

}  // namespace knowhere
}  // namespace zilliz
