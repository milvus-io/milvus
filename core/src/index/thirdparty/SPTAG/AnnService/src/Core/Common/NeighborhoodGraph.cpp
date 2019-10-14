// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Core/Common/NeighborhoodGraph.h"
#include "inc/Core/Common/RelativeNeighborhoodGraph.h"

using namespace SPTAG::COMMON;

std::shared_ptr<NeighborhoodGraph> NeighborhoodGraph::CreateInstance(std::string type)
{
    std::shared_ptr<NeighborhoodGraph> res;
    if (type == "RNG")
    {
        res.reset(new RelativeNeighborhoodGraph);
    }
    return res;
}