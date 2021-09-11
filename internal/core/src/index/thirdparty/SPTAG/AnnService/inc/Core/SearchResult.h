// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SEARCHRESULT_H_
#define _SPTAG_SEARCHRESULT_H_

#include "CommonDataStructure.h"

namespace SPTAG
{
    struct BasicResult
    {
        SizeType VID;
        float Dist;
        ByteArray Meta;

        BasicResult() : VID(-1), Dist(MaxDist) {}

        BasicResult(SizeType p_vid, float p_dist) : VID(p_vid), Dist(p_dist) {}

        BasicResult(SizeType p_vid, float p_dist, ByteArray p_meta) : VID(p_vid), Dist(p_dist), Meta(p_meta) {}
    };

} // namespace SPTAG

#endif // _SPTAG_SEARCHRESULT_H_
