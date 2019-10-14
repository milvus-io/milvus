// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_PW_TRANSFERDATATYPE_H_
#define _SPTAG_PW_TRANSFERDATATYPE_H_

#include "inc/Core/CommonDataStructure.h"
#include "inc/Core/SearchQuery.h"
#include "inc/Socket/RemoteSearchQuery.h"

typedef SPTAG::ByteArray ByteArray;

typedef SPTAG::QueryResult QueryResult;

typedef SPTAG::Socket::RemoteSearchResult RemoteSearchResult;

class Result {
public:
    int VID;
    float Dist;
    ByteArray Meta;

    Result(int _VID, float _Dist, ByteArray _Meta): VID(_VID), Dist(_Dist), Meta(_Meta) {}
};

#endif // _SPTAG_PW_TRANSFERDATATYPE_H_
