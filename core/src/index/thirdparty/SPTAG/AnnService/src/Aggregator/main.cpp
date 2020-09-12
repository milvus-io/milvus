// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Aggregator/AggregatorService.h"

SPTAG::Aggregator::AggregatorService g_service;

int main(int argc, char* argv[])
{
    if (!g_service.Initialize())
    {
        return 1;
    }

    g_service.Run();

    return 0;
}

