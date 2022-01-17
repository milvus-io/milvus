// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Server/SearchService.h"

SPTAG::Service::SearchService g_service;

int main(int argc, char* argv[])
{
    if (!g_service.Initialize(argc, argv))
    {
        return 1;
    }

    g_service.Run();

    return 0;
}

