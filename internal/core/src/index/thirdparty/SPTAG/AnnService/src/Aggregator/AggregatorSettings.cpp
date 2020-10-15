// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Aggregator/AggregatorSettings.h"

using namespace SPTAG;
using namespace SPTAG::Aggregator;

AggregatorSettings::AggregatorSettings()
    : m_searchTimeout(100),
      m_threadNum(8),
      m_socketThreadNum(8)
{
}
