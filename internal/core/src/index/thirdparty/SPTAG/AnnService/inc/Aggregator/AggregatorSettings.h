// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_AGGREGATOR_AGGREGATORSETTINGS_H_
#define _SPTAG_AGGREGATOR_AGGREGATORSETTINGS_H_

#include "../Core/Common.h"

#include <string>

namespace SPTAG
{
namespace Aggregator
{

struct AggregatorSettings
{
    AggregatorSettings();

    std::string m_listenAddr;

    std::string m_listenPort;

    std::uint32_t m_searchTimeout;

    SizeType m_threadNum;

    SizeType m_socketThreadNum;
};




} // namespace Aggregator
} // namespace AnnService


#endif // _SPTAG_AGGREGATOR_AGGREGATORSETTINGS_H_

