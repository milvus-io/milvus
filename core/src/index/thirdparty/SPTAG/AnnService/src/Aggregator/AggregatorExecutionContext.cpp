// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Aggregator/AggregatorExecutionContext.h"

using namespace SPTAG;
using namespace SPTAG::Aggregator;

AggregatorExecutionContext::AggregatorExecutionContext(std::size_t p_totalServerNumber,
                                                       Socket::PacketHeader p_requestHeader)
    : m_requestHeader(std::move(p_requestHeader))
{
    m_results.clear();
    m_results.resize(p_totalServerNumber);

    m_unfinishedCount = static_cast<std::uint32_t>(p_totalServerNumber);
}


AggregatorExecutionContext::~AggregatorExecutionContext()
{
}


std::size_t
AggregatorExecutionContext::GetServerNumber() const
{
    return m_results.size();
}


AggregatorResult&
AggregatorExecutionContext::GetResult(std::size_t p_num)
{
    return m_results[p_num];
}


const Socket::PacketHeader&
AggregatorExecutionContext::GetRequestHeader() const
{
    return m_requestHeader;
}


bool
AggregatorExecutionContext::IsCompletedAfterFinsh(std::uint32_t p_finishedCount)
{
    auto lastCount = m_unfinishedCount.fetch_sub(p_finishedCount);
    return lastCount <= p_finishedCount;
}
