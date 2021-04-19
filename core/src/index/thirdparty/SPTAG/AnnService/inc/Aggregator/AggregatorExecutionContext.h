// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_AGGREGATOR_AGGREGATOREXECUTIONCONTEXT_H_
#define _SPTAG_AGGREGATOR_AGGREGATOREXECUTIONCONTEXT_H_

#include "inc/Socket/RemoteSearchQuery.h"
#include "inc/Socket/Packet.h"

#include <memory>
#include <atomic>

namespace SPTAG
{
namespace Aggregator
{

typedef std::shared_ptr<Socket::RemoteSearchResult> AggregatorResult;

class AggregatorExecutionContext
{
public:
    AggregatorExecutionContext(std::size_t p_totalServerNumber,
                               Socket::PacketHeader p_requestHeader);

    ~AggregatorExecutionContext();

    std::size_t GetServerNumber() const;

    AggregatorResult& GetResult(std::size_t p_num);

    const Socket::PacketHeader& GetRequestHeader() const;

    bool IsCompletedAfterFinsh(std::uint32_t p_finishedCount);

private:
    std::atomic<std::uint32_t> m_unfinishedCount;

    std::vector<AggregatorResult> m_results;

    Socket::PacketHeader m_requestHeader;

};




} // namespace Aggregator
} // namespace AnnService


#endif // _SPTAG_AGGREGATOR_AGGREGATOREXECUTIONCONTEXT_H_

