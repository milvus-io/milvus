// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Socket/RemoteSearchQuery.h"
#include "inc/Socket/SimpleSerialization.h"

using namespace SPTAG;
using namespace SPTAG::Socket;


RemoteQuery::RemoteQuery()
    : m_type(QueryType::String)
{
}


std::size_t
RemoteQuery::EstimateBufferSize() const
{
    std::size_t sum = 0;
    sum += SimpleSerialization::EstimateBufferSize(MajorVersion());
    sum += SimpleSerialization::EstimateBufferSize(MirrorVersion());
    sum += SimpleSerialization::EstimateBufferSize(m_type);
    sum += SimpleSerialization::EstimateBufferSize(m_queryString);

    return sum;
}


std::uint8_t*
RemoteQuery::Write(std::uint8_t* p_buffer) const
{
    p_buffer = SimpleSerialization::SimpleWriteBuffer(MajorVersion(), p_buffer);
    p_buffer = SimpleSerialization::SimpleWriteBuffer(MirrorVersion(), p_buffer);

    p_buffer = SimpleSerialization::SimpleWriteBuffer(m_type, p_buffer);
    p_buffer = SimpleSerialization::SimpleWriteBuffer(m_queryString, p_buffer);

    return p_buffer;
}


const std::uint8_t*
RemoteQuery::Read(const std::uint8_t* p_buffer)
{
    decltype(MajorVersion()) majorVer = 0;
    decltype(MirrorVersion()) mirrorVer = 0;

    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, majorVer);
    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, mirrorVer);
    if (majorVer != MajorVersion())
    {
        return nullptr;
    }

    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, m_type);
    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, m_queryString);

    return p_buffer;
}


RemoteSearchResult::RemoteSearchResult()
    : m_status(ResultStatus::Timeout)
{
}


RemoteSearchResult::RemoteSearchResult(const RemoteSearchResult& p_right)
    : m_status(p_right.m_status),
      m_allIndexResults(p_right.m_allIndexResults)
{
}


RemoteSearchResult::RemoteSearchResult(RemoteSearchResult&& p_right)
    : m_status(std::move(p_right.m_status)),
      m_allIndexResults(std::move(p_right.m_allIndexResults))
{
}


RemoteSearchResult&
RemoteSearchResult::operator=(RemoteSearchResult&& p_right)
{
    m_status = p_right.m_status;
    m_allIndexResults = std::move(p_right.m_allIndexResults);

    return *this;
}


std::size_t
RemoteSearchResult::EstimateBufferSize() const
{
    std::size_t sum = 0;
    sum += SimpleSerialization::EstimateBufferSize(MajorVersion());
    sum += SimpleSerialization::EstimateBufferSize(MirrorVersion());

    sum += SimpleSerialization::EstimateBufferSize(m_status);

    sum += sizeof(std::uint32_t);
    for (const auto& indexRes : m_allIndexResults)
    {
        sum += SimpleSerialization::EstimateBufferSize(indexRes.m_indexName);
        sum += sizeof(std::uint32_t);
        sum += sizeof(bool);

        for (const auto& res : indexRes.m_results)
        {
            sum += SimpleSerialization::EstimateBufferSize(res.VID);
            sum += SimpleSerialization::EstimateBufferSize(res.Dist);
        }

        if (indexRes.m_results.WithMeta())
        {
            for (int i = 0; i < indexRes.m_results.GetResultNum(); ++i)
            {
                sum += SimpleSerialization::EstimateBufferSize(indexRes.m_results.GetMetadata(i));
            }
        }
    }

    return sum;
}


std::uint8_t*
RemoteSearchResult::Write(std::uint8_t* p_buffer) const
{
    p_buffer = SimpleSerialization::SimpleWriteBuffer(MajorVersion(), p_buffer);
    p_buffer = SimpleSerialization::SimpleWriteBuffer(MirrorVersion(), p_buffer);

    p_buffer = SimpleSerialization::SimpleWriteBuffer(m_status, p_buffer);
    p_buffer = SimpleSerialization::SimpleWriteBuffer(static_cast<std::uint32_t>(m_allIndexResults.size()), p_buffer);
    for (const auto& indexRes : m_allIndexResults)
    {
        p_buffer = SimpleSerialization::SimpleWriteBuffer(indexRes.m_indexName, p_buffer);

        p_buffer = SimpleSerialization::SimpleWriteBuffer(static_cast<std::uint32_t>(indexRes.m_results.GetResultNum()), p_buffer);
        p_buffer = SimpleSerialization::SimpleWriteBuffer(indexRes.m_results.WithMeta(), p_buffer);

        for (const auto& res : indexRes.m_results)
        {
            p_buffer = SimpleSerialization::SimpleWriteBuffer(res.VID, p_buffer);
            p_buffer = SimpleSerialization::SimpleWriteBuffer(res.Dist, p_buffer);
        }

        if (indexRes.m_results.WithMeta())
        {
            for (int i = 0; i < indexRes.m_results.GetResultNum(); ++i)
            {
                p_buffer = SimpleSerialization::SimpleWriteBuffer(indexRes.m_results.GetMetadata(i), p_buffer);
            }
        }
    }

    return p_buffer;
}


const std::uint8_t*
RemoteSearchResult::Read(const std::uint8_t* p_buffer)
{
    decltype(MajorVersion()) majorVer = 0;
    decltype(MirrorVersion()) mirrorVer = 0;

    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, majorVer);
    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, mirrorVer);
    if (majorVer != MajorVersion())
    {
        return nullptr;
    }

    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, m_status);

    std::uint32_t len = 0;
    p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, len);
    m_allIndexResults.resize(len);

    for (auto& indexRes : m_allIndexResults)
    {
        p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, indexRes.m_indexName);

        std::uint32_t resNum = 0;
        p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, resNum);

        bool withMeta = false;
        p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, withMeta);

        indexRes.m_results.Init(nullptr, resNum, withMeta);
        for (auto& res : indexRes.m_results)
        {
            p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, res.VID);
            p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, res.Dist);
        }

        if (withMeta)
        {
            for (int i = 0; i < indexRes.m_results.GetResultNum(); ++i)
            {
                ByteArray meta;
                p_buffer = SimpleSerialization::SimpleReadBuffer(p_buffer, meta);
                indexRes.m_results.SetMetadata(i, std::move(meta));
            }
        }
    }

    return p_buffer;
}
