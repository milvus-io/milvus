// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SOCKET_REMOTESEARCHQUERY_H_
#define _SPTAG_SOCKET_REMOTESEARCHQUERY_H_

#include "inc/Core/CommonDataStructure.h"
#include "inc/Core/SearchQuery.h"

#include <cstdint>
#include <memory>
#include <functional>
#include <vector>
#include <unordered_map>

namespace SPTAG
{
namespace Socket
{

// TODO: use Bond replace below structures.

struct RemoteQuery
{
    static constexpr std::uint16_t MajorVersion() { return 1; }
    static constexpr std::uint16_t MirrorVersion() { return 0; }

    enum class QueryType : std::uint8_t
    {
        String = 0
    };

    RemoteQuery();

    std::size_t EstimateBufferSize() const;

    std::uint8_t* Write(std::uint8_t* p_buffer) const;

    const std::uint8_t* Read(const std::uint8_t* p_buffer);


    QueryType m_type;

    std::string m_queryString;
};


struct IndexSearchResult
{
    std::string m_indexName;

    QueryResult m_results;
};


struct RemoteSearchResult
{
    static constexpr std::uint16_t MajorVersion() { return 1; }
    static constexpr std::uint16_t MirrorVersion() { return 0; }

    enum class ResultStatus : std::uint8_t
    {
        Success = 0,

        Timeout = 1,

        FailedNetwork = 2,

        FailedExecute = 3,

        Dropped = 4
    };

    RemoteSearchResult();

    RemoteSearchResult(const RemoteSearchResult& p_right);

    RemoteSearchResult(RemoteSearchResult&& p_right);

    RemoteSearchResult& operator=(RemoteSearchResult&& p_right);

    std::size_t EstimateBufferSize() const;

    std::uint8_t* Write(std::uint8_t* p_buffer) const;

    const std::uint8_t* Read(const std::uint8_t* p_buffer);


    ResultStatus m_status;

    std::vector<IndexSearchResult> m_allIndexResults;
};



} // namespace SPTAG
} // namespace Socket

#endif // _SPTAG_SOCKET_REMOTESEARCHQUERY_H_
