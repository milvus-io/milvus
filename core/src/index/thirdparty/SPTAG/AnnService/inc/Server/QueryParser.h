// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_SERVER_QUERYPARSER_H_
#define _SPTAG_SERVER_QUERYPARSER_H_

#include "../Core/Common.h"
#include "../Core/CommonDataStructure.h"

#include <vector>

namespace SPTAG
{
namespace Service
{


class QueryParser
{
public:
    typedef std::pair<const char*, const char*> OptionPair;

    QueryParser();

    ~QueryParser();

    ErrorCode Parse(const std::string& p_query, const char* p_vectorSeparator);

    const std::vector<const char*>& GetVectorElements() const;

    const std::vector<OptionPair>& GetOptions() const;

    const char* GetVectorBase64() const;

    SizeType GetVectorBase64Length() const;

private:
    std::vector<OptionPair> m_options;

    std::vector<const char*> m_vectorElements;

    const char* m_vectorBase64;

    SizeType m_vectorBase64Length;

    ByteArray m_dataHolder;

    static const char* c_defaultVectorSeparator;
};


} // namespace Server
} // namespace AnnService


#endif // _SPTAG_SERVER_QUERYPARSER_H_
