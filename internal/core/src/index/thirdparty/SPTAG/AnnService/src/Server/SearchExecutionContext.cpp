// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Server/SearchExecutionContext.h"
#include "inc/Helper/StringConvert.h"
#include "inc/Helper/CommonHelper.h"
#include "inc/Helper/Base64Encode.h"

using namespace SPTAG;
using namespace SPTAG::Service;

namespace
{
namespace Local
{

template<typename ValueType>
ErrorCode
ConvertVectorFromString(const std::vector<const char*>& p_source, ByteArray& p_dest, SizeType& p_dimension)
{
    p_dimension = 0;
    p_dest = ByteArray::Alloc(p_source.size() * sizeof(ValueType));
    ValueType* arr = reinterpret_cast<ValueType*>(p_dest.Data());
    for (std::size_t i = 0; i < p_source.size(); ++i)
    {
        if (!Helper::Convert::ConvertStringTo<ValueType>(p_source[i], arr[i]))
        {
            p_dest.Clear();
            p_dimension = 0;
            return ErrorCode::Fail;
        }

        ++p_dimension;
    }

    return ErrorCode::Success;
}

}
}


SearchExecutionContext::SearchExecutionContext(const std::shared_ptr<const ServiceSettings>& p_serviceSettings)
    : c_serviceSettings(p_serviceSettings),
      m_vectorDimension(0),
      m_inputValueType(VectorValueType::Undefined),
      m_extractMetadata(false),
      m_resultNum(p_serviceSettings->m_defaultMaxResultNumber)
{
}


SearchExecutionContext::~SearchExecutionContext()
{
    m_results.clear();
}


ErrorCode
SearchExecutionContext::ParseQuery(const std::string& p_query)
{
    return m_queryParser.Parse(p_query, c_serviceSettings->m_vectorSeparator.c_str());
}


ErrorCode
SearchExecutionContext::ExtractOption()
{
    for (const auto& optionPair : m_queryParser.GetOptions())
    {
        if (Helper::StrUtils::StrEqualIgnoreCase(optionPair.first, "indexname"))
        {
            const char* begin = optionPair.second;
            const char* end = optionPair.second;
            while (*end != '\0')
            {
                while (*end != '\0' && *end != ',')
                {
                    ++end;
                }

                if (end != begin)
                {
                    m_indexNames.emplace_back(begin, end - begin);
                }

                if (*end != '\0')
                {
                    ++end;
                    begin = end;
                }
            }
        }
        else if (Helper::StrUtils::StrEqualIgnoreCase(optionPair.first, "datatype"))
        {
            Helper::Convert::ConvertStringTo<VectorValueType>(optionPair.second, m_inputValueType);
        }
        else if (Helper::StrUtils::StrEqualIgnoreCase(optionPair.first, "extractmetadata"))
        {
            Helper::Convert::ConvertStringTo<bool>(optionPair.second, m_extractMetadata);
        }
        else if (Helper::StrUtils::StrEqualIgnoreCase(optionPair.first, "resultnum"))
        {
            Helper::Convert::ConvertStringTo<SizeType>(optionPair.second, m_resultNum);
        }
    }

    return ErrorCode::Success;
}


ErrorCode
SearchExecutionContext::ExtractVector(VectorValueType p_targetType)
{
    if (!m_queryParser.GetVectorElements().empty())
    {
        switch (p_targetType)
        {
#define DefineVectorValueType(Name, Type) \
        case VectorValueType::Name: \
            return Local::ConvertVectorFromString<Type>( \
                        m_queryParser.GetVectorElements(), m_vector, m_vectorDimension); \
            break; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

        default:
            break;
        }
    }
    else if (m_queryParser.GetVectorBase64() != nullptr
             && m_queryParser.GetVectorBase64Length() != 0)
    {
        SizeType estLen = m_queryParser.GetVectorBase64Length();
        auto temp = ByteArray::Alloc(Helper::Base64::CapacityForDecode(estLen));
        std::size_t outLen = 0;
        if (!Helper::Base64::Decode(m_queryParser.GetVectorBase64(), estLen, temp.Data(), outLen))
        {
            return ErrorCode::Fail;
        }

        if (outLen % GetValueTypeSize(p_targetType) != 0)
        {
            return ErrorCode::Fail;
        }

        m_vectorDimension = outLen / GetValueTypeSize(p_targetType);
        m_vector = ByteArray(temp.Data(), outLen, temp.DataHolder());

        return ErrorCode::Success;
    }

    return ErrorCode::Fail;
}


const std::vector<std::string>&
SearchExecutionContext::GetSelectedIndexNames() const
{
    return m_indexNames;
}


void
SearchExecutionContext::AddResults(std::string p_indexName, QueryResult& p_results)
{
    m_results.emplace_back();
    m_results.back().m_indexName.swap(p_indexName);
    m_results.back().m_results = p_results;
}


std::vector<SearchResult>&
SearchExecutionContext::GetResults()
{
    return m_results;
}


const std::vector<SearchResult>&
SearchExecutionContext::GetResults() const
{
    return m_results;
}


const ByteArray&
SearchExecutionContext::GetVector() const
{
    return m_vector;
}


const SizeType
SearchExecutionContext::GetVectorDimension() const
{
    return m_vectorDimension;
}


const std::vector<QueryParser::OptionPair>&
SearchExecutionContext::GetOptions() const
{
    return m_queryParser.GetOptions();
}


const SizeType
SearchExecutionContext::GetResultNum() const
{
    return m_resultNum;
}


const bool
SearchExecutionContext::GetExtractMetadata() const
{
    return m_extractMetadata;
}
