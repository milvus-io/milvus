// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Server/QueryParser.h"

#include <cctype>
#include <cstring>

using namespace SPTAG;
using namespace SPTAG::Service;


const char* QueryParser::c_defaultVectorSeparator = "|";


QueryParser::QueryParser()
    : m_vectorBase64(nullptr),
      m_vectorBase64Length(0)
{
}


QueryParser::~QueryParser()
{
}


ErrorCode
QueryParser::Parse(const std::string& p_query, const char* p_vectorSeparator)
{
    if (p_vectorSeparator == nullptr)
    {
        p_vectorSeparator = c_defaultVectorSeparator;
    }

    m_vectorElements.clear();
    m_options.clear();

    m_dataHolder = ByteArray::Alloc(p_query.size() + 1);
    memcpy(m_dataHolder.Data(), p_query.c_str(), p_query.size() + 1);

    enum class State : uint8_t
    {
        OptionNameBegin,
        OptionName,
        OptionValueBegin,
        OptionValue,
        Vector,
        VectorBase64,
        None
    };

    State currState = State::None;

    char* optionName = nullptr;
    char* vectorStrBegin = nullptr;
    char* vectorStrEnd = nullptr;
    SizeType estDimension = 0;

    char* iter = nullptr;

    for (iter = reinterpret_cast<char*>(m_dataHolder.Data()); *iter != '\0'; ++iter)
    {
        if (std::isspace(*iter))
        {
            *iter = '\0';
            if (State::Vector == currState)
            {
                ++estDimension;
                vectorStrEnd = iter;
            }
            else if (State::VectorBase64 == currState)
            {
                m_vectorBase64Length = iter - m_vectorBase64;
            }

            currState = State::None;
            continue;
        }

        switch (currState)
        {
        case State::None:
            if ('$' == *iter)
            {
                currState = State::OptionNameBegin;
            }
            else if ('#' == *iter)
            {
                currState = State::VectorBase64;
                m_vectorBase64 = iter + 1;
            }
            else
            {
                currState = State::Vector;
                vectorStrBegin = iter;
            }

            break;

        case State::OptionNameBegin:
            optionName = iter;
            currState = State::OptionName;
            break;

        case State::OptionName:
            if (':' == *iter || '=' == *iter)
            {
                *iter = '\0';
                currState = State::OptionValueBegin;
            }
            else if (std::isupper(*iter))
            {
                // Convert OptionName to lowercase.
                *iter = (*iter) | 0x20;
            }

            break;

        case State::OptionValueBegin:
            currState = State::OptionValue;
            m_options.emplace_back(optionName, iter);
            break;

        case State::Vector:
            if (std::strchr(p_vectorSeparator, *iter) != nullptr)
            {
                ++estDimension;
                *iter = '\0';
            }

            break;

        default:
            break;
        }
    }

    if (State::Vector == currState)
    {
        ++estDimension;
        vectorStrEnd = iter;
    }
    else if (State::VectorBase64 == currState)
    {
        m_vectorBase64Length = iter - m_vectorBase64;
    }

    if (vectorStrBegin == nullptr || 0 == estDimension)
    {
        return ErrorCode::Fail;
    }

    m_vectorElements.reserve(estDimension);
    while (vectorStrBegin < vectorStrEnd)
    {
        while (vectorStrBegin < vectorStrEnd && '\0' == *vectorStrBegin)
        {
            ++vectorStrBegin;
        }

        if (vectorStrBegin >= vectorStrEnd)
        {
            break;
        }

        m_vectorElements.push_back(vectorStrBegin);

        while (vectorStrBegin < vectorStrEnd && '\0' != *vectorStrBegin)
        {
            ++vectorStrBegin;
        }
    }

    if (m_vectorElements.empty())
    {
        return ErrorCode::Fail;
    }

    return ErrorCode::Success;
}


const std::vector<const char*>&
QueryParser::GetVectorElements() const
{
    return m_vectorElements;
}


const std::vector<QueryParser::OptionPair>&
QueryParser::GetOptions() const
{
    return m_options;
}


const char*
QueryParser::GetVectorBase64() const
{
    return m_vectorBase64;
}


SizeType
QueryParser::GetVectorBase64Length() const
{
    return m_vectorBase64Length;
}
