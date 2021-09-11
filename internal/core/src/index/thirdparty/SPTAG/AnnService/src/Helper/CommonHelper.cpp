// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/CommonHelper.h"

#include <iostream>
#include <fstream>
#include <cctype>
#include <functional>

using namespace SPTAG;
using namespace SPTAG::Helper;

void
StrUtils::ToLowerInPlace(std::string& p_str)
{
    for (char& ch : p_str)
    {
        if (std::isupper(ch))
        {
            ch = ch | 0x20;
        }
    }
}


std::vector<std::string>
StrUtils::SplitString(const std::string& p_str, const std::string& p_separator)
{
    std::vector<std::string> ret;

    std::size_t begin = p_str.find_first_not_of(p_separator);
    while (std::string::npos != begin)
    {
        std::size_t end = p_str.find_first_of(p_separator, begin);
        if (std::string::npos == end)
        {
            ret.emplace_back(p_str.substr(begin, p_str.size() - begin));
            break;
        }
        else
        {
            ret.emplace_back(p_str.substr(begin, end - begin));
        }

        begin = p_str.find_first_not_of(p_separator, end);
    }

    return ret;
}


std::pair<const char*, const char*>
StrUtils::FindTrimmedSegment(const char* p_begin,
                             const char* p_end,
                             const std::function<bool(char)>& p_isSkippedChar)
{
    while (p_begin < p_end)
    {
        if (!p_isSkippedChar(*p_begin))
        {
            break;
        }

        ++p_begin;
    }

    while (p_end > p_begin)
    {
        if (!p_isSkippedChar(*(p_end - 1)))
        {
            break;
        }

        --p_end;
    }

    return std::make_pair(p_begin, p_end);
}


bool
StrUtils::StartsWith(const char* p_str, const char* p_prefix)
{
    if (nullptr == p_prefix)
    {
        return true;
    }

    if (nullptr == p_str)
    {
        return false;
    }

    while ('\0' != (*p_prefix) && '\0' != (*p_str))
    {
        if (*p_prefix != *p_str)
        {
            return false;
        }
        ++p_prefix;
        ++p_str;
    }

    return '\0' == *p_prefix;
}


bool
StrUtils::StrEqualIgnoreCase(const char* p_left, const char* p_right)
{
    if (p_left == p_right)
    {
        return true;
    }

    if (p_left == nullptr || p_right == nullptr)
    {
        return false;
    }

    auto tryConv = [](char p_ch) -> char
    {
        if ('a' <= p_ch && p_ch <= 'z')
        {
            return p_ch - 32;
        }

        return p_ch;
    };

    while (*p_left != '\0' && *p_right != '\0')
    {
        if (tryConv(*p_left) != tryConv(*p_right))
        {
            return false;
        }

        ++p_left;
        ++p_right;
    }

    return *p_left == *p_right;
}
