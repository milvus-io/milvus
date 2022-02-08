// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#include "inc/Helper/SimpleIniReader.h"
#include "inc/Helper/CommonHelper.h"

#include <iostream>
#include <fstream>
#include <cctype>
#include <functional>

using namespace SPTAG;
using namespace SPTAG::Helper;

const IniReader::ParameterValueMap IniReader::c_emptyParameters;


IniReader::IniReader()
{
}


IniReader::~IniReader()
{
}


ErrorCode IniReader::LoadIni(std::istream& p_input)
{
    const std::size_t c_bufferSize = 1 << 16;

    std::unique_ptr<char[]> line(new char[c_bufferSize]);

    std::string currSection;
    std::shared_ptr<ParameterValueMap> currParamMap(new ParameterValueMap);

    if (m_parameters.count(currSection) == 0)
    {
        m_parameters.emplace(currSection, currParamMap);
    }

    auto isSpace = [](char p_ch) -> bool
    {
        return std::isspace(p_ch) != 0;
    };

    while (!p_input.eof())
    {
        if (!p_input.getline(line.get(), c_bufferSize))
        {
            break;
        }

        std::size_t len = 0;
        while (len < c_bufferSize && line[len] != '\0')
        {
            ++len;
        }

        auto nonSpaceSeg = StrUtils::FindTrimmedSegment(line.get(), line.get() + len, isSpace);

        if (nonSpaceSeg.second <= nonSpaceSeg.first)
        {
            // Blank line.
            continue;
        }

        if (';' == *nonSpaceSeg.first)
        {
            // Comments.
            continue;
        }
        else if ('[' == *nonSpaceSeg.first)
        {
            // Parse Section
            if (']' != *(nonSpaceSeg.second - 1))
            {
                return ErrorCode::ReadIni_FailedParseSection;
            }

            auto sectionSeg = StrUtils::FindTrimmedSegment(nonSpaceSeg.first + 1, nonSpaceSeg.second - 1, isSpace);

            if (sectionSeg.second <= sectionSeg.first)
            {
                // Empty section name.
                return ErrorCode::ReadIni_FailedParseSection;
            }

            currSection.assign(sectionSeg.first, sectionSeg.second);
            StrUtils::ToLowerInPlace(currSection);

            if (m_parameters.count(currSection) == 0)
            {
                currParamMap.reset(new ParameterValueMap);
                m_parameters.emplace(currSection, currParamMap);
            }
            else
            {
                return ErrorCode::ReadIni_DuplicatedSection;
            }
        }
        else
        {
            // Parameter Value Pair.
            const char* equalSignLoc = nonSpaceSeg.first;
            while (equalSignLoc < nonSpaceSeg.second && '=' != *equalSignLoc)
            {
                ++equalSignLoc;
            }

            if (equalSignLoc >= nonSpaceSeg.second)
            {
                return ErrorCode::ReadIni_FailedParseParam;
            }

            auto paramSeg = StrUtils::FindTrimmedSegment(nonSpaceSeg.first, equalSignLoc, isSpace);

            if (paramSeg.second <= paramSeg.first)
            {
                // Empty parameter name.
                return ErrorCode::ReadIni_FailedParseParam;
            }

            std::string paramName(paramSeg.first, paramSeg.second);
            StrUtils::ToLowerInPlace(paramName);

            if (currParamMap->count(paramName) == 0)
            {
                currParamMap->emplace(std::move(paramName), std::string(equalSignLoc + 1, nonSpaceSeg.second));
            }
            else
            {
                return ErrorCode::ReadIni_DuplicatedParam;
            }
        }
    }
    return ErrorCode::Success;
}


ErrorCode
IniReader::LoadIniFile(const std::string& p_iniFilePath)
{
    std::ifstream input(p_iniFilePath);
    if (!input.is_open()) return ErrorCode::FailedOpenFile;
    ErrorCode ret = LoadIni(input);
    input.close();
    return ret;
}


bool
IniReader::DoesSectionExist(const std::string& p_section) const
{
    std::string section(p_section);
    StrUtils::ToLowerInPlace(section);
    return m_parameters.count(section) != 0;
}


bool
IniReader::DoesParameterExist(const std::string& p_section, const std::string& p_param) const
{
    std::string name(p_section);
    StrUtils::ToLowerInPlace(name);
    auto iter = m_parameters.find(name);
    if (iter == m_parameters.cend())
    {
        return false;
    }

    const auto& paramMap = iter->second;
    if (paramMap == nullptr)
    {
        return false;
    }

    name = p_param;
    StrUtils::ToLowerInPlace(name);
    return paramMap->count(name) != 0;
}


bool
IniReader::GetRawValue(const std::string& p_section, const std::string& p_param, std::string& p_value) const
{
    std::string name(p_section);
    StrUtils::ToLowerInPlace(name);
    auto sectionIter = m_parameters.find(name);
    if (sectionIter == m_parameters.cend())
    {
        return false;
    }

    const auto& paramMap = sectionIter->second;
    if (paramMap == nullptr)
    {
        return false;
    }

    name = p_param;
    StrUtils::ToLowerInPlace(name);
    auto paramIter = paramMap->find(name);
    if (paramIter == paramMap->cend())
    {
        return false;
    }

    p_value = paramIter->second;
    return true;
}


const IniReader::ParameterValueMap&
IniReader::GetParameters(const std::string& p_section) const
{
    std::string name(p_section);
    StrUtils::ToLowerInPlace(name);
    auto sectionIter = m_parameters.find(name);
    if (sectionIter == m_parameters.cend() || nullptr == sectionIter->second)
    {
        return c_emptyParameters;
    }

    return *(sectionIter->second);
}

void
IniReader::SetParameter(const std::string& p_section, const std::string& p_param, const std::string& p_val)
{
    std::string name(p_section);
    StrUtils::ToLowerInPlace(name);
    auto sectionIter = m_parameters.find(name);
    if (sectionIter == m_parameters.cend() || sectionIter->second == nullptr) 
    {
        m_parameters[name] = std::shared_ptr<ParameterValueMap>(new ParameterValueMap);
    }

    std::string param(p_param);
    StrUtils::ToLowerInPlace(param);
    (*m_parameters[name])[param] = p_val;
}
