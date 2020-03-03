// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_INIREADER_H_
#define _SPTAG_HELPER_INIREADER_H_

#include "../Core/Common.h"
#include "StringConvert.h"

#include <vector>
#include <map>
#include <memory>
#include <string>
#include <sstream>


namespace SPTAG
{
namespace Helper
{

// Simple INI Reader with basic functions. Case insensitive.
class IniReader
{
public:
    typedef std::map<std::string, std::string> ParameterValueMap;

    IniReader();

    ~IniReader();

    ErrorCode LoadIniFile(const std::string& p_iniFilePath);

    ErrorCode LoadIni(std::istream& p_input);

    bool DoesSectionExist(const std::string& p_section) const;

    bool DoesParameterExist(const std::string& p_section, const std::string& p_param) const;

    const ParameterValueMap& GetParameters(const std::string& p_section) const;

    template <typename DataType>
    DataType GetParameter(const std::string& p_section, const std::string& p_param, const DataType& p_defaultVal) const;

    void SetParameter(const std::string& p_section, const std::string& p_param, const std::string& p_val);

private:
    bool GetRawValue(const std::string& p_section, const std::string& p_param, std::string& p_value) const;

    template <typename DataType>
    static inline DataType ConvertStringTo(std::string&& p_str, const DataType& p_defaultVal);

private:
    const static ParameterValueMap c_emptyParameters;

    std::map<std::string, std::shared_ptr<ParameterValueMap>> m_parameters;
};


template <typename DataType>
DataType
IniReader::GetParameter(const std::string& p_section, const std::string& p_param, const DataType& p_defaultVal) const
{
    std::string value;
    if (!GetRawValue(p_section, p_param, value))
    {
        return p_defaultVal;
    }

    return ConvertStringTo<DataType>(std::move(value), p_defaultVal);
}


template <typename DataType>
inline DataType
IniReader::ConvertStringTo(std::string&& p_str, const DataType& p_defaultVal)
{
    DataType value;
    if (Convert::ConvertStringTo<DataType>(p_str.c_str(), value))
    {
        return value;
    }

    return p_defaultVal;
}


template <>
inline std::string
IniReader::ConvertStringTo<std::string>(std::string&& p_str, const std::string& p_defaultVal)
{
    return std::move(p_str);
}


} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_INIREADER_H_
