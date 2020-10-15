// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_STRINGCONVERTHELPER_H_
#define _SPTAG_HELPER_STRINGCONVERTHELPER_H_

#include "inc/Core/Common.h"
#include "CommonHelper.h"

#include <string>
#include <cstring>
#include <sstream>
#include <cctype>
#include <limits>
#include <cerrno>

namespace SPTAG
{
namespace Helper
{
namespace Convert
{

template <typename DataType>
inline bool ConvertStringTo(const char* p_str, DataType& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

    std::istringstream sstream;
    sstream.str(p_str);
    if (p_str >> p_value)
    {
        return true;
    }

    return false;
}


template<typename DataType>
inline std::string ConvertToString(const DataType& p_value)
{
    return std::to_string(p_value);
}


// Specialization of ConvertStringTo<>().

template <typename DataType>
inline bool ConvertStringToSignedInt(const char* p_str, DataType& p_value)
{
    static_assert(std::is_integral<DataType>::value && std::is_signed<DataType>::value, "type check");

    if (nullptr == p_str)
    {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    auto val = std::strtoll(p_str, &end, 10);
    if (errno == ERANGE || end == p_str || *end != '\0')
    {
        return false;
    }

    if (val < (std::numeric_limits<DataType>::min)() || val >(std::numeric_limits<DataType>::max)())
    {
        return false;
    }

    p_value = static_cast<DataType>(val);
    return true;
}


template <typename DataType>
inline bool ConvertStringToUnsignedInt(const char* p_str, DataType& p_value)
{
    static_assert(std::is_integral<DataType>::value && std::is_unsigned<DataType>::value, "type check");

    if (nullptr == p_str)
    {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    auto val = std::strtoull(p_str, &end, 10);
    if (errno == ERANGE || end == p_str || *end != '\0')
    {
        return false;
    }

    if (val < (std::numeric_limits<DataType>::min)() || val >(std::numeric_limits<DataType>::max)())
    {
        return false;
    }

    p_value = static_cast<DataType>(val);
    return true;
}


template <>
inline bool ConvertStringTo<std::string>(const char* p_str, std::string& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

    p_value = p_str;
    return true;
}


template <>
inline bool ConvertStringTo<float>(const char* p_str, float& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    p_value = std::strtof(p_str, &end);
    return (errno != ERANGE && end != p_str && *end == '\0');
}


template <>
inline bool ConvertStringTo<double>(const char* p_str, double& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

    char* end = nullptr;
    errno = 0;
    p_value = std::strtod(p_str, &end);
    return (errno != ERANGE && end != p_str && *end == '\0');
}


template <>
inline bool ConvertStringTo<std::int8_t>(const char* p_str, std::int8_t& p_value)
{
    return ConvertStringToSignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::int16_t>(const char* p_str, std::int16_t& p_value)
{
    return ConvertStringToSignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::int32_t>(const char* p_str, std::int32_t& p_value)
{
    return ConvertStringToSignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::int64_t>(const char* p_str, std::int64_t& p_value)
{
    return ConvertStringToSignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::uint8_t>(const char* p_str, std::uint8_t& p_value)
{
    return ConvertStringToUnsignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::uint16_t>(const char* p_str, std::uint16_t& p_value)
{
    return ConvertStringToUnsignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::uint32_t>(const char* p_str, std::uint32_t& p_value)
{
    return ConvertStringToUnsignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<std::uint64_t>(const char* p_str, std::uint64_t& p_value)
{
    return ConvertStringToUnsignedInt(p_str, p_value);
}


template <>
inline bool ConvertStringTo<bool>(const char* p_str, bool& p_value)
{
    if (StrUtils::StrEqualIgnoreCase(p_str, "true"))
    {
        p_value = true;
        
    }
    else if (StrUtils::StrEqualIgnoreCase(p_str, "false"))
    {
        p_value = false;
    }
    else
    {
        return false;
    }

    return true;
}


template <>
inline bool ConvertStringTo<IndexAlgoType>(const char* p_str, IndexAlgoType& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

#define DefineIndexAlgo(Name) \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) \
    { \
        p_value = IndexAlgoType::Name; \
        return true; \
    } \

#include "inc/Core/DefinitionList.h"
#undef DefineIndexAlgo

    return false;
}


template <>
inline bool ConvertStringTo<DistCalcMethod>(const char* p_str, DistCalcMethod& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

#define DefineDistCalcMethod(Name) \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) \
    { \
        p_value = DistCalcMethod::Name; \
        return true; \
    } \

#include "inc/Core/DefinitionList.h"
#undef DefineDistCalcMethod

    return false;
}


template <>
inline bool ConvertStringTo<VectorValueType>(const char* p_str, VectorValueType& p_value)
{
    if (nullptr == p_str)
    {
        return false;
    }

#define DefineVectorValueType(Name, Type) \
    else if (StrUtils::StrEqualIgnoreCase(p_str, #Name)) \
    { \
        p_value = VectorValueType::Name; \
        return true; \
    } \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    return false;
}


// Specialization of ConvertToString<>().

template<>
inline std::string ConvertToString<std::string>(const std::string& p_value)
{
    return p_value;
}


template<>
inline std::string ConvertToString<bool>(const bool& p_value)
{
    return p_value ? "true" : "false";
}


template <>
inline std::string ConvertToString<IndexAlgoType>(const IndexAlgoType& p_value)
{
    switch (p_value)
    {
#define DefineIndexAlgo(Name) \
    case IndexAlgoType::Name: \
        return #Name; \

#include "inc/Core/DefinitionList.h"
#undef DefineIndexAlgo

    default:
        break;
    }

    return "Undefined";
}


template <>
inline std::string ConvertToString<DistCalcMethod>(const DistCalcMethod& p_value)
{
    switch (p_value)
    {
#define DefineDistCalcMethod(Name) \
    case DistCalcMethod::Name: \
        return #Name; \

#include "inc/Core/DefinitionList.h"
#undef DefineDistCalcMethod

    default:
        break;
    }

    return "Undefined";
}


template <>
inline std::string ConvertToString<VectorValueType>(const VectorValueType& p_value)
{
    switch (p_value)
    {
#define DefineVectorValueType(Name, Type) \
    case VectorValueType::Name: \
        return #Name; \

#include "inc/Core/DefinitionList.h"
#undef DefineVectorValueType

    default:
        break;
    }

    return "Undefined";
}


} // namespace Convert
} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_STRINGCONVERTHELPER_H_
