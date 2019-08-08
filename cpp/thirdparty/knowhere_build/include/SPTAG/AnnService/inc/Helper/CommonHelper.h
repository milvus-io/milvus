// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#ifndef _SPTAG_HELPER_COMMONHELPER_H_
#define _SPTAG_HELPER_COMMONHELPER_H_

#include "../Core/Common.h"

#include <string>
#include <vector>
#include <cctype>
#include <functional>
#include <limits>
#include <cerrno>


namespace SPTAG
{
namespace Helper
{
namespace StrUtils
{

void ToLowerInPlace(std::string& p_str);

std::vector<std::string> SplitString(const std::string& p_str, const std::string& p_separator);

std::pair<const char*, const char*> FindTrimmedSegment(const char* p_begin,
                                                       const char* p_end,
                                                       const std::function<bool(char)>& p_isSkippedChar);

bool StartsWith(const char* p_str, const char* p_prefix);

bool StrEqualIgnoreCase(const char* p_left, const char* p_right);

} // namespace StrUtils
} // namespace Helper
} // namespace SPTAG

#endif // _SPTAG_HELPER_COMMONHELPER_H_
