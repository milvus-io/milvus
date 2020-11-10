#pragma once
#include <string_view>
#include <stdio.h>
#include <stdlib.h>

/* Paste this on the file you want to debug. */

namespace milvus::impl {
void
EasyAssertInfo(
    bool value, std::string_view expr_str, std::string_view filename, int lineno, std::string_view extra_info);
}

#define AssertInfo(expr, info) milvus::impl::EasyAssertInfo(bool(expr), #expr, __FILE__, __LINE__, (info))
#define Assert(expr) AssertInfo((expr), "")
