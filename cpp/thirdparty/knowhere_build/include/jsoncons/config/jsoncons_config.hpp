// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSONCONS_CONFIG_HPP
#define JSONCONS_JSONCONS_CONFIG_HPP

#include <stdexcept>
#include <string>
#include <cmath>
#include <exception>

// Uncomment the following line to suppress deprecated names (recommended for new code)
//#define JSONCONS_NO_DEPRECATED

// The definitions below follow the definitions in compiler_support_p.h, https://github.com/01org/tinycbor
// MIT license

// https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54577
#if defined(__GNUC__) && __GNUC__ == 4 && __GNUC_MINOR__ < 9
#define JSONCONS_NO_ERASE_TAKING_CONST_ITERATOR 1
#endif

#if defined(__clang__) 
#  define JSONCONS_FALLTHROUGH [[clang::fallthrough]]
#elif defined(__GNUC__) && ((__GNUC__ >= 7))
#  define JSONCONS_FALLTHROUGH __attribute__((fallthrough))
#elif defined (__GNUC__)
#  define JSONCONS_FALLTHROUGH // FALLTHRU
#else
#  define JSONCONS_FALLTHROUGH
#endif

#if defined(__GNUC__) || defined(__clang__)
#define JSONCONS_LIKELY(x) __builtin_expect(!!(x), 1)
#define JSONCONS_UNLIKELY(x) __builtin_expect(!!(x), 0)
#define JSONCONS_UNREACHABLE() __builtin_unreachable()
#elif defined(_MSC_VER)
#define JSONCONS_LIKELY(x) x
#define JSONCONS_UNLIKELY(x) x
#define JSONCONS_UNREACHABLE() __assume(0)
#else
#define JSONCONS_LIKELY(x) x
#define JSONCONS_UNLIKELY(x) x
#define JSONCONS_UNREACHABLE() do {} while (0)
#endif

// Follows boost 1_68
#if !defined(JSONCONS_HAS_STRING_VIEW)
#  if defined(__clang__)
#   if (__cplusplus >= 201703)
#    if __has_include(<string_view>)
#     define JSONCONS_HAS_STRING_VIEW 1
#    endif // __has_include(<string_view>)
#   endif // (__cplusplus >= 201703)
#  endif // defined(__clang__)
#  if defined(__GNUC__)
#   if (__GNUC__ >= 7)
#    if (__cplusplus >= 201703) || (defined(_HAS_CXX17) && _HAS_CXX17 == 1)
#     define JSONCONS_HAS_STRING_VIEW 1
#    endif // (__cplusplus >= 201703)
#   endif // (__GNUC__ >= 7)
#  endif // defined(__GNUC__)
#  if defined(_MSC_VER)
#   if (_MSC_VER >= 1910 && _HAS_CXX17)
#    define JSONCONS_HAS_STRING_VIEW
#   endif // (_MSC_VER >= 1910 && _HAS_CXX17)
#  endif // defined(_MSC_VER)
#endif // !defined(JSONCONS_HAS_STRING_VIEW)

#define JSONCONS_NO_TO_CHARS

#if defined(ANDROID) || defined(__ANDROID__)
#define JSONCONS_HAS_STRTOLD_L
#if __ANDROID_API__ >= 21
#else
#define JSONCONS_NO_LOCALECONV
#endif
#endif

#if defined(_MSC_VER)
#define JSONCONS_HAS_MSC__STRTOD_L
#define JSONCONS_HAS_FOPEN_S
#endif

#if !defined(JSONCONS_HAS_STRING_VIEW)
#include <jsoncons/detail/string_view.hpp>
namespace jsoncons {
template <class CharT, class Traits = std::char_traits<CharT>>
using basic_string_view = jsoncons::detail::basic_string_view<CharT, Traits>;
using string_view = basic_string_view<char, std::char_traits<char>>;
using wstring_view = basic_string_view<wchar_t, std::char_traits<wchar_t>>;
}
#else 
#include <string_view>
namespace jsoncons {
template <class CharT, class Traits = std::char_traits<CharT>>
using basic_string_view = std::basic_string_view<CharT, Traits>;
using string_view = std::string_view;
using wstring_view = std::wstring_view;
}
#endif
 
#define JSONCONS_STRING_LITERAL(name, ...) \
    template <class CharT> \
    const std::basic_string<CharT>& name##_literal() {\
        static const CharT s[] = { __VA_ARGS__};\
        static const std::basic_string<CharT> sv(s, sizeof(s) / sizeof(CharT));\
        return sv;\
    }

#endif
