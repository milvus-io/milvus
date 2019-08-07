/// Copyright 2018 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_UBJSON_UBJSON_ERROR_HPP
#define JSONCONS_UBJSON_UBJSON_ERROR_HPP

#include <system_error>
#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons { namespace ubjson {

enum class ubjson_errc
{
    ok = 0,
    unexpected_eof = 1,
    source_error,
    count_required_after_type,
    length_cannot_be_negative,
    length_must_be_integer,
    unknown_type,
    invalid_utf8_text_string,
    too_many_items,
    too_few_items,
    number_too_large
};

class ubjson_error_category_impl
   : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "jsoncons/ubjson";
    }
    std::string message(int ev) const override
    {
        switch (static_cast<ubjson_errc>(ev))
        {
            case ubjson_errc::unexpected_eof:
                return "Unexpected end of file";
            case ubjson_errc::source_error:
                return "Source error";
            case ubjson_errc::count_required_after_type:
                return "Type is specified for container, but count is not specified";
            case ubjson_errc::length_cannot_be_negative:
                return "Length cannot be negative";
            case ubjson_errc::length_must_be_integer:
                return "Length must be a integer numeric type (int8, uint8, int16, int32, int64)";
            case ubjson_errc::invalid_utf8_text_string:
                return "Illegal UTF-8 encoding in text string";
            case ubjson_errc::too_many_items:
                return "Too many items were added to a UBJSON object or array of known length";
            case ubjson_errc::too_few_items:
                return "Too few items were added to a UBJSON object or array of known length";
            case ubjson_errc::number_too_large:
                return "Number exceeds implementation limits";
            default:
                return "Unknown UBJSON parser error";
        }
    }
};

inline
const std::error_category& ubjson_error_category()
{
  static ubjson_error_category_impl instance;
  return instance;
}

inline 
std::error_code make_error_code(ubjson_errc e)
{
    return std::error_code(static_cast<int>(e),ubjson_error_category());
}


}}

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::ubjson::ubjson_errc> : public true_type
    {
    };
}

#endif
