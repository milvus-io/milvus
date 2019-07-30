/// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CSV_CSV_ERROR_HPP
#define JSONCONS_CSV_CSV_ERROR_HPP

#include <system_error>
#include <jsoncons/json_exception.hpp>

namespace jsoncons { namespace csv {

    enum class csv_errc : int
    {
        ok = 0,
        unexpected_eof = 1,
        source_error,
        expected_quote,
        invalid_csv_text,
        invalid_state
    };

#if !defined(JSONCONS_NO_DEPRECATED)
typedef csv_errc csv_parser_errc;
#endif

class csv_error_category_impl
   : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "jsoncons/csv";
    }
    std::string message(int ev) const override
    {
        switch (static_cast<csv_errc>(ev))
        {
            case csv_errc::unexpected_eof:
                return "Unexpected end of file";
            case csv_errc::source_error:
                return "Source error";
            case csv_errc::expected_quote:
                return "Expected quote character";
            case csv_errc::invalid_csv_text:
                return "Invalid CSV text";
            default:
                return "Unknown JSON parser error";
        }
    }
};

inline
const std::error_category& csv_error_category()
{
  static csv_error_category_impl instance;
  return instance;
}

inline 
std::error_code make_error_code(csv_errc result)
{
    return std::error_code(static_cast<int>(result),csv_error_category());
}

}}

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::csv::csv_errc> : public true_type
    {
    };
}

#endif
