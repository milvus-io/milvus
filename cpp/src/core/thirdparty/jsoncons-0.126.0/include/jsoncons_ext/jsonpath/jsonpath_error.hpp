/// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSONPATH_JSONPATH_ERROR_HPP
#define JSONCONS_JSONPATH_JSONPATH_ERROR_HPP

#include <jsoncons/json_exception.hpp>
#include <system_error>

namespace jsoncons { namespace jsonpath {

class jsonpath_error : public std::system_error, public virtual json_exception
{
    std::string buffer_;
    size_t line_number_;
    size_t column_number_;
public:
    jsonpath_error(std::error_code ec)
        : std::system_error(ec), line_number_(0), column_number_(0)
    {
    }
    jsonpath_error(std::error_code ec, size_t position)
        : std::system_error(ec), line_number_(0), column_number_(position)
    {
    }
    jsonpath_error(std::error_code ec, size_t line, size_t column)
        : std::system_error(ec), line_number_(line), column_number_(column)
    {
    }
    jsonpath_error(const jsonpath_error& other) = default;

    jsonpath_error(jsonpath_error&& other) = default;

    const char* what() const noexcept override
    {
        try
        {
            std::ostringstream os;
            os << std::system_error::what();
            if (line_number_ != 0 && column_number_ != 0)
            {
                os << " at line " << line_number_ << " and column " << column_number_;
            }
            else if (column_number_ != 0)
            {
                os << " at position " << column_number_;
            }
            const_cast<std::string&>(buffer_) = os.str();
            return buffer_.c_str();
        }
        catch (...)
        {
            return std::system_error::what();
        }
    }

    size_t line() const noexcept
    {
        return line_number_;
    }

    size_t column() const noexcept
    {
        return column_number_;
    }
#if !defined(JSONCONS_NO_DEPRECATED)
    size_t line_number() const noexcept
    {
        return line();
    }

    size_t column_number() const noexcept
    {
        return column();
    }
#endif
};

enum class jsonpath_errc 
{
    ok = 0,
    expected_root,
    expected_current_node,
    expected_right_bracket,
    expected_name,
    expected_separator,
    invalid_filter,
    invalid_filter_expected_slash,
    invalid_filter_unbalanced_paren,
    invalid_filter_unsupported_operator,
    invalid_filter_expected_right_brace,
    invalid_filter_expected_primary,
    expected_slice_start,
    expected_slice_end,
    expected_slice_step,
    expected_left_bracket_token,
    expected_minus_or_digit_or_colon_or_comma_or_right_bracket,
    expected_digit_or_colon_or_comma_or_right_bracket,
    expected_minus_or_digit_or_comma_or_right_bracket,
    expected_digit_or_comma_or_right_bracket,
    unexpected_operator,
    invalid_function_name,
    invalid_argument,
    function_name_not_found,
    parse_error_in_filter,
    argument_parse_error,
    unidentified_error,
    unexpected_end_of_input
};

class jsonpath_error_category_impl
   : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "jsoncons/jsonpath";
    }
    std::string message(int ev) const override
    {
        switch (static_cast<jsonpath_errc>(ev))
        {
            case jsonpath_errc::expected_root:
                return "Expected $";
            case jsonpath_errc::expected_current_node:
                return "Expected @";
            case jsonpath_errc::expected_right_bracket:
                return "Expected ]";
            case jsonpath_errc::expected_name:
                return "Expected a name following a dot";
            case jsonpath_errc::expected_slice_start:
                return "Expected slice start";
            case jsonpath_errc::expected_slice_end:
                return "Expected slice end";
            case jsonpath_errc::expected_slice_step:
                return "Expected slice step";
            case jsonpath_errc::expected_separator:
                return "Expected dot or left bracket separator";
            case jsonpath_errc::invalid_filter:
                return "Invalid path filter";
            case jsonpath_errc::invalid_filter_expected_slash:
                return "Invalid path filter, expected '/'";
            case jsonpath_errc::invalid_filter_unbalanced_paren:
                return "Invalid path filter, unbalanced parenthesis";
            case jsonpath_errc::invalid_filter_unsupported_operator:
                return "Unsupported operator";
            case jsonpath_errc::invalid_filter_expected_right_brace:
                return "Invalid path filter, expected right brace }";
            case jsonpath_errc::invalid_filter_expected_primary:
                return "Invalid path filter, expected primary expression.";
            case jsonpath_errc::expected_left_bracket_token:
                return "Expected ?,',\",0-9,*";
            case jsonpath_errc::expected_minus_or_digit_or_colon_or_comma_or_right_bracket:
                return "Expected - or 0-9 or : or , or ]";
            case jsonpath_errc::expected_minus_or_digit_or_comma_or_right_bracket:
                return "Expected - or 0-9 or , or ]";
            case jsonpath_errc::expected_digit_or_comma_or_right_bracket:
                return "Expected - or 0-9 or , or ]";
            case jsonpath_errc::expected_digit_or_colon_or_comma_or_right_bracket:
                return "Expected 0-9 or : or , or ]";
            case jsonpath_errc::invalid_function_name:
                return "Invalid function name";
            case jsonpath_errc::invalid_argument:
                return "Invalid argument type";
            case jsonpath_errc::function_name_not_found:
                return "Function name not found";
            case jsonpath_errc::parse_error_in_filter:
                return "Could not parse JSON expression in a JSONPath filter";
            case jsonpath_errc::argument_parse_error:
                return "Could not parse JSON expression passed to JSONPath function";
            case jsonpath_errc::unidentified_error:
                return "Unidentified error";
            case jsonpath_errc::unexpected_end_of_input:
                return "Unexpected end of jsonpath input";
            default:
                return "Unknown jsonpath parser error";
        }
    }
};

inline
const std::error_category& jsonpath_error_category()
{
  static jsonpath_error_category_impl instance;
  return instance;
}

inline 
std::error_code make_error_code(jsonpath_errc result)
{
    return std::error_code(static_cast<int>(result),jsonpath_error_category());
}

}}

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::jsonpath::jsonpath_errc> : public true_type
    {
    };
}

#endif
