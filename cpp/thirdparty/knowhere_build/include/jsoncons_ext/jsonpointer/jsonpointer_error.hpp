// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_JSONPOINTER_JSONPOINTER_ERROR_HPP
#define JSONCONS_JSONPOINTER_JSONPOINTER_ERROR_HPP

#include <jsoncons/json_exception.hpp>
#include <system_error>

namespace jsoncons { namespace jsonpointer {

class jsonpointer_error : public std::system_error, public virtual json_exception
{
public:
    jsonpointer_error(const std::error_code& ec)
        : std::system_error(ec)
    {
    }
    jsonpointer_error(const std::error_code& ec, const std::string& what_arg)
        : std::system_error(ec, what_arg)
    {
    }
    jsonpointer_error(const std::error_code& ec, const char* what_arg)
        : std::system_error(ec, what_arg)
    {
    }
    jsonpointer_error(const jsonpointer_error& other) = default;

    jsonpointer_error(jsonpointer_error&& other) = default;

    const char* what() const noexcept override
    {
        return std::system_error::what();
    }
};

enum class jsonpointer_errc 
{
    ok = 0,
    expected_slash = 1,
    index_exceeds_array_size,
    expected_0_or_1,
    invalid_index,
    name_not_found,
    key_already_exists,
    expected_object_or_array,
    end_of_input
};

class jsonpointer_error_category_impl
   : public std::error_category
{
public:
    const char* name() const noexcept override
    {
        return "jsoncons/jsonpointer";
    }
    std::string message(int ev) const override
    {
        switch (static_cast<jsonpointer_errc>(ev))
        {
            case jsonpointer_errc::expected_slash:
                return "Expected /";
            case jsonpointer_errc::index_exceeds_array_size:
                return "Index exceeds array size";
            case jsonpointer_errc::expected_0_or_1:
                return "Expected '0' or '1' after escape character '~'";
            case jsonpointer_errc::name_not_found:
                return "Name not found";
            case jsonpointer_errc::invalid_index:
                return "Invalid array index";
            case jsonpointer_errc::key_already_exists:
                return "Key already exists";
            case jsonpointer_errc::expected_object_or_array:
                return "Expected object or array";
            case jsonpointer_errc::end_of_input:
                return "Unexpected end of input";
            default:
                return "Unknown jsonpointer error";
        }
    }
};

inline
const std::error_category& jsonpointer_error_category()
{
  static jsonpointer_error_category_impl instance;
  return instance;
}

inline 
std::error_code make_error_code(jsonpointer_errc result)
{
    return std::error_code(static_cast<int>(result),jsonpointer_error_category());
}

}}

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::jsonpointer::jsonpointer_errc> : public true_type
    {
    };
}

#endif
