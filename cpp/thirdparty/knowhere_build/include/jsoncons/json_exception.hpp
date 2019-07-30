// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSON_EXCEPTION_HPP
#define JSON_EXCEPTION_HPP

#include <string> // std::string
#include <sstream> // std::ostringstream
#include <system_error> // std::error_code
#include <jsoncons/unicode_traits.hpp> // unicons::convert
#include <jsoncons/config/jsoncons_config.hpp>

namespace jsoncons {

// json_exception

class json_exception 
{
public:
    virtual const char* what() const noexcept = 0;
};

template <class Base>
class json_runtime_error : public Base, public virtual json_exception
{
public:
    json_runtime_error(const std::string& s) noexcept
        : Base(""), message_(s)
    {
    }
    ~json_runtime_error() noexcept
    {
    }
    const char* what() const noexcept override
    {
        return message_.c_str();
    }
private:
    std::string message_;
};

class key_not_found : public std::out_of_range, public virtual json_exception
{
public:
    template <class CharT>
    explicit key_not_found(const CharT* key, size_t length) noexcept
        : std::out_of_range("")
    {
        buffer_.append("Key '");
        unicons::convert(key, key+length, std::back_inserter(buffer_),
                         unicons::conv_flags::strict);
        buffer_.append("' not found");
    }
    ~key_not_found() noexcept
    {
    }
    const char* what() const noexcept override
    {
        return buffer_.c_str();
    }
private:
    std::string buffer_;
};

class not_an_object : public std::runtime_error, public virtual json_exception
{
public:
    template <class CharT>
    explicit not_an_object(const CharT* key, size_t length) noexcept
        : std::runtime_error("")
    {
        buffer_.append("Attempting to access or modify '");
        unicons::convert(key, key+length, std::back_inserter(buffer_),
                         unicons::conv_flags::strict);
        buffer_.append("' on a value that is not an object");
    }
    ~not_an_object() noexcept
    {
    }
    const char* what() const noexcept override
    {
        return buffer_.c_str();
    }
private:
    std::string buffer_;
};

class ser_error : public std::system_error, public virtual json_exception
{
public:
    ser_error(std::error_code ec)
        : std::system_error(ec), line_number_(0), column_number_(0)
    {
    }
    ser_error(std::error_code ec, size_t position)
        : std::system_error(ec), line_number_(0), column_number_(position)
    {
    }
    ser_error(std::error_code ec, size_t line, size_t column)
        : std::system_error(ec), line_number_(line), column_number_(column)
    {
    }
    ser_error(const ser_error& other) = default;

    ser_error(ser_error&& other) = default;

    const char* what() const noexcept override
    {
        try
        {
            std::ostringstream os;
            os << this->code().message();
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
private:
    std::string buffer_;
    size_t line_number_;
    size_t column_number_;
};

#if !defined(JSONCONS_NO_DEPRECATED)
typedef ser_error serialization_error;
typedef ser_error json_parse_exception;
typedef ser_error parse_exception;
typedef ser_error parse_error;
#endif

#define JSONCONS_STR2(x)  #x
#define JSONCONS_STR(x)  JSONCONS_STR2(x)

#ifdef _DEBUG
#define JSONCONS_ASSERT(x) if (!(x)) { \
    throw jsoncons::json_runtime_error<std::runtime_error>("assertion '" #x "' failed at " __FILE__ ":" \
            JSONCONS_STR(__LINE__)); }
#else
#define JSONCONS_ASSERT(x) if (!(x)) { \
    throw jsoncons::json_runtime_error<std::runtime_error>("assertion '" #x "' failed at  <> :" \
            JSONCONS_STR( 0 )); }
#endif // _DEBUG

#define JSONCONS_THROW(x) throw (x)

}
#endif
