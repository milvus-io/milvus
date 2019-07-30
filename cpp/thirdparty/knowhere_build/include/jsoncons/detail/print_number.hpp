// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_DETAIL_PRINT_NUMBER_HPP
#define JSONCONS_DETAIL_PRINT_NUMBER_HPP

#include <stdexcept>
#include <string>
#include <cmath>
#include <locale>
#include <limits> // std::numeric_limits
#include <exception>
#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/json_options.hpp>
#include <jsoncons/detail/grisu3.hpp>
#include <jsoncons/detail/parse_number.hpp>

namespace jsoncons { namespace detail {

// print_integer

template<class Result> 
size_t print_integer(int64_t value, Result& result)
{
    typedef typename Result::value_type char_type;

    size_t count = 0;

    char_type buf[255];
    uint64_t u = (value < 0) ? static_cast<uint64_t>(-value) : static_cast<uint64_t>(value);
    char_type* p = buf;
    do
    {
        *p++ = static_cast<char_type>(48 + u%10);
    }
    while (u /= 10);
    count += (p-buf);
    if (value < 0)
    {
        result.push_back('-');
        ++count;
    }
    while (--p >= buf)
    {
        result.push_back(*p);
    }

    return count;
}

// print_uinteger

template<class Result>
size_t print_uinteger(uint64_t value, Result& result)
{
    typedef typename Result::value_type char_type;

    size_t count = 0;

    char_type buf[255];
    char_type* p = buf;
    do
    {
        *p++ = static_cast<char_type>(48 + value % 10);
    } 
    while (value /= 10);
    count += (p-buf);
    while (--p >= buf)
    {
        result.push_back(*p);
    }
    return count;
}

// print_integer

template<class Result> 
size_t integer_to_hex_string(int64_t value, Result& result)
{
    typedef typename Result::value_type char_type;

    size_t count = 0;

    char_type buf[255];
    uint64_t u = (value < 0) ? static_cast<uint64_t>(-value) : static_cast<uint64_t>(value);
    char_type* p = buf;
    do
    {
        *p++ = to_hex_character(u%16);
    }
    while (u /= 16);
    count += (p-buf);
    if (value < 0)
    {
        result.push_back('-');
        ++count;
    }
    while (--p >= buf)
    {
        result.push_back(*p);
    }

    return count;
}

// print_uinteger

template<class Result>
size_t uinteger_to_hex_string(uint64_t value, Result& result)
{
    typedef typename Result::value_type char_type;

    size_t count = 0;

    char_type buf[255];
    char_type* p = buf;
    do
    {
        *p++ = to_hex_character(value % 16);
    } 
    while (value /= 16);
    count += (p-buf);
    while (--p >= buf)
    {
        result.push_back(*p);
    }
    return count;
}

// print_double

template <class Result>
void dump_buffer(const char* buffer, size_t length, char decimal_point, Result& result)
{
    const char* sbeg = buffer;
    const char* send = sbeg + length;

    if (sbeg != send)
    {
        bool needs_dot = true;
        for (const char* q = sbeg; q < send; ++q)
        {
            switch (*q)
            {
                case '-':case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':case '+':
                    result.push_back(*q);
                    break;
                case 'e':
                case 'E':
                    result.push_back('e');
                    needs_dot = false;
                    break;
                default:
                    if (*q == decimal_point)
                    {
                        needs_dot = false;
                        result.push_back('.');
                    }
                    break;
            }
        }
        if (needs_dot)
        {
            result.push_back('.');
            result.push_back('0');
            needs_dot = true;
        }
    }
}

template <class Result>
bool dtoa(double val, char decimal_point, Result& result, std::false_type)
{
    if (val == 0)
    {
        result.push_back('0');
        result.push_back('.');
        result.push_back('0');
        return true;
    }

    jsoncons::detail::string_to_double to_double_;

    char buffer[100];
    int precision = std::numeric_limits<double>::digits10;
    int length = snprintf(buffer, sizeof(buffer), "%1.*g", precision, val);
    if (length < 0)
    {
        return false;
    }
    if (to_double_(buffer,sizeof(buffer)) != val)
    {
        const int precision2 = std::numeric_limits<double>::max_digits10;
        length = snprintf(buffer, sizeof(buffer), "%1.*g", precision2, val);
        if (length < 0)
        {
            return false;
        }
    }
    dump_buffer(buffer, length, decimal_point, result);
    return true;
}

template <class Result>
bool dtoa(double v, char decimal_point, Result& result, std::true_type)
{
    if (v == 0)
    {
        result.push_back('0');
        result.push_back('.');
        result.push_back('0');
        return true;
    }

    int length = 0;
    int k;

    char buffer[100];

    double u = std::signbit(v) ? -v : v;
    if (jsoncons::detail::grisu3(u, buffer, &length, &k))
    {
        if (std::signbit(v))
        {
            result.push_back('-');
        }
        // min exp: -4 is consistent with sprintf
        // max exp: std::numeric_limits<double>::max_digits10
        jsoncons::detail::prettify_string(buffer, length, k, -4, std::numeric_limits<double>::max_digits10, result);
        return true;
    }
    else
    {
        return dtoa(v, decimal_point, result, std::false_type());
    }
}

template <class Result>
bool dtoa(double v, char decimal_point, Result& result)
{
    return dtoa(v, decimal_point, result, std::integral_constant<bool,std::numeric_limits<double>::is_iec559>());
}

class print_double
{
private:
    string_to_double to_double_;
    floating_point_options override_;
    char decimal_point_;
public:
    print_double(const floating_point_options& options)
        : override_(options)
    {
#if !defined(JSONCONS_NO_LOCALECONV)
        struct lconv * lc = localeconv();
        if (lc != nullptr && lc->decimal_point[0] != 0)
        {
            decimal_point_ = lc->decimal_point[0];    
        }
        else
#endif
        {
            decimal_point_ = '.'; 
        }
    }

    template <class Result>
    size_t operator()(double val, Result& result)
    {
        size_t count = 0;

        chars_format format = override_.format() != chars_format() ? override_.format() : chars_format::general;

        int decimal_places;
        if (override_.decimal_places() != 0)
        {
            decimal_places = override_.decimal_places();
        }
        else
        {
            format = chars_format::general;
            decimal_places = 0;
        }             

        char number_buffer[200]; 
        int length = 0;

        switch (format)
        {
        case chars_format::fixed:
            {
                length = snprintf(number_buffer, sizeof(number_buffer), "%1.*f", decimal_places, val);
                if (length < 0)
                {
                    JSONCONS_THROW(json_runtime_error<std::invalid_argument>("print_double failed."));
                }
                dump_buffer(number_buffer, length, decimal_point_, result);
            }
            break;
        case chars_format::scientific:
            {
                length = snprintf(number_buffer, sizeof(number_buffer), "%1.*e", decimal_places, val);
                if (length < 0)
                {
                    JSONCONS_THROW(json_runtime_error<std::invalid_argument>("print_double failed."));
                }
                dump_buffer(number_buffer, length, decimal_point_, result);
            }
            break;
        case chars_format::general:
            {
                if (override_.precision() != 0)
                {
                    int precision = override_.precision();
                    length = snprintf(number_buffer, sizeof(number_buffer), "%1.*g", precision, val);
                    if (length < 0)
                    {
                        JSONCONS_THROW(json_runtime_error<std::invalid_argument>("print_double failed."));
                    }
                    dump_buffer(number_buffer, length, decimal_point_, result);
                }
                else
                {
                    if (!dtoa(val, decimal_point_, result))
                    {
                        JSONCONS_THROW(json_runtime_error<std::invalid_argument>("print_double failed."));
                    }
                }             
                break;
            }
            default:
                JSONCONS_THROW(json_runtime_error<std::invalid_argument>("print_double failed."));
                break;
        }
        return count;
    }
};

}}

#endif
