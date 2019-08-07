// Copyright 2017 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_CBOR_CBOR_DETAIL_HPP
#define JSONCONS_CBOR_CBOR_DETAIL_HPP

#include <string>
#include <vector>
#include <memory>
#include <iterator> // std::forward_iterator_tag
#include <limits> // std::numeric_limits
#include <utility> // std::move
#include <jsoncons/json.hpp>
#include <jsoncons/json_content_handler.hpp>
#include <jsoncons/config/binary_detail.hpp>

namespace jsoncons { namespace cbor { namespace detail {

// 0x00..0x17 (0..23)
#define JSONCONS_CBOR_0x00_0x17 \
        0x00:case 0x01:case 0x02:case 0x03:case 0x04:case 0x05:case 0x06:case 0x07:case 0x08:case 0x09:case 0x0a:case 0x0b:case 0x0c:case 0x0d:case 0x0e:case 0x0f:case 0x10:case 0x11:case 0x12:case 0x13:case 0x14:case 0x15:case 0x16:case 0x17

enum class cbor_major_type : uint8_t
{
    unsigned_integer = 0x00,
    negative_integer = 0x01,
    byte_string = 0x02,
    text_string = 0x03,
    array = 0x04,
    map = 0x05,   
    semantic_tag = 0x06,
    simple = 0x7
};

namespace additional_info
{
    const uint8_t indefinite_length = 0x1f;
}

inline
size_t min_length_for_stringref(uint64_t index)
{
    size_t n;
    if (index <= 23)
    {
        n = 3;
    }
    else if (index <= 255)
    {
        n = 4;
    }
    else if (index <= 65535)
    {
        n = 5;
    }
    else if (index <= 4294967295)
    {
        n = 7;
    }
    else 
    {
        n = 11;
    }
    return n;
}

}}}

#endif
