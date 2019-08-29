// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_BSON_BSON_DETAIL_HPP
#define JSONCONS_BSON_BSON_DETAIL_HPP

#include <string>
#include <memory>
#include <jsoncons/config/binary_detail.hpp>

namespace jsoncons { namespace bson { namespace detail {

namespace bson_format
{
    const uint8_t double_cd = 0x01;
    const uint8_t string_cd = 0x02; // UTF-8 string
    const uint8_t document_cd = 0x03;
    const uint8_t array_cd = 0x04;
    const uint8_t binary_cd = 0x05;
    const uint8_t object_id_cd = 0x07;
    const uint8_t bool_cd = 0x08;
    const uint8_t datetime_cd = 0x09;
    const uint8_t null_cd = 0x0a;
    const uint8_t regex_cd = 0x0b;
    const uint8_t javascript_cd = 0x0d;
    const uint8_t javascript_with_scope_cd = 0x0f;
    const uint8_t int32_cd = 0x10;
    const uint8_t timestamp_cd = 0x11; // MongoDB internal Timestamp, uint64
    const uint8_t int64_cd = 0x12;
    const uint8_t decimal128_cd = 0x13;
    const uint8_t min_key_cd = 0xff;
    const uint8_t max_key_cd = 0x7f;
}

enum class bson_container_type {document, array};

}}}

#endif
