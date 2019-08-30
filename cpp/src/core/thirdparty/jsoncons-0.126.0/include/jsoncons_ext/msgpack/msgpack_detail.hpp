// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_MSGPACK_MSGPACK_DETAIL_HPP
#define JSONCONS_MSGPACK_MSGPACK_DETAIL_HPP

#include <string>
#include <memory>
#include <jsoncons/config/binary_detail.hpp>

namespace jsoncons { namespace msgpack { namespace detail {

namespace msgpack_format
{
    const uint8_t positive_fixint_base_cd = 0x00;
    const uint8_t nil_cd = 0xc0;
    const uint8_t false_cd = 0xc2;
    const uint8_t true_cd = 0xc3;
    const uint8_t float32_cd = 0xca;
    const uint8_t float64_cd = 0xcb;
    const uint8_t uint8_cd = 0xcc;
    const uint8_t uint16_cd = 0xcd;
    const uint8_t uint32_cd = 0xce;
    const uint8_t uint64_cd = 0xcf;
    const uint8_t int8_cd = 0xd0;
    const uint8_t int16_cd = 0xd1;
    const uint8_t int32_cd = 0xd2;
    const uint8_t int64_cd = 0xd3;
    const uint8_t fixmap_base_cd = 0x80;
    const uint8_t fixarray_base_cd = 0x90;
    const uint8_t fixstr_base_cd = 0xa0;
    const uint8_t str8_cd = 0xd9;
    const uint8_t str16_cd = 0xda;
    const uint8_t str32_cd = 0xdb;
    const uint8_t bin8_cd = 0xc7;
    const uint8_t bin16_cd = 0xc8;
    const uint8_t bin32_cd = 0xc9;
    const uint8_t array16_cd = 0xdc;
    const uint8_t array32_cd = 0xdd;
    const uint8_t map16_cd = 0xde;
    const uint8_t map32_cd = 0xdf;
    const uint8_t negative_fixint_base_cd = 0xe0;
}
 
}}}

#endif
