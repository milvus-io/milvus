// Copyright 2013 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_UBJSON_UBJSON_DETAIL_HPP
#define JSONCONS_UBJSON_UBJSON_DETAIL_HPP

#include <string>
#include <memory>
#include <jsoncons/config/binary_detail.hpp>

namespace jsoncons { namespace ubjson { namespace detail {

namespace ubjson_format
{
    const uint8_t null_type = 'Z';
    const uint8_t no_op_type = 'N';
    const uint8_t true_type = 'T';
    const uint8_t false_type = 'F';
    const uint8_t int8_type = 'i';
    const uint8_t uint8_type = 'U';
    const uint8_t int16_type = 'I';
    const uint8_t int32_type = 'l';
    const uint8_t int64_type = 'L';
    const uint8_t float32_type = 'd';
    const uint8_t float64_type = 'D';
    const uint8_t high_precision_number_type = 'H';
    const uint8_t char_type = 'C';
    const uint8_t string_type = 'S';
    const uint8_t start_array_marker = '[';
    const uint8_t end_array_marker = ']';
    const uint8_t start_object_marker = '{';
    const uint8_t end_object_marker = '}';
    const uint8_t type_marker = '$';
    const uint8_t count_marker = '#';
}
 
}}}

#endif
