master
------

Enhancements

- The `json_reader` and `csv_reader` constructors have been generalized to take either a value 
from which a `jsoncons::string_view` is constructible (e.g. std::string), or a value from which 
a `source_type` is constructible (e.g. std::istream). 

- With this enhancement, the convenience typedefs `json_string_reader` and `csv_string_reader` are no 
longer needed, and have been deprecated.

Name change

- The name `json_pull_reader` has been deprecated (still works) and renamed to `json_cursor` 

Changes to bigfloat mapping

In previous versions, jsoncons arrays that contained

- an int64_t or a uint64_t (defines base-2 exponent)
- an int64_t or a uint64_t or a string tagged with `semantic_tag::bigint` (defines the mantissa)

and that were tagged with `semantic_tag::bigfloat`, were encoded into CBOR bigfloats. 
This behaviour has been deprecated.

CBOR bigfloats are now decoded into a jsoncons string that consists of the following parts

- (optional) minus sign
- 0x
- nonempty sequence of hexadecimal digits (defines mantissa)
- p followed with optional minus or plus sign and nonempty sequence of hexadecimal digits (defines base-2 exponent)

and tagged with `semantic_tag::bigfloat` (before they were decoded into a jsoncons array and tagged with `semantic_tag::bigfloat`)

jsoncons strings that consist of the following parts

- (optional) plus or minus sign
- 0x or 0X
- nonempty sequence of hexadecimal digits optionally containing a decimal-point character
- (optional) p or P followed with optional minus or plus sign and nonempty sequence of decimal digits,

and tagged with `semantic_tag::bigfloat` are now encoded into CBOR bignums.

v0.125.0
--------

CMake build fix

- The CMAKE_BUILD_TYPE variable is now left alone if already set

Non-breaking changes to lighten `semantic_tag` names

- The `semantic_tag` enum values `big_integer`, `big_decimal`, `big_float` and `date_time` 
  have been deprecated (still work) and renamed to `bigint`, `bigdec`, `bigfloat` and `datetime`.

- The `json_content_handler` functions `big_integer_value`, `big_decimal_value`, `date_time_value`
  and `timestamp_value` have been deprecated (still work.) Calls to these functions should be replaced 
  by calls to `string_value` with `semantic_tag::bigint`, `semantic_tag::bigdec`, and `semantic_tag::datetime`,
  and by calls to `int64_vaue` with `semantic_tag::timestamp`.

- The enum type `big_integer_chars_format` has been deprecated (still works) and renamed to 
  `bigint_chars_format`. 

- The `json_options` modifier `big_integer_format` has been deprecated (still works) and renamed to
  `bigint_format`.

Non-breaking changes to `ser_context`, `ser_error` and `jsonpath_error`

- The function names `line_number` and `column_number` have been deprecated (still work) and
  renamed to `line` and `column`.

v0.124.0
--------

- Fixed bug in `json_encoder` with `pad_inside_array_brackets` and `pad_inside_object_braces` options

- Fixed issue with escape character in jsonpath quoted names.

- Fixed pedantic level compiler warnings

- Added doozer tests for CentOS 7.6 and Fedora release 24

- New macro `JSONCONS_GETTER_CTOR_TRAITS_DECL` that can be used to generate the `json_type_traits` boilerplate
from getter functions and a constructor.

v0.123.2
--------

defect fix:

- Fixed name of JSONCONS_MEMBER_TRAITS_DECL

v0.123.1
--------

jsonpath bug fixes:

- Fixed bug in construction of normalized paths [#177](https://github.com/danielaparker/jsoncons/issues/177). 

- Fixed bug in jsonpath recursive descent with filters which could result in too many values being returned

v0.123.0
--------

Warning fix:

- Removed redundant macro continuation character [#176](https://github.com/danielaparker/jsoncons/pull/176)

Non-breaking change to names (old name still works)

- The name JSONCONS_TYPE_TRAITS_DECL has been deprecated and changed to JSONCONS_MEMBER_TRAITS_DECL

Changes to jsonpath:

- jsonpath unions now return distinct values (no duplicates)
- a single dot immediately followed by a left bracket now results in an error (illegal JSONPath)

Enhancements to jsonpath

- Union of completely separate paths are allowed, e.g.

    $[firstName,address.city]

- Names in the dot notation may be single or double quoted

Other enhancements:

- `basic_json` now supports operators `<`, `<=`, `>`, `>=` 

v0.122.0
--------

Changes:

- The template parameter `CharT` has been removed from the binary encoders
  `basic_bson_encoder`, `basic_cbor_encoder`, `basic_msgpack_encoder`,
  and `basic_ubjson_encoder`. 

Enhancements:

- Added macro JSONCONS_TYPE_TRAITS_FRIEND

- Generalized the `csv_encode`, `bson_encode`, `cbor_encode`, `msgpack_encode` and `ubjson_encode` functions to convert from any type T that implements `json_type_traits` 

- Generalized the `csv_decode`, `bson_decode`, `cbor_decode`, `msgpack_decode` and `ubjson_decode` functions to convert to any type T that implements `json_type_traits` 

v0.121.1
--------

Bug fix:

- Fixed issue with cbor_reader only reading tag values 0 through 23

Name change

- The name `json::semantic_tag()` has been renamed to `json::get_semantic_tag()`

Non-breaking changes (old names still work)

- The name `semantic_tag_type` has been deprecated and renamed to `semantic_tag`
- The names `json_serializer`, `bson_serializer`, `cbor_serializer`, `csv_serializer`, `msgpack_serializer`,
  and `ubjson_serializer` have been deprecated and renamed to `json_encoder`, `bson_encoder`, 
  `cbor_encoder`, `csv_encoder`, `msgpack_encoder`, and `ubjson_encoder`
- The names `bson_buffer_serializer`, `cbor_buffer_serializer`, `msgpack_buffer_serializer`,
  and `ubjson_buffer_serializer` have been deprecated and renamed to `bson_bytes_encoder`, 
  `cbor_bytes_encoder`, `msgpack_bytes_encoder`, and `ubjson_bytes_encoder`
- The names `bson_buffer_reader`, `cbor_buffer_reader`, `msgpack_buffer_reader`,
  and `ubjson_buffer_reader` have been deprecated and renamed to `bson_bytes_reader`, 
  `cbor_bytes_reader`, `msgpack_bytes_reader`, and `ubjson_bytes_reader`

Enhancements

- Cleanup of `encode_json` and `decode_json` functions and increased test coverage
- Rewrote cbor_reader to avoid recursive function call
- CBOR reader supports [stringref extension to CBOR](http://cbor.schmorp.de/stringref)
- New `cbor_options` has `packed_strings` option

v0.120.0
--------

Bug fix:

- Fixed issue with `j.as<byte_string_view>()`

Non-breaking changes

- The name `is_json_type_traits_impl` has been deprecated and renamed to `is_json_type_traits_declared`
- The name `serializing_context` has been deprecated and renamed to `ser_context`
- The name `serialization_error` has been deprecated and renamed to `ser_error`

Enhancements

- json `as_byte_string` attempts to decode string values if `semantic_tag_type` is `base64`, `base64url`, or `base16`.

- New macro `JSONCONS_TYPE_TRAITS_DECL` that can be used to generate the `json_type_traits` boilerplate 
for your own types.

- New `basic_json` member function `get_allocator`

v0.119.1
--------

Bug fix:

Fixed issue wjson dump() not formatting booleans correctly #174

v0.119.0
--------

Name change:

- The name `json_staj_reader` has been deprecated and renamed to `json_pull_reader`

Bug fix:

- Fixed a bug in json function `empty()` when type is `byte_string`.
- Fixed a bug with preserving semantic_tag_type when copying json values of long string type.

Changes:

- Removed deprecated feature `cbor_view`

- CBOR decimal fraction and bigfloat string formatting now consistent with double string formatting

Enhancements:

- json `to_string()` and `to_double()` now work with CBOR bigfloat

- JSONPath operators in filter expressions now work with `big_integer`, `big_decimal`, and `big_float`
  tagged json values

- json `is_number()` function now returns `true` if string value is tagged with `big_integer` or 
  `big_decimal`, or if array value is tagged with `big_float`.

- json `as_string()` function now converts arrays tagged with `big_float` to decimal strings

- json `as_double()` function now converts arrays tagged with `big_float` to double values

v0.118.0
--------

New features

- New csv option `lossless_number`. If set to `true`, parse numbers with exponent and fractional parts as strings with
  semantic tagging `semantic_tag_type::big_decimal` (instead of double.) Defaults to `false`.

- A class `jsonpointer::address` has been introduced to make it simpler to construct JSON Pointer addresses

Name change

- The `json_options` name `dec_to_str` has been deprecated and renamed to `lossless_number`.

v0.117.0
--------

Deprecated features:

- cbor_view has been deprecated. Rationale: The complexity of supporting and documenting this component 
  exceeded its benefits.

New features

- New `json_options` option `dec_to_str`. If set to `true`, parse decimal numbers as strings with
  semantic tagging `semantic_tag_type::big_decimal` instead of double. Defaults to `false`.

- The `ojson` (order preserving) implementation now has an index to support binary search 
  for retrieval. 

- Added `std::string_view` detection

- jsoncons-CBOR semantic tagging supported for CBOR tags 32 (uri)

Name changes (non-breaking)

- The json options name `bignum_chars_format` has been deprecated and replaced with `big_integer_chars_format`.
- `big_integer_chars_format::integer` (`bignum_chars_format::integer`) has been deprecated and replaced with 
  `big_integer_chars_format::number`
- The `json_options function` `bignum_format` has been deprecated and replaced with `big_integer_format`

Changes to floating-point printing

- If the platform supports the IEEE 754 standard, jsoncons now uses the Grisu3 algorithm for printing floating-point numbers, 
  falling back to a safe method using C library functions for the estimated 0.5% of floating-point numbers that might be rejected by Grisu3.
  The Grisu3 implementation follows Florian Loitsch's [grisu3_59_56](https://www.cs.tufts.edu/~nr/cs257/archive/florian-loitsch/printf.pdf) 
  implementation. If the platform does not support the IEEE 754 standard, the fall back method is used. 

- In previous versions, jsoncons preserved information about the format, precision, and decimal places
  of the floating-point numbers that it read, and used that information when printing them. With the
  current strategy, that information is no longer needed. Consequently, the `floating_point_options`
  parameter in the `do_double_value` and `double_value` functions of the SAX-style interface have
  been removed. 

- The `json` functions `precision()` and `decimal_places()` have been deprecated and return 0
  (as this information is no longer preserved.)

- The constructor `json(double val, uint8_t precision)` has been deprecated.  

- Note that it is still possible to set precision as a json option when serializing.

v0.116.0
--------

New features:

- New jsonpath functions `keys` and `tokenize`.

- jsoncons-CBOR data item mappings supported for CBOR tags 33 (string base64url) and 34 (string base64)

v0.115.0
--------

New features:

- bson UTC datetime associated with jsoncons `semantic_tag_type::timestamp`

- New traits class `is_json_type_traits_impl` that addresses issues 
  [#133](https://github.com/danielaparker/jsoncons/issues/133) and 
  [#115](https://github.com/danielaparker/jsoncons/issues/115) (duplicates)

- Following a proposal from soberich, jsonpath functions on JSONPath expressions 
  are no longer restricted to filter expressions.

- New jsonpath functions `sum`, `count`, `avg`, and `prod`

- Added `semantic_tag_type::base16`, `semantic_tag_type::base64`, `semantic_tag_type::base64url`

Non-breaking changes:

- The json constructor that takes a `byte_string` and a `byte_string_chars_format` 
  has been deprecated, use a `semantic_tag_type` to supply an encoding hint for a byte string, if any.

- The content handler `byte_string_value` function that takes a `byte_string` and a `byte_string_chars_format` 
  has been deprecated, use a `semantic_tag_type` to supply an encoding hint for a byte string, if any.

Changes:

- The `byte_string_chars_format` parameter in the content handler `do_byte_string_value` function
  has been removed, the `semantic_tag_type` parameter is now used to supply an encoding hint for a byte string, if any.

v0.114.0
--------

Bug fixes:

- On Windows platforms, fixed issue with macro expansion of max when 
  including windows.h  (also in 0.113.1)

- Fixed compile issue with `j = json::make_array()` (also in 0.113.2)

Breaking changes to jsoncons semantic tag type names: 

- semantic_tag_type::bignum to semantic_tag_type::big_integer
- semantic_tag_type::decimal_fraction to semantic_tag_type::big_decimal
- semantic_tag_type::epoch_time to semantic_tag_type::timestamp

Non-breaking name changes:

The following names have been deprecated and renamed (old names still work)

- `bignum_value` to `big_integer_value` in `json_content_handler`
- `decimal_value` to `big_decimal_value` in `json_content_handler`
- `epoch_time_value` to `timestamp_value` in `json_content_handler`

- `cbor_bytes_serializer` to `cbor_buffer_serializer`
- `msgpack_bytes_serializer` to `msgpack_buffer_serializer`

- `json_serializing_options` to `json_options`
- `csv_serializing_options` to `csv_options`

- `parse_error` to `serialization_error`

The rationale for renaming `parse_error` to `serialization_error`
is that we need to use error category codes for serializer 
errors as well as parser errors, so we need a more general name
for the exception type. 

Message Pack enhancements

- New `msgpack_serializer` that supports Message Pack bin formats 

- New `msgpack_parser` that supports Message Pack bin formats 

- `encode_msgpack` and `decode_msgpack` have been
  rewritten using `msgpack_serializer` and `msgpack_parser`,
  and also now support bin formats.

New features:

- decode from and encode to the [Universal Binary JSON Specification (ubjson)](http://bsonspec.org/) data format
- decode from and encode to the [Binary JSON (bson)](http://bsonspec.org/) data format

- The cbor, msgpack and ubjson streaming serializers now validate that the expected number of
  items have been supplied in an object or array of pre-determined length.

version 0.113.0
---------------

Bug fix

- Fixed issue with indefinite length byte strings, text strings, arrays, 
  and maps nested inside other CBOR items (wasn't advancing the
  input pointer past the "break" indicator.) 

Changes

- __FILE__ and __LINE__ macros removed from JSONCONS_ASSERT 
  if not defined _DEBUG (contributed by zhskyy.)

- semantic_tag_type name `decimal` changed to `decimal_fraction`

New CBOR feature

- CBOR semantic tagging of expected conversion of byte strings 
  to base64, base64url and base16 are preserved and respected in JSON 
  serialization (unless overridden in `json_serializing_options`.)

- CBOR semantic tagging of bigfloat preserved with `semantic_tag_type::bigfloat`

- CBOR non text string keys converted to strings when decoding
  to json values

Changes to `json_serializing_options`

New options 

- spaces_around_colon (defaults to `space_after`)
- spaces_around_comma (defaults to `space_after`)
- pad_inside_object_braces (defaults to `false`)
- pad_inside_array_brackets (defaults to `false`)
- line_length_limit (defaults to '120`)
- new_line_chars (for json serialization, defaults to `\n`)

`nan_replacement`, `pos_inf_replacement`, and `neg_inf_replacement` are deprecated (still work)
These have been replaced by

- nan_to_num/nan_to_str
- inf_to_num/inf_to_str
- neginf_to_num/neginf_to_str (default is `-` followed by inf_to_num/inf_to_str)

`nan_to_str`, `inf_to_str` and `neginf_to_str` are also used to substitute back to `nan`, `inf` and `neginf` in the parser. 

- Long since deprecated options `array_array_block_option`,
  `array_object_block_option`, `object_object_block_option` and
  `object_array_block_option` have been removed.

- The names `object_array_split_lines`, `object_object_split_lines`,
  `array_array_split_lines` and `array_object_split_lines` have
  been deprecated (still work) and renamed to `object_array_line_splits`, 
  `object_object_line_splits`, `array_array_line_splits` and `array_object_line_splits`.
  Rationale: consistency with `line_split_kind` name.  

Changes to json_serializer

- Previously the constructor of `json_serializer` took an optional argument to 
  indicate whether "indenting" was on. `json_serializer` now always produces
  indented output, so this argument has been removed.

- A new class `json_compressed_serializer` produces compressed json without 
  indenting.
   
  The jsoncons functions that perform serialization including `json::dump`, 
  `pretty_print` and the output stream operator are unaffected.

v0.112.0
--------

Changes to `json_content_handler`

- The function `byte_string_value` no longer supports passing a byte string as

    handler.byte_string_value({'b','a','r'});

(shown in some of the examples.) Instead use

    handler.byte_string_value(byte_string({'b','a','r'}));

(or a pointer to utf8_t data and a size.)

- The function `bignum_value` no longer supports passing a CBOR signum and
  byte string, `bignum_value` now accepts only a string view. If you
  have a CBOR signum and byte string, you can use the bignum class to 
  convert it into a string. 

Name changes (non breaking)

- The name `json_stream_reader` has been deprecated and replaced with `json_staj_reader`.
- The name `stream_event_type` has been deprecated and replaced with `staj_event_type`
- The names `basic_stream_event` (`stream_event`) have been deprecated and replaced with `basic_staj_event` (`staj_event`)
- The names `basic_stream_filter` (`stream_filter`) have been deprecated and replaced with `basic_staj_filter` (`staj_filter`)
(staj stands for "streaming API for JSON, analagous to StAX in XML)

- The `json_parser` function `end_parse` has been deprecated and replaced with `finish_parse`.

Enhancements

- json double values convert to CBOR float if double to float 
  round trips.

- csv_parser `ignore_empty_values` option now applies to
  `m_columns` style json output.

- json_reader and json_staj_reader can be initialized with strings 
  in addition to streams.

Extension of semantic tags to other values

    - The `json_content_handler` functions `do_null_value`, `do_bool_value`,
      `do_begin_array` and `do_begin_object` have been given the
      semantic_tag_type parameter.  

    - New tag type `semantic_tag_type::undefined` has been added
    
    - The `cbor_parser` encodes a CBOR undefined tag to a json null
      value with a `semantic_tag_type::undefined` tag, and the 
      `cbor_serializer` maps that combination back to a CBOR undefined tag. 

Removed:

- Long since deprecated `value()` functions have been removed from `json_content_handler`

v0.111.1
--------

Enhancements

- Improved efficiency of json_decoder

- Improved efficiency of json_proxy

- Conversion of CBOR decimal fraction to string uses exponential 
  notation if exponent is positive or if the exponent plus the 
  number of digits in the mantissa is negative.

Bug fix

- Fixed issue with conversion of CBOR decimal fraction to string 
  when mantissa is negative

v0.111.0
--------

Bug and warning fixes:

- A case where the json parser performed validation on a string 
  before all bytes of the string had been read, and failed if 
  missing part of a multi-byte byte sequence, is fixed.  

- An issue with reading a bignum  with the pull parser 
`json_stream_reader` (in the case that an integer value 
overflows) has been fixed.

- GCC and clang warnings about switch fall through have 
  been fixed

Non-breaking changes:

- The functions `json::has_key` and `cbor::has_key` have been
  deprecated (but still work) and renamed to `json::contains` 
  and `cbor::contains`. Rationale: consistency with C++ 20
  associative map `contains` function.  

- The json function `as_integer()` is now a template function,

```c++
template <class T = int64_t>
T as_integer();
```

where T can be any integral type, signed or unsigned. The
default parameter is for backwards compatability, but is
a depreated feature, and may be removed in a future version.
Prefer j.as<int64_t>().

- The json functions `is_integer()` and `is_uinteger()`
have been deprecated and renamed to `is_int64()`, `is_uint64()`.
Prefer j.is<int64_t>() and j.is<uint64_t>().

- The json function `as_uinteger()` has been deprecated.
Prefer j.as<uint64_t>().

and `as_uinteger()` have been deprecated and renamed to 
`is_int64()`, `is_uint64()`, `as_int64()` and `as_uint64()`. 

Change to pull parser API:

- The `stream_filter` function `accept` has been changed to
  take a `const stream_event&` and a `const serializing_context&`.   

- `stream_event_type::bignum_value` has been removed. `stream_event`
  now exposes information about optional semantic tagging through
  the `semantic_tag()` function.

Enhancements:

- `j.as<bignum>()` has been enhanced to return a bignum value
if j is an integer, floating point value, or any string that
contains an optional minus sign character followed by a sequence 
of digits. 

- `j.as<T>()` has been enhanced to support extended integer
types that have `std::numeric_limits` specializations. In particular,
it supports GCC `__int128` and `unsigned __int128` when code is 
compiled with `std=gnu++NN`, allowing a `bignum` to be returned as 
an `__int128` or `unsigned __int128`. (when code is compiled with 
`-std=c++NN`, `__int128` and `unsigned __int128` do not have 
`std::numeric_limits` specializations.)

New feature:

This release accomodate the additional semantics for the 
CBOR data items date-time (a string), and epoch time (a positive or
negative integer or floating point value), and decimal fraction
(converted in the jsoncons data model to a string).

But first, some of the virtual functions in `json_content_handler` 
have to be modified to preserve these semantics. Consequently, 
the function signatures
  
    bool do_int64_value(int64_t, const serializing_context&)

    bool do_uint64_value(uint64_t, const serializing_context&)

    bool do_double_value(double, const floating_point_options&, const serializing_context&)

    bool do_string_value(const string_view_type&, const serializing_context&)
  
    bool do_byte_string_value(const uint8_t*, size_t, const serializing_context&)

have been given an additonal parameter, a `semantic_tag_type`, 
  
    bool do_int64_value(int64_t, semantic_tag_type, const serializing_context&)

    bool do_uint64_value(uint64_t, semantic_tag_type, const serializing_context&)

    bool do_double_value(double, const floating_point_options&, semantic_tag_type, const serializing_context&)

    bool do_string_value(const string_view_type&, semantic_tag_type, const serializing_context&)
  
    bool do_byte_string_value(const uint8_t*, size_t, semantic_tag_type, const serializing_context&)

For consistency, the virtual function

    bool do_bignum_value(const string_view_type&, const serializing_context&)

has been removed, and in its place `do_string_value` will be called
with semantic_tag_type::bignum_type.

For users who have written classes that implement all or part of 
`json_content_handler`, including extensions to `json_filter`, 
these are breaking changes. But otherwise users should be unaffected.

v0.110.2
--------

Continuous integration:

- jsoncons is now cross compiled for ARMv8-A architecture on Travis using 
  clang and executed using the emulator qemu.

- UndefinedBehaviorSanitizer (UBSan) is enabled for selected gcc and clang builds.

Maintenance:

- Removed compiler warnings

v0.110.1
--------

Bug fixes contributed by Cebtenzzre

- Fixed a case where `as_double()`, `as_integer()` etc on a `basic_json` 
  value led to an infinite recursion when the value was a bignum 

- Fixed undefined behavior in bignum class

v0.110.0
--------

### New features

- A JSON pull parser, `json_stream_reader`, has been added. This 
  required a change to the `json_content_handler` function signatures,
  the return values has been changed to bool, to indicate whether 
  parsing is to continue. (An earlier version on master was
  called `json_event_reader`.)    

- `json_parser` has new member function `stopped()`. 

- The `json::is` function now supports `is<jsoncons::string_view>()`,
  and if your compiler has `std::string_view`, `is<std::string_view>()`
  as well. It returns `true` if the json value is a string, otherwise
  `false`.

- The `json::as` function now supports `as<jsoncons::string_view>()`,
  and if your compiler has `std::string_view`, `as<std::string_view>()`
  as well.

### Changes to `json_content_handler` and related streaming classes

#### Non-breaking changes

These changes apply to users that call the public functions defined by
`json_content_handler`, e.g. begin_object, end_object, etc., but are
non-breaking because the old function signatures, while deprecated,
have been preserved. Going forward, however, users should remove
calls to `begin_document`, replace `end_document` with `flush`,
and replace `integer_value` and `end_integer_value` with `int64_value`
and `uint64_value`.

- The public functions defined by `json_content_handler` have been changed 
  to return a bool value, to indicate whether parsing is to continue.

- The function names `integer_value` and `uinteger_value` have been 
  changed to `int64_value` and `uint64_value`.

- The function names `begin_document` and `end_document` have been 
  deprecated. The deprecated `begin_document` does nothing, and the 
  deprecated `end_document` calls `do_flush`.

- The function `flush` has been added, which calls `do_flush`.

- The `json` member function `dump_fragment` has been deprecated, as with
  the dropping of `begin_document` and `end_document`, it is now
  equivalent to `dump`. 

- The function `encode_fragment` has been deprecated, as with
  the dropping of `begin_document` and `end_document`, it is now
  equivalent to `encode_json`. 

- The `json_filter` member function `downstream_handler` has been
  renamed to `destination_handler`.

#### Breaking changes

These changes will affect users who have written classes that implement
all or part of `json_content_handler`, including extensions to `json_filter`.

- The virtual functions defined for `json_content_handler`, `do_begin_object`,
  `do_end_object`, etc. have been changed to return a bool value, to indicate 
  whether serializing or deserializing is to continue. 

- The virtual functions `do_begin_document` and `do_end_document` have been removed.
  A virtual function `do_flush` has been added to allow producers of json events to 
  flush whatever they've buffered.

- The function names `do_integer_value` and `do_uinteger_value` have been changed to 
  `do_int64_value` and `do_uint64_value`.

- The signature of `do_bignum_value` has been changed to 
```
    bool do_bignum_value(const string_view_type& value, 
                         const serializing_context& context)
```

v0.109.0
--------

Enhancements

- Added byte string formatting option `byte_string_chars_format::base16`

Changes

- Scons dropped as a build system for tests and examples, use CMake

- Tests no longer depend on boost, boost test framework replaced by Catch2.
  boost code in tests moved to `examples_boost` directory.

- Previously, if `json_parser` encountered an unopened object or array, e.g. "1]",
  this would cause a JSONCONS_ASSERT failure, resulting in an `std::runtime_error`. This has been
  changed to cause a `json_parse_errc::unexpected_right_brace` or `json_parse_errc::unexpected_right_bracket` error code. 

Warning fixes

- Eliminated vs2017 level 3 and level 4 warnings

v0.108.0
--------

Enhancements

- `bignum_chars_format::base64` is supported

- The incremental parser `json_parser` has been documented

Changes (non-breaking)

- Previously, jsonpointer::get returned values (copies)
  Now, jsonpointer::get returns references if applied to `basic_json`, and values if applied to `cbor_view`

- `bignum_chars_format::string` has been deprecated (still works) and replaced with `bignum_chars_format::base10`

- `json_parser_errc`, `cbor_parser_errc`, and `csv_parser_errc` have been deprecated (still work) and renamed to 
  `json_parse_errc`, `cbor_parse_errc`, and `csv_parse_errc`

v0.107.2
--------

Bug fixes:

- Fixed issue with `UINT_MAX` not declared in `bignum.hpp`

v0.107.1
--------

Bug fixes:

- Fixed issue with cbor_view iterators over indefinite length arrays and maps 

Enhancements:

- csv_serializer recognizes byte strings and bignums.

v0.107.0
--------

Enhancements

- Support for CBOR bignums
- Added json serializing options for formatting CBOR bignums as integer, string, or base64url encoded byte string
- Added json serializing options for formatting CBOR bytes strings as base64 or base64url
- Enhanced interface for `cbor_view` including `dump`, `is<T>`, and `as<t>` functions 

Changes

- If the json parser encounters an integer overflow, the value is now handled as a bignum rather than a double value.

- The `json_content_handler` names `begin_json` and `end_json` have been 
  deprecated and replaced with `begin_document` and `end_document`, and the 
  names `do_begin_json` and `do_end_json` have been removed and replaced with 
  `do_begin_document`, and `do_end_document`. 
  Rationale: meaningfullness across JSON and other data formats including
  CBOR.

Bug fixes:

- Fixed bug in base64url encoding of CBOR byte strings
- Fixed bug in parsing indefinite length CBOR arrays and maps

v0.106.0
--------

Changes

- If a fractional number is read in in fixed format, serialization now preserves
  that fixed format, e.g. if 0.000071 is read in, serialization gives 0.000071
  and not 7.1e-05. In previous versions, the floating point format, whether
  fixed or scientific, was determined by the behavior of snprintf using the g
  conversion specifier.

Bug fix:

- Fixed issue with parsing cbor indefinite length arrays and maps

Warning fix:

- Use memcpy in place of reinterpret_cast in binary data format utility 
  `from_big_endian`

Compiler fix:

- Fixed issues with g++ 4.8

v0.105.0
--------

Enhancements

- The CSV extension now supports multi-valued fields separated by subfield delimiters

- New functions `decode_json` and `encode_json` convert JSON 
  formatted strings to C++ objects and back. These functions attempt to 
  perform the conversion by streaming using `json_convert_traits`, and if
  streaming is not supported, fall back to using `json_type_traits`. `decode_json` 
  and `encode_json` will work for all types that have `json_type_traits` defined.

- The json::parse functions and the json_parser and json_reader constructors 
  optionally take a json_serializing_options parameter, which allows replacing
  a string that matches nan_replacement(), pos_inf_replacement(), and neg_inf_replacement(). 

Changes to Streaming

- The `basic_json_input_handler` and `basic_json_output_handler` interfaces have been 
  combined into one class `basic_json_content_handler`. This greatly simplifies the
  implementation of `basic_json_filter`. Also, the name `parsing_context` has been 
  deprecated and renamed to `serializing_context`, as it now applies to both 
  serializing and deserializing.

  If you have subclassed `json_filter` or have fed JSON events directlty to a 
  `json_serializer`, you shouldn't have to make any changes. In the less likely
  case that you've implemented the `basic_json_input_handler` or 
  `basic_json_output_handler` interfaces, you'll need to change that to
  `json_content_handler`.

Other Changes

- `serialization_traits` and the related `dump` free functions have been deprecated,
  as their functionality has been subsumed by `json_convert_traits` and the
  `encode_json` functions. 

- The option bool argument to indicate pretty printing in the `json` `dump` functions 
  and the `json_serializer` class has been deprecated. It is replaced by the enum class 
  `indenting` with enumerators `indenting::no_indent` and `indenting::indent`.

- The name `serialization_options` has been deprecated (still works) and renamed to 
  `json_serializing_options`. Rationale: naming consistency.

- The `json_reader` `max_nesting_depth` getter and setter functions have been deprecated.
  Use the `json_serializing_options` `max_nesting_depth` getter and setter functions instead.

- The name `csv_parameters` has been deprecated (still works) and renamed to 
  `csv_serializing_options`. Rationale: naming consistency.

v0.104.0
--------

Changes

- `decode_csv` by default now attempts to infer null, true, false, integer and floating point values
  in the CSV source. In previous versions the default was to read everything as strings,
  and other types had to be specified explicitly. If the new default behavior is not desired, the
  `csv_parameters` option `infer_types` can be set to `false`. Column types can still be set explicitly
  if desired.

v0.103.0
--------

Changes

- Default `string_view_type` `operator std::basic_string<CharT,Traits,Allocator>() const` made explicit
  to be consistent with `std::string_view`

- The virtual method `do_double_value` of `json_input_handler` and `json_output_handler` takes a `number_format` parameter

Performance improvements

- Faster json dump to string (avoids streams)
- Faster floating point conversions for linux and MacOSX
- Memory allocation decoding larger string values reduced by half 
- Optimization to json_parser parse_string 
- Improvements to json_decoder

v0.102.1
--------

Bug fix:

Fixed an off-by-one error that could lead to an out of bounds read. Reported by mickcollyer (issue #145)

v0.102.0
--------

Bug fixes:

Fixed issue with how jsonpath filters are applied to arrays in the presence of recursion, resulting in
duplicate results.

Changes:

The signatures of `jsonpointer::get`, `jsonpointer::insert`, `jsonpointer::insert_or_assign`, 
`jsonpointer::remove` and `jsonpointer::replace` have been changed to be consistent
with other functions in the jsoncons library. Each of these functions now has two overloads,
one that takes an `std::error_code` parameter and uses it to report errors, and one that 
throws a `jsonpointer_error` exception to report errors.

The function `jsonpatch::patch` has been replaced by `jsonpatch::apply_patch`, which takes
a json document, a patch, and a `std::error_code&` to report errors. The function
`jsonpatch::diff` has been renamed to `jsonpatch::from_diff`

The old signatures for `encode_cbor` and `encode_msgpack` that returned a `std::vector<uint8_t>` 
have been deprecated and replaced by new signatures that have void return values and have
an output parameter 'std::vector<uint8_t>&'. The rationale for this change is consistency
with other functions in the jsoncons library.

v0.101.0
--------

Fixes:

- Fixes to `string_view` code when `JSONCONS_HAS_STRING_VIEW` is defined in `jsoncons_config.hpp` 

Changes:

- `as_double` throws if `json` value is null (previously returned NaN)

Enhancements:

- Added convenience functions `decode_csv` and `encode_csv`
- Support custom allocaor (currently stateless only) in `json_decoder`, `json_reader`, 
  `csv_reader`, `csv_parameters`

v0.100.2
-------

Resolved warnings on GCC Issue #127

v0.100.1
-------

Fix for platform issue with vs2017:

- Renamed label `minus` to `minus_sign` in `json_parser.hpp` 

Enhancements:

- New classes `byte_string` and `byte_string_view`, to augment support for cbor byte strings in `json` values

v0.100.0
-------

Changes:

- `template <class CharT> json_traits<CharT>` replaced with `sorted_policy` 
- `template <class CharT> o_json_traits<CharT>` replaced with `preserve_order_policy`

- The return type for the json::get_with_default function overload for `const char*` has been
  changed from `const char*` to `json::string_view_type`, which is assignable to `std::string`.

- New functions `byte_string_value` and `do_byte_string_value` have been added to
  `basic_json_input_handler` and `basic_json_output_handler`

- json::is<const char*>() and json::as<const char*>() specializations (supported but never 
  documented) have been deprecated

- In android specific `string_to_double`, `strtod_l` changed to `strtold_l`

Enhancements:

- The `json` class and the `decode_cbor` and `encode_cbor` functions now support byte strings
  A `json` byte string value will, when serialized to JSON, be converted to a base64url string.

- `version.hpp` added to `include` directory

v0.99.9.2
--------

Bug fixes:

- Fixed issue with jsonpatch::diff (fix contributed by Alexander (rog13)) 

Enhancements:

- New class `cbor_view` for accessing packed `cbor` values. A `cbor_view` satisfies the requirements for `jsonpointer::get`.

Changes (non breaking)
----------------------

- `jsonpointer::erase` renamed to `jsonpointer::remove`, old name deprecated

v0.99.9.1
--------

New features
------------

- JSON Pointer implementation

- JSON Patch implementation, includes patch and diff

- `json::insert` function for array that inserts values from range [first, last) before pos. 

Bug fixes

- Fixed issue with serialization of json array of objects to csv file

Changes (non breaking)
----------------------

- The member function name `json::dump_body` has been deprecated and replaced with `json::dump_fragment`. 
- The non member function name `dump_body` has been deprecated and replaced with `dump_fragment`. 

- The class name `rename_name_filter` has been deprecated and replaced with `rename_object_member_filter`.

- In the documentation and examples, the existing function `json::insert_or_assign` 
  is now used instead of the still-supported `json::set`. The reason is that 
  `insert_or_assign` follows the naming convention of the C++ standard library.   

Changes
-------

- The recently introduced class `json_stream_traits` has been renamed to `serialization_traits`

- Removed template parameter `CharT` from class `basic_parsing_context` and renamed it to `parsing_context`

- Removed template parameter `CharT` from class `basic_parse_error_handler` and renamed it to `parse_error_handler`

v0.99.8.2
--------

New features

- Added `json` functions `push_back` and `insert` for appending values 
  to the end of a `json` array and inserting values at a specifed position

Rationale: While these functions provide the same functionality as the existing
`json::add` function, they have the advantage of following the naming conventions 
of the C++ library, and have been given prominence in the examples and documentation 
(`add` is still supported.)

v0.99.8.1
--------

New features

- cbor extension supports encoding to and decoding from the cbor binary serialization format.

- `json_type_traits` supports `std::valarray`

Documentation

- Documentation is now in the repository itself. Please see the documentation
  link in the README.md file

Changed

- Removed `CharT` template parameter from `json_stream_traits`

v0.99.8
------
 
Changes

- Visual Studio 2013 is no longer supported (jsonpath uses string initilizer lists)

- `json_input_handler` overloaded functions value(value,context)` have been deprecated.
  Instead use `string_value(value,context)`, `integer_value(value,context)`, 
  `uinteger_value(value,context)`, `double_value(value,precision,context)`, 
  `bool_value(value,context)` and `null_value(context)`

- `json_output_handler` overloaded functions value(value)` have been deprecated.
  Instead use `string_value(value)`, `integer_value(value)`, `uinteger_value(value)`,
  `double_value(value,precision=0)`, `bool_value(value)` and `null_value(context)`

- For consistency, the names `jsoncons_ext/msgpack/message_pack.hpp`, 
  `encode_message_pack` and `decode_message_pack` have been deprecated and 
  replaced with `jsoncons_ext/msgpack/msgpack.hpp`, `encode_msgpack` and `decode_msg_pack`

Bug fixes

- Fixed operator== throws when comparing a string against an empty object

- Fixed jsonpath issue with array 'length' (a.length worked but not a['length'])

- `msgpack` extension uses intrinsics for determing whether to swap bytes 

New features

- Stream supported C++ values directly to JSON output, governed by `json_stream_traits` 

- json::is<T>() and json::as<T>() accept template packs, which they forward to the 
  `json_type_traits` `is` and `as` functions. This allows user defined `json_type_traits` 
  implementations to resolve, for instance, a name into a C++ object looked up from a
  registry. See [Type Extensibility](https://github.com/danielaparker/jsoncons), Example 2. 

- jsonpath `json_query` now supports returning normalized paths (with
  optional `return_type::path` parameter)

- New jsonpath `max` and `min` aggregate functions over numeric values

- New `json::merge` function that inserts another json object's key-value pairs 
  into a json object, if they don't already exist.

- New `json::merge_or_update` function that inserts another json object's key-value 
  pairs into a json object, or assigns them if they already exist.

v0.99.7.3
--------

- `json_type_traits` supports `std::pair` (convert to/from json array of size 2)

- `parse_stream` renamed to `parse` (backwards compatible)

- `kvp_type` renamed to `key_value_pair_type` (backwards compatible)

- The `_json` and `_ojson` literal operators have been moved to the namespace `jsoncons::literals`.
  Access to these literals now requires
```c++
    using namespace jsoncons::literals;    
```
Rationale: avoid name clashes with other `json` libraries        

- The name `owjson` has been deprecated (still works) and changed to `wojson`. Rationale: naming consistency

- Added json array functions `emplace_back` and `emplace`, and json object functions `try_emplace`
  and `insert_or_assign`, which are analagous to the standard library vector and map functions. 

v0.99.7.2
--------

Bug fix

- A bug was introduced in v0.99.7 causing the values of existing object members to not be changed wiht set or assignment operations. This has been fixed.

Change

- jsoncons_ext/binary changed to jsoncons_ext/msgpack
- namespace jsoncons::binary changed to jsoncons::msgpack

v0.99.7.1
--------

- Workarounds in unicode_traits and jsonpath to maintain support for vs2013 
- Added `mapping_type::n_rows`, `mapping_type::n_objects`, and `mapping_type::m_columns` options for csv to json 

v0.99.7
------

Bug fixes

- Issues with precedence in JsonPath filter evaluations have been fixed
- An issue with (a - expression) in JsonPath filter evaluations has been fixed

New feature

- The new binary extension supports encoding to and decoding from the MessagePack binary serialization format.
- An extension to JsonPath to allow filter expressions over a single object.
- Added support for `*` and `/` operators to jsonpath filter
- literal operators _json and _ojson have been introduced

Non-breaking changes

- The `json` `write` functions have been renamed to `dump`. The old names have been deprecated but still work.
- Support for stateful allocators
- json function object_range() now returns a pair of RandomAccessIterator (previously BidirectionalIterator)
- json operator [size_t i] applied to a json object now returns the ith object (previously threw) 

Breaking change (if you've implemented your own input and output handlers)

In basic_json_input_handler, the virtual functions
```c++
virtual void do_name(const CharT* value, size_t length, 
                     const basic_parsing_context<CharT>& context)

virtual void do_string_value(const CharT* value, size_t length, 
                             const basic_parsing_context<CharT>& context)
```
have been changed to
```c++
virtual void do_name(string_view_type val, 
                     const basic_parsing_context<CharT>& context) 

virtual void do_string_value(string_view_type val, 
                             const basic_parsing_context<CharT>& context) 
```

In basic_json_output_handler, the virtual functions
```c++
virtual void do_name(const CharT* value, size_t length) 

virtual void do_string_value(const CharT* value, size_t length) 
```
have been changed to
```c++
virtual void do_name(string_view_type val)

virtual void do_string_value(string_view_type val)
```

Removed features:

- The jsonx extension has been removed 

v0.99.5
------

- Validations added to utf8 and utf16 string parsing to pass all [JSONTestSuite](https://github.com/nst/JSONTestSuite) tests
- The name `json_encoder` introduced in v0.99.4 has been changed to `json_decoder`. Rationale: consistencty with common usage (encoding and serialization, decoding and deserialization)

v0.99.4a
-------

Fixes Issue #101, In json.hpp, line 3376 change "char__type" to "char_type" 
Fixes Issue #102, include cstring and json_error_category.hpp in json.hpp

v0.99.4
------

Changes

- The deprecated class `json::any` has been removed. 
- The jsoncons `boost` extension has been removed. That extension contained a sample `json_type_traits` specialization for `boost::gregorian::date`, which may still be found in the "Type Extensibility" tutorial.  
- The member `json_type_traits` member function `assign` has been removed and replaced by `to_json`. if you have implemented your own type specializations, you will also have to change your `assign` function to `to_json`.
- `json_type_traits` specializations no longer require the `is_assignable` data member

Non-breaking name changes

- The names `json_deserializer`,`ojson_deserializer`,`wjson_deserializer`,`owjson_deserializer` have been deprecated (they still work) and replaced by `json_encoder<json>`, `json_encoder<ojson>`, `json_encoder<wjson>` and `json_encoder<owjson>`.  
- The name `output_format` has been deprecated (still works) and renamed to `serialization_options`.  
- The name `wojson` has been deprecated (still works) and renamed to `owjson`.  
- The `json_filter` member function `input_handler` has been deprecated (still works) and renamed to `downstream_handler`.  
- The name `elements` has been deprecated (still works) and renamed to `owjson`.  
- The `json` member function `members()` has been deprecated (still works) and renamed to `object_range()`.  
- The `json` member function `elements()` has been deprecated (still works) and renamed to `array_range()`.  
- The `json` member_type function `name()` has been  deprecated (still works) and renamed to `key()`. Rationale: consistency with more general underlying storage classes.

New features

- `json_filter` instances can be passed to functions that take a `json_output_handler` argument (previously only a `json_input_handler` argument)
- New `jsonpath` function `json_replace` that searches for all values that match a JsonPath expression and replaces them with a specified value.
- `json` class has new method `has_key()`, which returns `true` if a `json` value is an object and has a member with that key
- New filter class `rename_name` allows search and replace of JSON object names

v0.99.3a
-------

Changes

The `json` initializer-list constructor has been removed, it gives inconsistent results when an initializer has zero elements, or one element of the type being initialized (`json`). Please replace

`json j = {1,2,3}` with `json j = json::array{1,2,3}`, and 

`json j = {{1,2,3},{4,5,6}}` with `json j = json::array{json::array{1,2,3},json::array{4,5,6}}`

- Initializer-list constructors are now supported in `json::object` as well as `json::array`, e.g.
```c++
json j = json::object{{"first",1},{"second",json::array{1,2,3}}};
```

- json::any has been deprecated and will be removed in the future

- The json method `to_stream` has been renamed to `write`, the old name is still supported.

- `output_format` `object_array_block_option`, `array_array_block_option` functions have been deprecated and replaced by 
   `object_array_split_lines`, `array_array_split_lines` functions. 

Enhancements

- A new method `get_with_default`, with return type that of the default, has been added to `json`

- A new template parameter, `JsonTraits`, has been added to the `basic_json` class template. 

- New instantiations of `basic_json`, `ojson` and `wojson`, have been added for users who wish to preserve the alphabetical sort of parsed json text and to insert new members in arbitrary name order.

- Added support for `json` `is<T>`, `as<T>`, constructor, and assignment operator for any sequence container (`std::array`, `std::vector`, `std::deque`, `std::forward_list`, `std::list`) whose values are assignable to JSON types (e.g., ints, doubles, bools, strings, STL containers of same) and for associative containers (`std::set`, `std::multiset`, `std::unordered_set`, `std::unordered_multiset`.)

- Added static method `null()` to `json` class to return null value

- A new extension jsonx that supports serializing JSON values to [JSONx](http://www.ibm.com/support/knowledgecenter/SS9H2Y_7.5.0/com.ibm.dp.doc/json_jsonx.html) (XML)

- json parser will skip `bom` in input if present

Fixes:

- Fixes to the `jsonpath` extension, including the union operator and applying index operations to string values

- Fixes to remove warnings and issues reported by VS2015 with 4-th warnings level, PVS-Studio static analyzer tool, and UBSAN. 

v0.99.2
------

- Included workaround for a C++11 issue in GCC 4.8, contributed by Alex Merry
- Fixed operator== so that json() == json(json::object())
- Fixed issue with `json` assignment to initializer list
- Fixed issue with assignment to empty json object with multiple keys, e.g. 

    json val; 
    val["key1"]["key2"] = 1; 

v0.99.1
------

- Fix to json_filter class
- Fix to readme_examples

v0.99
----

- Fixes to deprecated json parse functions (deprecated, but still supposed to work)
- The Visual C++ specific implementation for reading floating point numbers should have freed a `_locale_t` object, fixed 
- Added `json_type_traits` specialization to support assignment from non-const strings 
- When parsing fractional numbers in text, floating point number precision is retained, and made available to serialization to preserve round-trip. The default output precision has been changed from 15 to 16.
- Added json `std::initializer_list` constructor for constructing arrays
- The deprecated json member constants null, an_object, and an_array have been removed
- Microsoft VC++ versions earlier than 2013 are no longer supported

v0.98.4
------

- Fixes issues with compilation with clang

v0.98.3
------

New features

- Supports [Stefan Goessner's JsonPath](http://goessner.net/articles/JsonPath/).
- json member function `find` added
- json member function `count` added
- json array range accessor `elements()` added, which supports range-based for loops over json arrays, and replaces `begin_elements` and `end_elements`
- json object range accessor `members()` added, which supports range-based for loops over json objects, and replaces `begin_members` and `end_members`
- New version of json `add` member function that takes a parameter `array_iterator` 
- json member function `shrink_to_fit` added

API Changes 

- The json internal representation of signed and unsigned integers has been changed from `long long` and `unsigned long long` to `int64_t` and `uint64_t`. This should not impact you unless you've implemented your own `json_input_handler` or `json_output_handler`, in which case you'll need to change your `json_input_handler` function signatures
 
    void do_longlong_value(long long value, const basic_parsing_context<Char>& context) override
    void do_ulonglong_integer_value(unsigned long long value, const basic_parsing_context<Char>& context) override

to

    void do_integer_value(int64_t value, const basic_parsing_context<Char>& context) override
    void do_uinteger_value(uint64_t value, const basic_parsing_context<Char>& context) override
  
and your `json_output_handler` function signatures from     

    void do_longlong_value(long long value) override
    void do_ulonglong_integer_value(unsigned long long value) override

to

    void do_integer_value(int64_t value) override
    void do_uinteger_value(uint64_t value) override

- `output_format` drops support for `floatfield` property

Non-beaking API Changes
- remove_range has been deprecated, use erase(array_iterator first, array_iterator last) instead
- remove has been deprecated, use erase(const std::string& name ) instead
- `json::parse_string` has been renamed to `json::parse`, `parse_string` is deprecated but still works
- `json member function `is_empty` has been renamed to `empty`, `is_empty` is deprecated but still works. Rationale: consistency with C++ containers
- json member functions `begin_elements` and `end_elements` have been deprecated, instead use `elements().begin()` and `elements.end()`
- json member functions `begin_members` and `end_members` have been deprecated, instead use `members().begin()` and `members.end()`
- json member function `has_member` has been deprecated, instead use `count`. Rationale: consistency with C++ containers
- json member function `remove_member` has been deprecated, instead use `remove`. Rationale: only member function left with _element or _member suffix 
- json_parse_exception renamed to parse_error, json_parse_exception typedef to parse_error
- json::parse(std::istream& is) renamed to json::parse_stream. json::parse(std::istream is) is deprecated but still works.

v0.98.2 Release
--------------

- `json` constructor is now templated, so constructors now accept extended types
- Following [RFC7159](http://www.ietf.org/rfc/rfc7159.txt), `json_parser` now accepts any JSON value, removing the constraint that it be an object or array.
- The member `json_type_traits` member functions `is`, `as`, and `assign` have been changed to static functions. if you have implemented your own type specializations, you will also have to change your `is`, `as` and `assign` functions to be static.
- Removed json deprecated functions `custom_data`, `set_custom_data`, `add_custom_data`
- `json_reader` member function `max_depth` has been renamed to `max_nesting_depth`, the former name is still supported. 
- `json` member function `resize_array` has been renamed to `resize`, the former name is still supported. 

jsoncons supports alternative ways for constructing  `null`, `object`, and `array` values.

null:

    json a = jsoncons::null_type();  // Using type constructor
    json b = json::null_type();      // Using alias
    json c(json::null);              // From static data member prototype

object:

    json a();                 // Default is empty object
    json b = json::object();  // Using type constructor
    json c(json::an_object);  // From static data member prototype

array:

    json a = json::array();      // Using type constructor
    json b = json::make_array(); // Using factory method
    json c(json::an_array);      // From static data member prototype

Since C++ has possible order issues with static data members, the jsoncons examples and documentation have been changed to consistently use the other ways, and `json::null`, `json::an_object` and `json::an_array` have been, while still usable, deprecated.

v0.98.1 Release
--------------

- Enhances parser for CSV files that outputs JSON, see example below. 
- Adds `get_result` member function to `json_deserializer`, which returns the json value `v` stored in a `json_deserializer` as `std::move(v)`. The `root()` member function has been deprecated but is still supported.
- Adds `is_valid` member function to `json_deserializer`
- Enhances json::any class, adds type checks when casting back to original value
- Fixes some warning messages

v0.98 Release
--------------

Bug fixes:

- Fixes the noexcept specification (required for Visual Studio 2015 and later.) Fix
  contributed by Rupert Steel.
- Fixes bug with proxy operator== when comparing object member values,
  such as in val["field"] == json("abc")

Enhancements:

- Refines error codes and improves error messages

- Renames `json_reader` method `read` to `read_next`, reflecting the fact that it supports reading a sequence of JSON texts from a stream. The 
  former name is deprecated but still works.

- Adds `json_reader` method `check_done` that throws if there are unconsumed non-whitespace characters after one or more calls to `read_next`.

- Adds getter and setter `max_depth` methods to allow setting the maximum JSON parse tree depth if desired, by default
it is arbitrarily large (limited by heap memory.)

- Modifies `json` static methods `parse_string`, `parse_file`, and `parse` behaviour to throw if there are unconsumed non-whitespace characters after reading one JSON text.  

Changes to extensions:

- Changes the top level namespace for the extensions from `jsoncons_ext` to `jsoncons`, e.g. `jsoncons_ext::csv::csv_reader` becomes `jsoncons::csv::csv_reader`
- Modifies csv_reader and csv_serializer so that the constructors are passed parameters in a `csv_parameters` object rather than a `json` object.
- Adds more options to csv_reader

v0.97.2 Release
--------------

- Incorporates test suite files from http://www.json.org/JSON_checker/ into test suite
- The `jsoncons` parser accepts all of the JSON_checker files that its supposed to accept.
- Failures to reject incorrect exponential notation (e.g. [0e+-1]) have been fixed.
- The `jsoncons` parser now rejects all of the JSON_checker files that its supposed to reject except ones with stuff after the end of the document, e.g.

    ["Extra close"]]

  (Currently the `jsoncons` parser stops after reading a complete JSON text, and supports reading a sequence of JSON texts.)  

- Incorporates a fix to operator== on json objects, contributed by Alex Merry

v0.97.1 Release
--------------

- "Transforming JSON with filters" example fixed
- Added a class-specific in-place new to the json class that is implemented in terms of the global version (required to create json objects with placement new operator.)
- Reorganized header files, removing unnecessary includes. 
- Incorporates validation contributed by Alex Merry for ensuring that there is an object or array on parse head.
- Incorporates fix contributed by Milan Burda for “Switch case is in protected scope” clang build error

v0.97 Release
------------

- Reversion of v0.96 change:

The virtual methods `do_float_value`, `do_integer_value`, and `do_unsigned_value` of `json_input_handler` and `json_output_handler` have been restored to `do_double_value`, `do_longlong_value` and `do_ulonglong_value`, and their typedefed parameter types `float_type`, `integer_type`, and `unsigned_type` have been restored to `double`, `long long`, and `unsigned long long`.

The rationale for this reversion is that the change doesn't really help to make the software more flexible, and that it's better to leave out the typedefs. There will be future enhancements to support greater numeric precision, but these will not affect the current method signatures.

- Fix for "unused variable" warning message

v0.96 Release
------------

This release includes breaking changes to interfaces. Going forward, the interfaces are expected to be stable.

Breaking changes:

- Renamed `error_handler` to `parse_error_handler`.

- Renamed namespace `json_parser_error` to `json_parser_errc`

- Renamed `value_adapter` to `json_type_traits`, if you have implemented your own type specializations,
  you will have to rename `value_adapter` also.

- Only json arrays now support `operator[](size_t)` to loop over values, this is no longer supported for `json` objects. Use a json object iterator instead.

- The virtual methods `do_double_value`, `do_integer_value` and `do_uinteger_value` of `json_input_handler` and `json_output_handler` have been renamed to `do_float_value`, `do_integer_value`, and `do_unsigned_value`, 
  and their parameters have been changed from `double`, `long long`, and `unsigned long long` to typedefs `float_type`, `integer_type`, and `unsigned_type`.
  The rationale for this change is to allow different configurations for internal number types (reversed in v0.97.)

General changes

- `json` member function `begin_object` now returns a bidirectional iterator rather than a random access iterator.

- Static singleton `instance` methods have been added to `default_parse_error_handler`
  and `empty_json_input_handler`. 

- Added to the `json` class overloaded static methods parse, parse_string 
  and parse_file that take a `parse_error_handler` as a parameter. 

- Added methods `last_char()` and `eof()` to `parsing_context`.

- Enhancements to json parsing and json parse event error notification.

- Added to `json_input_handler` and `json_output_handler` a non virtual method `value` that takes a null terminated string.

- Added methods `is_integer`, `is_unsigned` and `is_float` to `json` to replace `is_longlong`, `is_ulonglong` and `is_double`, which have been deprecated.

- Added methods `as_integer`, `as_unsigned` and `as_float` to `json` to replace `is_longlong`, `is_ulonglong` and `is_double`, which have been deprecated.

Bug fixes:

- Fixed issue with column number reported by json_reader

- Where &s[0] and s.length() were passed to methods, &s[0] has been replaced with s.c_str(). 
  While this shouldn't be an issue on most implementations, VS throws an exception in debug modes when the string has length zero.

- Fixes two issues in v0.95 reported by Alex Merry that caused errors with GCC: a superfluous typename has been removed in csv_serializer.hpp, and a JSONCONS_NOEXCEPT specifier has been added to the json_error_category_impl name method.

- Fixed a number of typename issues in the v0.96 candidate identifed by Ignatov Serguei.

- Fixes issues with testsuite cmake and scons reported by Alex Merry and Ignatov Serguei

v0.95
----

Enhancements:

- Added template method `any_cast` to `json` class.

- The allocator type parameter in basic_json is now supported, it allows you to supply a 
  custom allocator for dynamically allocated, fixed size small objects in the json container.
  The allocator type is not used for structures including vectors and strings that use large 
  or variable amounts of memory, these always use the default allocators.

Non-breaking Change:
 
- `json_filter` method `parent` has been renamed to `input_handler` (old name still works) 

Breaking change (if you've implemented your own input and output handlers, or if you've
passed json events to input and output handlers directly):

- The input handler virtual method 
  `name(const std::string& name, const parsing_context& context)` 
  has been changed to
  `do_name(const char* p, size_t length, const parsing_context& context)` 

- The output handler virtual method 
  `name(const std::string& name)` 
  has been changed to
  `do_name(const char* p, size_t length)` 

- The input handler virtual method 
  `string_value(const std::string& value, const parsing_context& context)` 
  has been changed to
  `do_string_value(const char* p, size_t length, const parsing_context& context)` 

- The output handler virtual method 
  `string_value(const std::string& value)` 
  has been changed to
  `do_string_value(const char* p, size_t length)` 

The rationale for the method parameter changes is to allow different internal
representations of strings but preserve efficiency. 

- The input and output handler virtual implementation methods begin_json, end_json,
  begin_object, end_object, begin_array, end_array, name, string_value, 
  longlong_value, ulonglong_value, double_value, bool_value and null_value 
  have been renamed to do_begin_json, do_end_json, do_begin_object, do_end_object, 
  do_begin_array, do_end_array, do_name, do_string_value, do_longlong_value, 
  do_ulonglong_value, do_double_value, do_bool_value and do_null_value and have been 
  made private. 
  
- Public non-virtual interface methods begin_json, end_json,
  begin_object, end_object, begin_array, end_array, name
  have been added to json_input_handler and json_output_handler. 

The rationale for these changes is to follow best C++ practices by making the
json_input_handler and json_output_handler interfaces public non-virtual and 
the implementations private virtual. Refer to the documentation and tutorials for details.   

- The error_handler virtual implementation methods have been renamed to `do_warning` and 
  `do_error`, and made private. Non virtual public interface methods `warning` and `error` 
  have been added. Error handling now leverages `std::error_code` to communicate parser 
  error events in an extendable way.

Bug fixes:

- Fixed bug in csv_reader

v0.94.1
------

Bug fixes:

- Incorporates fix from Alex Merry for comparison of json objects

v0.94
----

Bug fixes 

- Incorporates contributions from Cory Fields for silencing some compiler warnings
- Fixes bug reported by Vitaliy Gusev in json object operator[size_t]
- Fixes bug in json is_empty method for empty objects

Changes

- json constructors that take string, double etc. are now declared explicit (assignments and defaults to get and make_array methods have their own implementation and do not depend on implicit constructors.)
- make_multi_array renamed to make_array (old name is still supported)
- Previous versions supported any type values through special methods set_custom_data, add_custom_data, and custom_data. This version introduces a new type json::any that wraps any values and works with the usual accessors set, add and as, so the specialized methods are no longer required.

Enhancements 

- json get method with default value now accepts extended types as defaults
- json make_array method with default value now accepts extended types as defaults

New extensions

- Added jsoncons_ext/boost/type_extensions.hpp to collect 
  extensions traits for boost types, in particular, for
  boost::gregorian dates.

v0.93 Release
------------

New features

- Supports wide character strings and streams with wjson, wjson_reader etc. Assumes UTF16 encoding if sizeof(wchar_t)=2 and UTF32 encoding if sizeof(wchar_t)=4.
- The empty class null_type  is added to the jsoncons namespace, it replaces the member type json::null_type (json::null_type is typedefed to jsoncons::null_type for backward compatibility.)

Defect fixes:

- The ascii character 0x7f (del) was not being considered a control character to be escaped, this is fixed.
- Fixed two issues with serialization when the output format property escape_all_non_ascii is enabled. One, the individual bytes were being checked if they were non ascii, rather than first converting to a codepoint. Two, continuations weren't being handled when decoding.

v0.92a Release
-------------

Includes contributed updates for valid compilation and execution in gcc and clang environments

v0.92 Release
------------

Breaking change (but only if you have subclassed json_input_handler or json_output_handler)

- For consistency with other names, the input and output handler methods new to v0.91 - value_string, value_double, value_longlong, value_ulonglong and value_bool - have been renamed to string_value, double_value, longlong_value, ulonglong_value and bool_value.

Non breaking changes (previous features are deprecated but still work)

- name_value_pair has been renamed to member_type (typedefed to previous name.)

- as_string(output_format format) has been deprecated, use the existing to_string(output_format format) instead

Enhancements:

- json now has extensibilty, you can access and modify json values with new types, see the tutorial Extensibility 

Preparation for allocator support:

- The basic_json and related classes now have an Storage template parameter, which is currently just a placeholder, but will later provide a hook to allow users to control how json storage is allocated. This addition is transparent to users of the json and related classes.

v0.91 Release
------------

This release should be largely backwards compatible with v0.90 and 0.83 with two exceptions: 

1. If you have used object iterators, you will need to replace uses of std::pair with name_value_pair, in particular, first becomes name() and second becomes value(). 

2. If you have subclassed json_input_handler, json_output_handler, or json_filter, and have implemented value(const std::string& ...,  value(double ..., etc., you will need to modify the names to  value_string(const std::string& ...,  value_double(double ... (no changes if you are feeding existing implementations.)

The changes are

- Replaced std::pair<std::string,json> with name_value_pair that has accessors name() and value()

- In json_input_handler and json_output_handler, allowed for overrides of the value methods by making them non-virtual and adding virtual methods value_string, value_double, value_longlong, value_ulonglong, and value_bool

Other new features:

- Changed implementation of is<T> and as<T>, the current implementation should be user extensible

- make_multi_array<N> makes a multidimensional array with the number of dimensions specified as a template parameter. Replaces make_2d_array and make_3d_array, which are now deprecated.

- Added support for is<std::vector<T>> and as<std::vector<T>>

- Removed JSONCONS_NO_CXX11_RVALUE_REFERENCES, compiler must support move semantics

Incorporates a number of contributions from Pedro Larroy and the developers of the clearskies_core project:

- build system for posix systems
- GCC to list of supported compilers
- Android fix
- fixed virtual destructors missing in json_input_handler, json_output_handler and parsing_context
- fixed const_iterator should be iterator in json_object implementation 

To clean up the interface and avoid too much duplicated functionality, we've deprecated some json methods (but they still work)

    make_array
Use json val(json::an_array) or json::make_multi_array<1>(...) instead (but make_array will continue to work)

    make_2d_array
    make_3d_array
Use make_multi_array<2> and make_multi_array<3> instead

    as_vector
Use as<std::vector<int>> etc. instead

    as_int
    as_uint
    as_char
Use as<int>, as<unsigned int>, and as<char> instead

Release v0.90a
-------------

Fixed issue affecting clang compile

Release v0.90
------------

This release should be fully backwards compatible with 0.83. 

Includes performance enhancements to json_reader and json_deserializer

Fixes issues with column numbers reported with exceptions

Incorporates a number of patches contributed by Marc Chevrier:

- Fixed issue with set member on json object when a member with that name already exists
- clang port
- cmake build files for examples and test suite
- json template method is<T> for examining the types of json values
- json template method as<T> for accessing json values

v0.83
------------

Optimizations (very unlikely to break earlier code)

- get(const std::name& name) const now returns const json& if keyed value exists, otherwise a const reference to json::null

- get(const std::string& name, const json& default_val) const now returns const json (actually a const proxy that evaluates to json if read)

Bug fixes

- Line number not incremented within multiline comment - fixed

Deprecated features removed

- Removed deprecated output_format properties (too much bagage to carry around)

v0.82a
-------------

- The const version of the json operator[](const std::string& name) didn't need to return a proxy, the return value has been changed to const json& (this change is transparent to the user.) 

- get(const std::name& name) has been changed to return a copy (rather than a reference), and json::null if there is no member with that name (rather than throw.) This way both get methods return default values if no keyed value exists.

- non-const and const methods json& at(const std::name& name) have been added to replace the old single argument get method. These have the same behavior as the corresponding operator[] functions, but the non-const at is more efficient.

v0.81
------------

- Added accessor and modifier methods floatfield to output_format to provide a supported way to set the floatfield format flag to fixed or scientific with a specified number of decimal places (this can be done in older versions, but only with deprecated methods.)

- The default constructor now constructs an empty object (rather than a null object.) While this is a change, it's unlikely to break exisitng code (all test cases passed without modification.)

This means that instead of

    json obj(json::an_object);
    obj["field"] = "field";

you can simply write

    json obj;
    obj["field"] = "field";

The former notation is still supported, though.

- Added a version of 'resize_array' to json that resizes the array to n elements and initializes them to a specified value.

- Added a version of the static method json::make_array that takes no arguments and makes an empty json array

Note that

    json arr(json::an_array);

is equivalent to

    json arr = json::make_array();

and

    json arr(json::an_array);
    arr.resize_array(10,0.0);

is equivalent to

    json arr = json::make_array(10,0.0);

For consistency the json::make_array notation is now favored in the documentation. 

v0.71
-------------

- Added resize_array method to json for resizing json arrays 

- Fixed issue with remove_range method (templated code failed to compile if calling this method.)

- Added remove_member method to remove a member from a json object

- Fixed issue with multiline line comments, added test case

- Fixed issue with adding custom data to a json array using add_custom_data, added examples.

v0.70
-------------

- Since 0.50, jsoncons has used snprintf for default serialization of double values to string values. This can result in invalid json output when running on a locale like German or Spanish. The period character (â€˜.â€™) is now always used as the decimal point, non English locales are ignored.

- The output_format methods that support alternative floating point formatting, e.g. fixed, have been deprecated.

- Added a template method as_vector<T> to the json class. If a json value is an array and conversion is possible to the template type, returns a std::vector of that type, otherwise throws an std::exception. Specializations are provided for std::string, bool, char, int, unsigned int, long, unsigned long, long long, unsigned long long, and double. For example

    std::string s("[0,1,2,3]");

    json val = json::parse_string(s);

    std::vector<int> v = val.as_vector<int>(); 

- Undeprecated the json member function precision

v0.60b
-------------

This release (0.60b) is fully backwards compatible with 0.50.

A change introduced with 0.60 has been reversed. 0.60 introduced an alternative method of constructing a json arrray or object with an initial default constructor, a bug with this was fixed in 0.60a, but this feature and related documentation has been removed because it added complexity but no real value.

### Enhancements

- Added swap member function to json

- Added add and add_custom_data overrides to json that take an index value, for adding a new element at the specified index and shifting all elements currently at or above that index to the right.

- Added capacity member functions to json

### 0.60  extensions

- csv_serializer has been added to the csv extension

v0.50
------------

This release is fully backwards compatible with 0.4*, and mostly backwards compatible to 0.32 apart from the two name changes in 0.41

Bug fixes

- When reading the escaped characters "\\b", "\\f", "\\r" and "\\t" appearing in json strings, json_reader was replacing them with the linefeed character, this has been fixed.

Deprecated 

- Deprecated modifiers precision and fixed_decimal_places from output_format. Use set_floating_point_format instead.
- Deprecated constructor that takes indenting parameter from output_format. For pretty printing with indenting, use the pretty_print function or pass the indenting parameter in json_serializer.

Changes

- When serializing floating point values to a stream, previous versions defaulted to default floating point precision with a precision of 16. This has been changed to truncate trailing zeros  but keep one if immediately after a decimal point.

New features

- For line reporting in parser error messages, json_reader now recognizes \\r\\n, \\n alone or \\r alone (\\r alone is new.)
- Added set_floating_point_format methods to output_format to give more control over floating point notation.

Non functional enhancements

- json_reader now estimates the minimum capacity for arrays and objects, and reports that information for the begin_array and begin_object events. This greatly reduces reallocations.

v0.42
------------

- Fixed another bug with multi line /**/ comments 
- Minor fixes to reporting line and column number of errors
- Added fixed_decimal_places setter to output_format
- Added version of as_string to json that takes output_format as a parameter
- Reorganization of test cases and examples in source tree

v0.41
------------

- Added non-member overload swap(json& a, json& b)
- Fixed bug with multi line /**/ comments 
- Added begin_json and end_json methods to json_output_handler
- json_deserializer should now satisfy basic exception safety (no leak guarantee)
- Moved csv_reader.hpp to jsoncons_ext/csv directory
- Changed csv_reader namespace to jsoncons::csv
- json::parse_file no longer reads the entire file into memory before parsing
  (it now uses json_reader default buffering)

v0.40
------------

- json_listener renamed to json_input_handler
- json_writer renamed to json_output_handler

- Added json_filter class

- json get method that takes default argument now returns a value rather than a reference
- Issue in csv_reader related to get method issue fixed
- Issue with const json operator[] fixed
- Added as_char method to json
- Improved exception safety, some opportunites for memory leaks in the presence of exceptions removed

v0.33
------------

Added reserve method to json

Added static make_3d_array method to json

json_reader now configured for buffered reading

Added csv_reader class for reading CSV files and producing JSON events

Fixed bug with explicitly passing output_format in pretty_print.

v0.32
------------

Added remove_range method, operator== and  operator!= to proxy and json objects

Added static methods make_array and make_2d_array to json

v0.31
------------

error_handler method content_error renamed to error

Added error_code to warning, error and fatal_error methods of error_handler

v0.30
------------

json_in_stream renamed to json_listener

json_out_stream renamed to json_writer

Added buffer accessor method to parsing_context

v0.20
------------

Added parsing_context class for providing information about the
element being parsed.

error_handler methods take message and parsing_context parameters

json_in_stream handlers take parsing_context parameter

v0.19
------------

Added error_handler class for json_reader

Made json_exception a base class for all json exceptions

Added root() method to json_deserializer to get a reference to the json value

Removed swap_root() method from json_deserializer

v0.18
------------

Renamed serialize() class method to to_stream() in json  

Custom data serialization supported through template function specialization of serialize
(reverses change in 0.17)


v0.17
------------

Added is_custom() method to json and proxy

get_custom() method renamed to custom_data() in json and proxy

Added clear() method to json and proxy

set_member() method renamed to set()

set_custom() method renamed to set_custom_data()

push_back() method renamed to add() in json and proxy

Added add_custom_data method() in json and proxy

Custom data serialization supported through template class specialization of custom_serialization
(replaces template function specialization of serialize)

v0.16
------------

Change to json_out_stream and json_serializer:

    void value(const custom_data& value)

removed.

Free function serialize replaces free function to_stream for
serializing custom data.

pretty print tidied up for nested arrays

v0.15
------------

Made eof() method on json_reader public, to support reading
multiple JSON objects from a stream.

v0.14
------------

Added pretty_print class

Renamed json_stream_writer to json_serializer, 
implements pure virtual class json_out_stream
 
Renamed json_stream_listener to json_deserializer
implements pure virtual class json_in_stream

Renamed json_parser to json_reader, parse to read.

Changed indenting so object and array members start on new line.

Added support for storing user data in json object, and
serializing to JSON.

v0.13
------------

Replaced simple_string union member with json_string that 
wraps std::basic_string<Char>

name() and value() event handler methods on 
basic_json_stream_writer take const std::basic_string<Char>&
rather than const Char* and length.

v0.12
------------

Implemented operator<< for json::proxy

Added to_stream methods to json::proxy

v0.11
------------

Added members to json_parser to access and modify the buffer capacity

Added checks when parsing integer values to determine overflow for 
long long and unsigned long long, and if overflow, parse them as
doubles. 


