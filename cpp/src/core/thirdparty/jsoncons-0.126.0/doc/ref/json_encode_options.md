### jsoncons::json_encode_options

```c++
typedef basic_json_encode_options<char> json_encode_options
```

An abstract class that defines accessors for JSON encoding options. The `json_encode_options` class is an instantiation of the `basic_json_encode_options` class template that uses `char` as the character type.

#### Header
```c++
#include <jsoncons/json_options.hpp>
```

#### Implementing classes

[json_options](json_options.md)

#### Destructor

    virtual ~json_encode_options();

#### Accessors

    virtual size_t indent_size() const = 0;
The indent size, the default is 4.

    virtual spaces_option spaces_around_colon() const = 0;
Indicates [space option](spaces_option.md) for name separator (`:`). Default
is space after.

    virtual spaces_option spaces_around_comma() const = 0;
Indicates [space option](spaces_option.md) for array value and object name/value pair separators (`,`). Default
is space after.

    virtual bool pad_inside_object_braces() const = 0;
Default is `false`

    virtual bool pad_inside_array_brackets() const = 0;
Default is `false`

    virtual chars_format floating_point_format() const = 0 
Overrides [floating point format](chars_format.md) when serializing json.
The default, for a floating point value that was previously decoded from json text, is to preserve the original format when serializing.
The default, for a floating point value that was directly inserted into a json value, to serialize with [chars_format::general](chars_format.md). 

    virtual bigint_chars_format bigint_format() const = 0 
Overrides [bignum format](bigint_chars_format.md) when serializing json.
The default is [bigint_chars_format::base10](bigint_chars_format.md). 

    virtual byte_string_chars_format byte_string_format() const = 0 
Overrides [byte string format](byte_string_chars_format.md) when serializing json.
The default is [byte_string_chars_format::base64url](byte_string_chars_format.md). 

    virtual int precision() const = 0 
Overrides floating point precision when serializing json. 
The default, for a floating point value that was previously decoded from json text, is to preserve the original precision. 
The fefault, for a floating point value that was directly inserted into a json value, to serialize with shortest representation. 

    virtual bool escape_all_non_ascii() const = 0
Escape all non-ascii characters. The default is `false`.

    virtual bool escape_solidus() const = 0
Escape the solidus ('/') character. The default is `false`.

    virtual bool is_nan_to_num() const = 0; 
    virtual bool is_nan_to_str() const = 0; 
    virtual const std::string& nan_to_num() const = 0 
    virtual const std::string& nan_to_str() const = 0 
Replace `NaN` with a number, if `is_nan_to_num()` returns `true`,
or a string, if `is_nan_to_str()` returns `true`. If both
return `false`, replace `NaN` with `null`.

    virtual bool is_inf_to_num() const = 0;
    virtual bool is_inf_to_str() const = 0;
    virtual const std::string& inf_to_num() const = 0 
    virtual const std::string& inf_to_str() const = 0 
Replace positive infinity with a number, if `is_inf_to_num()` returns `true`,
or a string, if `is_inf_to_str()` returns `true`. If both
return `false`, replace positive infinity with `null`.

    virtual bool is_neginf_to_num() const = 0
    virtual bool is_neginf_to_str() const = 0
    virtual const std::string& neginf_to_num() const = 0 
    virtual const std::string& neginf_to_str() const = 0 
Replace negative infinity with a number, if `is_neginf_to_num()` returns `true`,
or a string, if `is_neginf_to_str()` returns true. If both
return `false`, replace negative infinity with `null`.

    virtual std::string new_line_chars() const = 0
Defaults to "\n"

    virtual size_t line_length_limit() const = 0

    virtual line_split_kind object_object_line_splits() const = 0;
For an object whose parent is an object, indicates whether that object is split on a new line, or if its members are split on multiple lines. The default is [line_split_kind::multi_line](line_split_kind.md).

    virtual line_split_kind array_object_line_splits() const = 0;
For an object whose parent is an array, indicates whether that object is split on a new line, or if its members are split on multiple lines. The default is [line_split_kind::multi_line](line_split_kind.md).

    virtual line_split_kind object_array_line_splits() const = 0;
For an array whose parent is an object, indicates whether that array is split on a new line, or if its elements are split on multiple lines. The default is [line_split_kind::same_line](line_split_kind.md).

    virtual line_split_kind array_array_line_splits() const = 0;
For an array whose parent is an array, indicates whether that array is split on a new line, or if its elements are split on multiple lines. The default is [line_split_kind::new_line](line_split_kind.md).

