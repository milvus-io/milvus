### jsoncons::json_decode_options

```c++
typedef basic_json_decode_options<char> json_decode_options
```

An abstract class that defines accessors for JSON decode options. The `json_decode_options` class is an instantiation of the `basic_json_decode_options` class template that uses `char` as the character type.

#### Header
```c++
#include <jsoncons/json_options.hpp>
```

#### Implementing classes

[json_options](json_options.md)

#### Destructor

    virtual ~json_decode_options();

#### Accessors

    virtual bool is_str_to_nan() const = 0;
Indicates `NaN` replacement for string when parsing.

    virtual std::string nan_to_str() const = 0;
When parsing JSON text, replace string with a `NaN` if `is_nan_to_str()` returns `true`.

    virtual bool is_str_to_inf() const = 0;
Indicates `Infinity` replacement for string when parsing.

    virtual const std::string& inf_to_str() const = 0; 
When parsing JSON text, replace string with infinity if `is_inf_to_str()` returns `true`.

    virtual bool is_str_to_neginf() const = 0;
Indicates `Negative Infinity` replacement for string when parsing.

    virtual const std::string& neginf_to_str() const = 0; 
When parsing JSON text, replace string with minus infinity if `is_neginf_to_str()` returns true.

    virtual bool lossless_number() const = 0; 
If set to `true`, parse decimal numbers as strings with semantic tagging `semantic_tag::bigdec` instead of double.

    virtual size_t max_nesting_depth() = 0;
 Maximum nesting depth when parsing JSON.

