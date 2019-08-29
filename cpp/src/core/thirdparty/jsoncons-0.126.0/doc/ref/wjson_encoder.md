### jsoncons::wjson_encoder

```c++
typedef basic_json_encoder<wchar_t> wjson_encoder
```

The `wjson_encoder` class is an instantiation of the `basic_json_encoder` class template that uses `wchar_t` as the character type. It implements [wjson_content_handler](basic_json_content_handler.md) and supports pretty print serialization.

#### Header

    #include <jsoncons/json_encoder.hpp>

#### Interface

The interface is the same as [json_encoder](json_encoder.md), substituting wide character instantiations of classes - `std::wstring`, etc. - for utf8 character ones.
