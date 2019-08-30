### jsoncons::wjson_reader

```c++
    typedef basic_json_reader<wchar_t,stream_source<wchar_t>> wjson_reader
```
The `wjson_reader` class is an instantiation of the `basic_json_reader` class template that uses `wchar_t` as the character type
and `stream_source` as the input source.

#### Header

    #include <jsoncons/json_reader.hpp>

#### Interface

The interface is the same as [json_reader](json_reader.md), substituting wide character instantiations of classes - `std::wstring`, `std::wistream`, etc. - for utf8 character ones.

