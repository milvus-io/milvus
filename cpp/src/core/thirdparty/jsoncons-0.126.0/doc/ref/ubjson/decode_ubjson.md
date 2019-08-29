### jsoncons::ubjson::decode_ubjson

Decodes a [Universal Binary JSON Specification (JSON)](http://ubjson.org/) data format into a C++ data structure.

#### Header
```c++
#include <jsoncons_ext/ubjson/ubjson.hpp>

template<class T>
T decode_ubjson(const std::vector<uint8_t>& v); // (1)

template<class T>
T decode_ubjson(std::istream>& is); // (2)
```

(1) Reads a UBJSON bytes buffer into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

(2) Reads a UBJSON binary stream into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

#### Exceptions

Throws [ser_error](../ser_error.md) if parsing fails.

#### See also

- [encode_ubjson](encode_ubjson.md) encodes a json value to the [Universal Binary JSON Specification](http://ubjson.org/) data format.


