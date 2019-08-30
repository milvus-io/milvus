### jsoncons::bson::decode_bson

Decodes a [Binary JSON (BSON)](http://bsonspec.org/) data format into a C++ data structure.

#### Header
```c++
#include <jsoncons_ext/bson/bson.hpp>

template<class T>
T decode_bson(const std::vector<uint8_t>& v); // (1)

template<class T>
T decode_bson(std::istream& is); // (2)
```

(1) Reads a BSON bytes buffer into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

(2) Reads a BSON binary stream into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

#### Exceptions

Throws [ser_error](../ser_error.md) if parsing fails.

#### See also

- [encode_bson](encode_bson.md) encodes a json value to the [Bin­ary JSON](http://bsonspec.org/) data format.


