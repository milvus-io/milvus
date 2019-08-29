### jsoncons::msgpack::decode_msgpack

Decodes a [MessagePack](http://msgpack.org/index.html) data format into a C++ data structure.

#### Header
```c++
#include <jsoncons_ext/msgpack/msgpack.hpp>

template<class T>
T decode_msgpack(const std::vector<uint8_t>& v); // (1)

template<class T>
T decode_msgpack(std::istream& is); // (2)
```

(1) Reads a MessagePack bytes buffer into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

(2) Reads a MessagePack binary stream into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

#### Exceptions

Throws [ser_error](../ser_error.md) if parsing fails.

#### See also

- [encode_msgpack](encode_msgpack.md) encodes a json value to the [MessagePack](http://msgpack.org/index.html) data format.


