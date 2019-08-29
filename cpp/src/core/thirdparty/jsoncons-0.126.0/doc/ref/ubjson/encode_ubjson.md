### jsoncons::ubjson::encode_ubjson

Encodes a C++ data structure to the [Universal Binary JSON Specification (UBJSON)](http://ubjsonspec.org/) data format.

#### Header
```c++
#include <jsoncons_ext/ubjson/ubjson.hpp>

template<class T>
void encode_ubjson(const T& jval, std::vector<uint8_t>& v); // (1)

template<class T>
void encode_ubjson(const T& jval, std::ostream& os); // (2)
```

(1) Writes a value of type T into a bytes buffer in the UBJSON data format. Type T must be an instantiation of [basic_json](../json.md) 
or support [json_type_traits](../json_type_traits.md). 

(2) Writes a value of type T into a binary stream in the UBJSON data format. Type T must be an instantiation of [basic_json](../json.md) 
or support [json_type_traits](../json_type_traits.md). 

#### See also

- [decode_ubjson](decode_ubjson) decodes a [Binary JSON](http://ubjsonspec.org/) data format to a json value.

