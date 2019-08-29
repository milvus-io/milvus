### jsoncons::cbor::decode_cbor

Decodes a [Concise Binary Object Representation](http://cbor.io/) data format into a C++ data structure.

#### Header
```c++
#include <jsoncons_ext/cbor/cbor.hpp>

template<class T>
T decode_cbor(const std::vector<uint8_t>& v); // (1)

template<class T>
T decode_cbor(std::istream& is); // (2)
```

(1) Reads a CBOR bytes buffer into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

(2) Reads a CBOR binary stream into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

#### Exceptions

Throws [ser_error](../ser_error.md) if parsing fails.

### Examples

#### Round trip (JSON to CBOR bytes back to JSON)

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    ojson j1 = ojson::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum.example.com",
           "assertion": "strong-hiker",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    ojson j2 = cbor::decode_cbor<ojson>(v);
    std::cout << pretty_print(j2) << std::endl;
}
```
Output:
```json
{
    "application": "hiking",
    "reputons": [
        {
            "rater": "HikingAsylum.example.com",
            "assertion": "strong-hiker",
            "rated": "Marilyn C",
            "rating": 0.9
        }
    ]
}
```

#### Round trip (JSON to CBOR file back to JSON)

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    json j = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum.example.com",
           "assertion": "strong-hiker",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    std::ofstream os;
    os.open("./output/store.cbor", std::ios::binary | std::ios::out);
    cbor::encode_cbor(j,os);

    std::vector<uint8_t> v;
    std::ifstream is;
    is.open("./output/store.cbor", std::ios::binary | std::ios::in);

    json j2 = cbor::decode_cbor<json>(is);

    std::cout << pretty_print(j2) << std::endl; 
}
```
Output:
```json
{
    "application": "hiking",
    "reputons": [
        {
            "assertion": "strong-hiker",
            "rated": "Marilyn C",
            "rater": "HikingAsylum.example.com",
            "rating": 0.9
        }
    ]
}
```

#### Decode CBOR byte string

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // byte string of length 5
    std::vector<uint8_t> buf = {0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bs = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bs << "\n\n";

    // byte string value to JSON text becomes base64url
    std::cout << "(2) " << j << std::endl;
}
```
Output:
```
(1) 0x480x650x6c0x6c0x6f

(2) "SGVsbG8"
```

#### Decode CBOR byte string with base64 encoding hint

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // semantic tag indicating expected conversion to base64
    // followed by byte string of length 5
    std::vector<uint8_t> buf = {0xd6,0x45,'H','e','l','l','o'};
    json j = cbor::decode_cbor<json>(buf);

    auto bs = j.as<byte_string>();

    // byte_string to ostream displays as hex
    std::cout << "(1) "<< bs << "\n\n";

    // byte string value to JSON text becomes base64
    std::cout << "(2) " << j << std::endl;
}
```
Output:
```
(1) 0x480x650x6c0x6c0x6f

(2) "SGVsbG8="
```

#### Decode packed strings [stringref-namespace, stringref](http://cbor.schmorp.de/stringref)

This example taken from [CBOR stringref extension](http://cbor.schmorp.de/stringref) shows three stringref-namespace tags, 
with two nested inside another:

```c++
int main()
{
    std::vector<uint8_t> v = {0xd9,0x01,0x00, // tag(256)
      0x85,                 // array(5)
         0x63,              // text(3)
            0x61,0x61,0x61, // "aaa"
         0xd8, 0x19,        // tag(25)
            0x00,           // unsigned(0)
         0xd9, 0x01,0x00,   // tag(256)
            0x83,           // array(3)
               0x63,        // text(3)
                  0x62,0x62,0x62, // "bbb"
               0x63,        // text(3)
                  0x61,0x61,0x61, // "aaa"
               0xd8, 0x19,  // tag(25)
                  0x01,     // unsigned(1)
         0xd9, 0x01,0x00,   // tag(256)
            0x82,           // array(2)
               0x63,        // text(3)
                  0x63,0x63,0x63, // "ccc"
               0xd8, 0x19,  // tag(25)
                  0x00,     // unsigned(0)
         0xd8, 0x19,        // tag(25)
            0x00           // unsigned(0)
    };

    ojson j = cbor::decode_cbor<ojson>(v);

    std::cout << pretty_print(j) << "\n";
}
```
Output:
```
[
    "aaa",
    "aaa",
    ["bbb", "aaa", "aaa"],
    ["ccc", "ccc"],
    "aaa"
]
```

#### See also

- [byte_string](../byte_string.md)
- [encode_cbor](encode_cbor.md) encodes a json value to the [Concise Binary Object Representation](http://cbor.io/) data format.


