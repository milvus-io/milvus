### jsoncons::cbor::encode_cbor

Encodes a C++ data structure to the [Concise Binary Object Representation](http://cbor.io/) data format.

#### Header
```c++
#include <jsoncons_ext/cbor/cbor.hpp>

template<class T>
void encode_cbor(const T& val, std::vector<uint8_t>& buffer, 
                 const cbor_encode_options& options = cbor_options::default_options()); // (1)

template<class T>
void encode_cbor(const T& val, std::ostream& os, 
                 const cbor_encode_options& options = cbor_options::default_options()); // (2)
```

(1) Writes a value of type T into a bytes buffer in the CBOR data format. Type T must be an instantiation of [basic_json](../json.md) 
or support [json_type_traits](../json_type_traits.md). Uses the [encode options](cbor_options.md)
supplied or defaults.

(2) Writes a value of type T into a binary stream in the CBOR data format. Type T must be an instantiation of [basic_json](../json.md) 
or support [json_type_traits](../json_type_traits.md). Uses the [encode options](cbor_options.md)
supplied or defaults.

#### See also

- [decode_cbor](decode_cbor) decodes a [Concise Binary Object Representation](http://cbor.io/) data format to a json value.

### Examples

#### cbor example

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    ojson j1;
    j1["zero"] = 0;
    j1["one"] = 1;
    j1["two"] = 2;
    j1["null"] = null_type();
    j1["true"] = true;
    j1["false"] = false;
    j1["max int64_t"] = (std::numeric_limits<int64_t>::max)();
    j1["max uint64_t"] = (std::numeric_limits<uint64_t>::max)();
    j1["min int64_t"] = (std::numeric_limits<int64_t>::lowest)();
    j1["max int32_t"] = (std::numeric_limits<int32_t>::max)();
    j1["max uint32_t"] = (std::numeric_limits<uint32_t>::max)();
    j1["min int32_t"] = (std::numeric_limits<int32_t>::lowest)();
    j1["max int16_t"] = (std::numeric_limits<int16_t>::max)();
    j1["max uint16_t"] = (std::numeric_limits<uint16_t>::max)();
    j1["min int16_t"] = (std::numeric_limits<int16_t>::lowest)();
    j1["max int8_t"] = (std::numeric_limits<int8_t>::max)();
    j1["max uint8_t"] = (std::numeric_limits<uint8_t>::max)();
    j1["min int8_t"] = (std::numeric_limits<int8_t>::lowest)();
    j1["max double"] = (std::numeric_limits<double>::max)();
    j1["min double"] = (std::numeric_limits<double>::lowest)();
    j1["max float"] = (std::numeric_limits<float>::max)();
    j1["zero float"] = 0.0;
    j1["min float"] = (std::numeric_limits<float>::lowest)();
    j1["Key too long for small string optimization"] = "String too long for small string optimization";

    std::vector<uint8_t> v;
    cbor::encode_cbor(j1, v);

    ojson j2 = cbor::decode_cbor<ojson>(v);

    std::cout << pretty_print(j2) << std::endl;
}
```
Output:
```json
{
    "zero": 0,
    "one": 1,
    "two": 2,
    "null": null,
    "true": true,
    "false": false,
    "max int64_t": 9223372036854775807,
    "max uint64_t": 18446744073709551615,
    "min int64_t": -9223372036854775808,
    "max int32_t": 2147483647,
    "max uint32_t": 4294967295,
    "min int32_t": -2147483648,
    "max int16_t": 32767,
    "max uint16_t": 65535,
    "min int16_t": -32768,
    "max int8_t": 127,
    "max uint8_t": 255,
    "min int8_t": -128,
    "max double": 1.79769313486232e+308,
    "min double": -1.79769313486232e+308,
    "max float": 3.40282346638529e+038,
    "zero float": 0.0,
    "min float": -3.40282346638529e+038,
    "Key too long for small string optimization": "String too long for small string optimization"
}
```

#### Encode CBOR byte string

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // construct byte string value
    json j(byte_string("Hello"));

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j, buf);

    std::cout << std::hex << std::showbase << "(1) ";
    for (auto c : buf)
    {
        std::cout << (int)c;
    }
    std::cout << std::dec << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << std::endl;
}
```
Output:
```
(1) 0x450x480x650x6c0x6c0x6f

(2) "SGVsbG8"
```

#### Encode byte string with encoding hint

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    // construct byte string value
     json j1(byte_string("Hello"), semantic_tag::base64);

    std::vector<uint8_t> buf;
    cbor::encode_cbor(j1, buf);

    std::cout << std::hex << std::showbase << "(1) ";
    for (auto c : buf)
    {
        std::cout << (int)c;
    }
    std::cout << std::dec << "\n\n";

    json j2 = cbor::decode_cbor<json>(buf);
    std::cout << "(2) " << j2 << std::endl;
}
```
Output:
```
(1) 0xd60x450x480x650x6c0x6c0x6f

(2) "SGVsbG8="
```

#### Encode packed strings [stringref-namespace, stringref](http://cbor.schmorp.de/stringref) 

This example taken from [CBOR stringref extension](http://cbor.schmorp.de/stringref) shows how to encode a
data structure that contains many repeated strings more efficiently.

```c++
#include <iomanip>
#include <cassert>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    ojson j = ojson::parse(R"(
[
     {
       "name" : "Cocktail",
       "count" : 417,
       "rank" : 4
     },
     {
       "rank" : 4,
       "count" : 312,
       "name" : "Bath"
     },
     {
       "count" : 691,
       "name" : "Food",
       "rank" : 4
     }
  ]
)");

    cbor::cbor_options options;
    options.pack_strings(true);
    std::vector<uint8_t> buf;

    cbor::encode_cbor(j, buf, options);

    for (auto c : buf)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                  << std::noshowbase << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n";

/*
    d90100 -- tag (256)
      83 -- array(3)
        a3 -- map(3)
          64 -- text string (4)
            6e616d65 -- "name"
          68 -- text string (8)
            436f636b7461696c -- "Cocktail"
          65 -- text string (5)
            636f756e74 -- "count"
            1901a1 -- unsigned(417)
          64 -- text string (4)
            72616e6b -- "rank"
            04 -- unsigned(4)
        a3 -- map(3)
          d819 -- tag(25)
            03 -- unsigned(3)
          04 -- unsigned(4)
          d819 -- tag(25)
            02 -- unsigned(2)
            190138 -- unsigned(312)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 -- text string(4)
            42617468 -- "Bath"
        a3 -- map(3)
          d819 -- tag(25)
            02 -- unsigned(2)
          1902b3 -- unsigned(691)
          d819 -- tag(25)
            00 -- unsigned(0)
          64 - text string(4)
            466f6f64 -- "Food"
          d819 -- tag(25)
            03 -- unsigned(3)
            04 -- unsigned(4)
*/

    ojson j2 = cbor::decode_cbor<ojson>(buf);
    assert(j2 == j);
}
```
Output:
```
d9010083a3646e616d6568436f636b7461696c65636f756e741901a16472616e6b04a3d8190304d81902190138d819006442617468a3d819021902b3d8190064466f6f64d8190304
```

#### See also

- [byte_string](../byte_string.md)
- [decode_cbor](decode_cbor.md) decodes a [Concise Binary Object Representation](http://cbor.io/) data format to a json value.

