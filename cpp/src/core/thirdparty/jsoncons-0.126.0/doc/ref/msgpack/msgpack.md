### msgpack extension

The msgpack extension implements encode to and decode from the [MessagePack](http://msgpack.org/index.html) data format.
You can either parse into or serialize from a variant-like structure, [basic_json](../json.md), or your own
data structures, using [json_type_traits](../json_type_traits.md).


[decode_msgpack](decode_msgpack.md)

[encode_msgpack](encode_msgpack.md)

[msgpack_encoder](msgpack_encoder.md)

#### jsoncons-MessagePack mappings

jsoncons data item|jsoncons tag|BSON data item
--------------|------------------|---------------
null          |                  | nil
bool          |                  | true or false
int64         |                  | negative fixnum, int 8, int 16, int 32, int 64
uint64        |                  | positive fixnum, uint 8, uint 16, uint 32, uint 64
double        |                  | float32 or float64
string        |                  | fixstr, str 8, str 16 or str 32
byte_string   |                  | bin 8, bin 16 or bin 32
array         |                  | array 
object        |                  | map

### Examples

Example file (book.json):
```json
[
    {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
    },
    {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
    }
]
```
```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>

using namespace jsoncons;

int main()
{
    std::ifstream is("input/book.json");
    ojson j1;
    is >> j1;

    // Encode ojson to MessagePack
    std::vector<uint8_t> v;
    msgpack::encode_msgpack(j1, v);

    // Decode MessagePack to ojson 
    ojson j2 = msgpack::decode_msgpack<ojson>(v);

    std::cout << pretty_print(j2) << std::endl;

    // or to json (now alphabetically sorted)
    json j3 = msgpack::decode_msgpack<json>(v);

    // or to wjson (converts from utf8 to wide characters)
    wjson j4 = msgpack::decode_msgpack<wjson>(v);
}
```
Output:
```json
[
    {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
    },
    {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
    }
]
```



