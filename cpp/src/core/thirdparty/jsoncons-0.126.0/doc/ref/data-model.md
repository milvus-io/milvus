## jsoncons data model

The jsoncons data model supports the familiar JSON types - nulls,
booleans, numbers, strings, arrays, objects - plus byte strings. It has

- null
- bool
- int64
- uint64
- double
- string
- byte string
- array
- object

In addition, jsoncons supports semantic tagging of date-time values, timestamp values, big integers, 
big decimals, bigfloats and binary encodings. This allows it to preserve these type semantics when parsing 
JSON-like data formats such as CBOR that have them, for example, the mappings between the jsoncons
and CBOR data items are shown below:

jsoncons data item|jsoncons tag|CBOR data item|CBOR tag
--------------|------------------|---------------|--------
null          |                  | null |&#160;
null          | undefined        | undefined |&#160;
bool          |                  | true or false |&#160;
int64         |                  | unsigned or negative integer |&#160;
int64         | timestamp        | unsigned or negative integer | 1 (epoch-based date/time)
uint64        |                  | unsigned integer |&#160;
uint64        | timestamp        | unsigned integer | 1 (epoch-based date/time)
double        |                  | half-precision float, float, or double |&#160;
double        | timestamp        | double | 1 (epoch-based date/time)
string        |                  | string |&#160;
string        | bigint      | byte string | 2 (positive bignum) or 2 (negative bignum)  
string        | bigdec      | array | 4 (decimal fraction)
string        | bigfloat      | array | 5 (bigfloat)
string        | datetime        | string | 0 (date/time string) 
string        | uri              | string | 32 (uri)
string        | base64url        | string | 33 (base64url)
string        | base64           | string | 34 (base64)
byte_string   |                  | byte string |&#160;
byte_string   | base64url        | byte string | 21 (Expected conversion to base64url encoding)
byte_string   | base64           | byte string | 22 (Expected conversion to base64 encoding)
byte_string   | base16           | byte string | 23 (Expected conversion to base16 encoding)
array         |                  | array |&#160;
object        |                  | map |&#160;

### Examples

#### json value to CBOR item

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

int main()
{
    json j = json::array();

    j.emplace_back("foo");
    j.emplace_back(byte_string{ 'b','a','r' });
    j.emplace_back("-18446744073709551617", semantic_tag::bigint);
    j.emplace_back("273.15", semantic_tag::bigdec);
    j.emplace_back("2018-10-19 12:41:07-07:00", semantic_tag::datetime);
    j.emplace_back(1431027667, semantic_tag::timestamp);
    j.emplace_back(-1431027667, semantic_tag::timestamp);
    j.emplace_back(1431027667.5, semantic_tag::timestamp);

    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    std::vector<uint8_t> bytes;
    cbor::encode_cbor(j, bytes);
    std::cout << "(2)\n";
    for (auto c : bytes)
    {
        std::cout << std::hex << std::noshowbase << std::setprecision(2) << std::setw(2)
                  << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";
/*
88 -- Array of length 8
  63 -- String value of length 3 
    666f6f -- "foo"
  43 -- Byte string value of length 3
    626172 -- 'b''a''r'
  c3 -- Tag 3 (negative bignum)
    49 Byte string value of length 9
      010000000000000000 -- Bytes content
  c4  - Tag 4 (decimal fraction)
    82 -- Array of length 2
      21 -- -2
      19 6ab3 -- 27315
  c0 -- Tag 0 (date-time)
    78 19 -- Length (25)
      323031382d31302d31392031323a34313a30372d30373a3030 -- "2018-10-19 12:41:07-07:00"
  c1 -- Tag 1 (epoch time)
    1a -- uint32_t
      554bbfd3 -- 1431027667 
  c1
    3a
      554bbfd2
  c1
    fb
      41d552eff4e00000
*/
}
```
Output
```
(1)
[
    "foo",
    "YmFy",
    "-18446744073709551617",
    "273.15",
    "2018-10-19 12:41:07-07:00",
    1431027667,
    -1431027667,
    1431027667.5
]

(2)
8863666f6f43626172c349010000000000000000c48221196ab3c07819323031382d31302d31392031323a34313a30372d30373a3030c11a554bbfd3c13a554bbfd2c1fb41d552eff4e00000
```

#### CBOR item to json value

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>

using namespace jsoncons;

void main()
{
    std::vector<uint8_t> bytes;
    cbor::cbor_bytes_encoder encoder(bytes);
    encoder.begin_array(); // indefinite length outer array
    encoder.string_value("foo");
    encoder.byte_string_value(byte_string({'b','a','r'}));
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.decimal_value("273.15");
    encoder.string_value("2018-10-19 12:41:07-07:00", semantic_tag::datetime) ;
    encoder.epoch_time_value(1431027667);
    encoder.int64_value(-1431027667, semantic_tag::timestamp);
    encoder.double_value(1431027667.5, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

    std::cout << "(1)\n";
    for (auto c : bytes)
    {
        std::cout << std::hex << std::noshowbase << std::setprecision(2) << std::setw(2)
                  << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";

/*
9f -- Start indefinite length array 
  63 -- String value of length 3 
    666f6f -- "foo"
  43 -- Byte string value of length 3
    626172 -- 'b''a''r'
  c3 -- Tag 3 (negative bignum)
    49 Byte string value of length 9
      010000000000000000 -- Bytes content
  c4  - Tag 4 (decimal fraction)
    82 -- Array of length 2
      21 -- -2
      19 6ab3 -- 27315
  c0 -- Tag 0 (date-time)
    78 19 -- Length (25)
      323031382d31302d31392031323a34313a30372d30373a3030 -- "2018-10-19 12:41:07-07:00"
  c1 -- Tag 1 (epoch time)
    1a -- uint32_t
      554bbfd3 -- 1431027667 
  c1
    3a
      554bbfd2
  c1
    fb
      41d552eff4e00000
  ff -- "break" 
*/

    json j = cbor::decode_cbor<json>(bytes);

    std::cout << "(2)\n" << pretty_print(j) << "\n\n";
}
```
Output:
```
(1)
9f63666f6f43626172c349010000000000000000c48221196ab3c07819323031382d31302d31392031323a34313a30372d30373a3030c11a554bbfd3c13a554bbfd2c1fb41d552eff4e00000ff

(2)
[
    "foo",
    "YmFy",
    "-18446744073709551617",
    "273.15",
    "2018-10-19 12:41:07-07:00",
    1431027667,
    -1431027667,
    1431027667.5
]
```

