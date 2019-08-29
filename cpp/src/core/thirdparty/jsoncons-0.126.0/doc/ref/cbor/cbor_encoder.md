### jsoncons::cbor::basic_cbor_encoder

```c++
template<
    class Result>
> class basic_cbor_encoder : public jsoncons::json_content_handler
```

`basic_cbor_encoder` is noncopyable

#### Header

    #include <jsoncons_ext/cbor/cbor_encoder.hpp>

![cbor_encoder](./diagrams/cbor_encoder.png)

Four specializations for common result types are defined:

Type                       |Definition
---------------------------|------------------------------
cbor_encoder            |basic_cbor_encoder<jsoncons::binary_stream_result>
cbor_bytes_encoder     |basic_cbor_encoder<jsoncons::binary_buffer_result>

#### Member types

Type                       |Definition
---------------------------|------------------------------
char_type                  |char
result_type                |Result
string_view_type           |

#### Constructors

    explicit basic_cbor_encoder(result_type result)
Constructs a new encoder that writes to the specified result.

#### Destructor

    virtual ~basic_cbor_encoder()

### Inherited from [basic_json_content_handler](../json_content_handler.md)

#### Member functions

    bool begin_object(semantic_tag tag=semantic_tag::none,
                      const ser_context& context=null_ser_context()); 

    bool begin_object(size_t length, 
                      semantic_tag tag=semantic_tag::none,
                      const ser_context& context=null_ser_context()); 

    bool end_object(const ser_context& context = null_ser_context())

    bool begin_array(semantic_tag tag=semantic_tag::none,
                     const ser_context& context=null_ser_context()); 

    bool begin_array(semantic_tag tag=semantic_tag::none,
                     const ser_context& context=null_ser_context()); 

    bool end_array(const ser_context& context=null_ser_context()); 

    bool name(const string_view_type& name, 
              const ser_context& context=null_ser_context()); 

    bool string_value(const string_view_type& value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context());

    bool byte_string_value(const byte_string_view& b, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context()); 

    bool byte_string_value(const uint8_t* p, size_t size, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context()); 

    bool int64_value(int64_t value, 
                     semantic_tag tag = semantic_tag::none, 
                     const ser_context& context=null_ser_context());

    bool uint64_value(uint64_t value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context()); 

    bool double_value(double value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context()); 

    bool bool_value(bool value, 
                    semantic_tag tag = semantic_tag::none,
                    const ser_context& context=null_ser_context());  

    bool null_value(semantic_tag tag = semantic_tag::none,
                    const ser_context& context=null_ser_context());  

    void flush()

### Examples

#### Encode to CBOR buffer

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>

int main()
{
    std::vector<uint8_t> buffer;
    cbor::cbor_bytes_encoder encoder(buffer);

    encoder.begin_array(); // Indefinite length array
    encoder.string_value("cat");
    encoder.byte_string_value(byte_string({'p','u','r','r'}));
    encoder.byte_string_value(byte_string({'h','i','s','s'}),
                             semantic_tag::base64); // suggested conversion to base64
    encoder.int64_value(1431027667, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

    for (auto c : buffer)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                  << std::noshowbase << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";

/* 
    9f -- Start indefinte length array
      63 -- String value of length 3
        636174 -- "cat"
      44 -- Byte string value of length 4
        70757272 -- 'p''u''r''r'
      d6 - Expected conversion to base64
      44
        68697373 -- 'h''i''s''s'
      c1 -- Tag value 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
      ff -- "break" 
*/ 
}
```
Output:
```
9f636361744470757272d64468697373c11a554bbfd3ff
```

#### Encode to CBOR stream

```c++

#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <iomanip>

int main()
{
    std::ostringstream os;
    cbor::cbor_encoder encoder(os);

    encoder.begin_array(3); // array of length 3
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.string_value("184467440737095516.16", semantic_tag::bigdec);
    encoder.int64_value(1431027667, semantic_tag::timestamp);
    encoder.end_array();
    encoder.flush();

    for (auto c : os.str())
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2) 
                  << std::noshowbase << std::setfill('0') << (int)unsigned char(c);
    }
    std::cout << "\n\n";

/*
    83 -- array of length 3
      c3 -- Tag 3 (negative bignum)
      49 -- Byte string value of length 9
        010000000000000000 -- Bytes content
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          21 -- -2 (exponent)
          c2 Tag 2 (positive bignum)
          49 -- Byte string value of length 9
            010000000000000000
      c1 -- Tag 1 (seconds relative to 1970-01-01T00:00Z in UTC time)
        1a -- 32 bit unsigned integer
          554bbfd3 -- 1431027667
*/
}
```
Output:
```
83c349010000000000000000c48221c249010000000000000000c11a554bbfd3
```

