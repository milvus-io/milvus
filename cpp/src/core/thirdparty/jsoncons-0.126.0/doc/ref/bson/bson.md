### bson extension

The bson extension implements decode from and encode to the [Binary JSON](http://bsonspec.org/) data format.
You can either parse into or serialize from a variant-like structure, [basic_json](../json.md), or your own
data structures, using [json_type_traits](../json_type_traits.md).

[decode_bson](decode_bson.md)

[encode_bson](encode_bson.md)

[bson_encoder](bson_encoder.md)

#### jsoncons-BSON mappings

jsoncons data item|jsoncons tag|BSON data item
--------------|------------------|---------------
null          |                  | null
bool          |                  | true or false
int64         |                  | int32 or int64
int64         | timestamp        | datetime
uint64        |                  | int32 or int64
uint64        | timestamp        | datetime
double        |                  | double
string        |                  | string
byte_string   |                  | binary
array         |                  | 0x04 (array )
object        |                  | 0x03 (document)


