# JSONCONS

jsoncons is a C++, header-only library for constructing [JSON](http://www.json.org) and JSON-like
data formats such as [CBOR](http://cbor.io/). It supports 

- Parsing JSON-like text or binary formats into a tree model
  that defines an interface for accessing and modifying that data.

- Serializing the tree model into different JSON-like text or binary formats.

- Converting from JSON-like text or binary formats to C++ data structures and back via [json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md).

- Streaming JSON read and write events, somewhat analogously to SAX (push parsing) and StAX (pull parsing) in the XML world. 

Compared to other JSON libraries, jsoncons has been designed to handle very large JSON texts. At its heart are
SAX style parsers and serializers. Its [json parser](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_parser.md) is an 
incremental parser that can be fed its input in chunks, and does not require an entire file to be loaded in memory at one time. 
Its tree model is more compact than most, and can be made more compact still with a user-supplied
allocator. It also supports memory efficient parsing of very large JSON texts with a [pull parser](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_cursor.md),
built on top of its incremental parser.  

The [jsoncons data model](doc/ref/data-model.md) supports the familiar JSON types - nulls,
booleans, numbers, strings, arrays, objects - plus byte strings. In addition, jsoncons 
supports semantic tagging of date-time values, timestamp values, big integers, 
big decimals, bigfloats and binary encodings. This allows it to preserve these type semantics when parsing 
JSON-like data formats such as CBOR that have them.

jsoncons is distributed under the [Boost Software License](http://www.boost.org/users/license.html).

## Extensions

- [jsonpointer](doc/ref/jsonpointer/jsonpointer.md) implements the IETF standard [JavaScript Object Notation (JSON) Pointer](https://tools.ietf.org/html/rfc6901)
- [jsonpatch](doc/ref/jsonpatch/jsonpatch.md) implements the IETF standard [JavaScript Object Notation (JSON) Patch](https://tools.ietf.org/html/rfc6902)
- [jsonpath](doc/ref/jsonpath/jsonpath.md) implements [Stefan Goessner's JSONPath](http://goessner.net/articles/JsonPath/).  It also supports search and replace using JSONPath expressions.
- [cbor](doc/ref/cbor/cbor.md) implements decode from and encode to the IETF standard [Concise Binary Object Representation](http://cbor.io/) data format. 
- [msgpack](doc/ref/msgpack/msgpack.md) implements decode from and encode to the [MessagePack](http://msgpack.org/index.html) data format.
- [ubjson](doc/ref/ubjson/ubjson.md) implements decode from and encode to the [Universal Binary JSON Specification](http://ubjson.org/) data format.
- [bson](doc/ref/bson/bson.md) implements decode from and encode to the [Binary JSON](http://bsonspec.org/) data format.
- [csv](doc/ref/csv/csv.md) implements reading (writing) JSON values from (to) CSV files

## What users say

_"I am so happy I have come across your json c++ library!"_

_"I’m using your library for an external interface to pass data, as well as using the conversions from csv to json, which are really helpful for converting data for use in javascript ... it's a great library."_

_"this software is great and the ability to have an xpath like facility is so useful."_

_"really good"_ _"awesome project"_ _"very solid and very dependable"_ _"great job"_ _"Your repo rocks!!!!!"_

## Supported compilers

jsoncons uses some features that are new to C++ 11, including [move semantics](http://thbecker.net/articles/rvalue_references/section_02.html) and the [AllocatorAwareContainer](http://en.cppreference.com/w/cpp/concept/AllocatorAwareContainer) concept. It is tested in continuous integration on [AppVeyor](https://ci.appveyor.com/project/danielaparker/jsoncons), [Travis](https://travis-ci.org/danielaparker/jsoncons), and [doozer](https://doozer.io/).

| Compiler                | Version                   |Architecture | Operating System  | Notes |
|-------------------------|---------------------------|-------------|-------------------|-------|
| Microsoft Visual Studio | vs2015 (MSVC 19.0.24241.7)| x86,x64     | Windows 10        |       |
|                         | vs2017                    | x86,x64     | Windows 10        |       |
| g++                     | 4.8 and above             | x64         | Ubuntu            |`std::regex` isn't fully implemented in 4.8, so `jsoncons::jsonpath` regular expression filters aren't supported in 4.8 |
|                         | 4.8.5                     | x64         | CentOS 7.6        |`std::regex` isn't fully implemented in 4.8, so `jsoncons::jsonpath` regular expression filters aren't supported in 4.8 |
|                         | 6.3.1 (Red Hat 6.3.1-1)   | x64         | Fedora release 24 |       |
| clang                   | 3.8 and above             | x64         | Ubuntu            |       |
| clang xcode             | 6.4 and above             | x64         | OSX               |       |

It is also cross compiled for ARMv8-A architecture on Travis using clang and executed using the emulator qemu. 

[UndefinedBehaviorSanitizer (UBSan)](http://clang.llvm.org/docs/UndefinedBehaviorSanitizer.html) diagnostics are enabled for selected gcc and clang builds.

## Get jsoncons

Download the [latest release](https://github.com/danielaparker/jsoncons/releases) and unpack the zip file. Copy the directory `include/jsoncons` to your `include` directory. If you wish to use extensions, copy `include/jsoncons_ext` as well. 

Or, download the latest code on [master](https://github.com/danielaparker/jsoncons/archive/master.zip).

## How to use it

- [Quick guide](http://danielaparker.github.io/jsoncons)
- [Examples](doc/Examples.md)
- [Reference](doc/Home.md)

As the `jsoncons` library has evolved, names have sometimes changed. To ease transition, jsoncons deprecates the old names but continues to support many of them. See the [deprecated list](doc/ref/deprecated.md) for the status of old names. The deprecated names can be suppressed by defining macro `JSONCONS_NO_DEPRECATED`, which is recommended for new code.

## Benchmarks

[json_benchmarks](https://github.com/danielaparker/json_benchmarks) provides some measurements about how `jsoncons` compares to other `json` libraries.

- [JSONTestSuite and JSON_checker test suites](https://danielaparker.github.io/json_benchmarks/) 

- [Performance benchmarks with text and integers](https://github.com/danielaparker/json_benchmarks/blob/master/report/performance.md)

- [Performance benchmarks with text and doubles](https://github.com/danielaparker/json_benchmarks/blob/master/report/performance_fp.md)

### A simple example

```c++
#include <iostream>
#include <fstream>
#include <jsoncons/json.hpp>

// For convenience
using jsoncons::json;

int main()
{
    json color_spaces = json::array();
    color_spaces.push_back("sRGB");
    color_spaces.push_back("AdobeRGB");
    color_spaces.push_back("ProPhoto RGB");

    json image_sizing; // empty object
    image_sizing["Resize To Fit"] = true; // a boolean 
    image_sizing["Resize Unit"] = "pixels"; // a string
    image_sizing["Resize What"] = "long_edge"; // a string
    image_sizing["Dimension 1"] = 9.84; // a double
    
    json export_settings;

    // create "File Format Options" as an object and put "Color Spaces" in it
    export_settings["File Format Options"]["Color Spaces"] = std::move(color_spaces); 

    export_settings["Image Sizing"] = std::move(image_sizing);

    // Write to stream
    std::ofstream os("export_settings.json");
    os << export_settings;

    // Read from stream
    std::ifstream is("export_settings.json");
    json j = json::parse(is);

    // Pretty print
    std::cout << "(1)\n" << pretty_print(j) << "\n\n";

    // Does object member exist?
    std::cout << "(2) " << std::boolalpha << j.contains("Image Sizing") << "\n\n";

    // Get reference to object member
    const json& val = j["Image Sizing"];

    // Access member as double
    std::cout << "(3) " << "Dimension 1 = " << val["Dimension 1"].as<double>() << "\n\n";

    // Try access member with default
    std::cout << "(4) " << "Dimension 2 = " << val.get_with_default("Dimension 2",0.0) << "\n";
}
```
Output:
```json
(1)
{
    "File Format Options": {
        "Color Spaces": ["sRGB","AdobeRGB","ProPhoto RGB"],
        "Image Formats": ["JPEG","PSD","TIFF","DNG"]
    },
    "File Settings": {
        "Color Space": "sRGB",
        "Image Format": "JPEG",
        "Limit File Size": true,
        "Limit File Size To": 10000
    },
    "Image Sizing": {
        "Dimension 1": 9.84,
        "Resize To Fit": true,
        "Resize Unit": "pixels",
        "Resize What": "long_edge"
    }
}

(2) true

(3) Dimension 1 = 9.8

(4) Dimension 2 = 0
```

## About jsoncons::basic_json

The jsoncons library provides a `basic_json` class template, which is the generalization of a `json` value for different 
character types, different policies for ordering name-value pairs, etc. A `basic_json` provides a tree model
of JSON-like data formats, and defines an interface for accessing and modifying that data.
Despite its name, it is not JSON specific.

```c++
typedef basic_json<char,
                   ImplementationPolicy = sorted_policy,
                   Allocator = std::allocator<char>> json;
```
The library includes four instantiations of `basic_json`:

- [json](doc/ref/json.md) constructs a utf8 character json value that sorts name-value members alphabetically

- [ojson](doc/ref/ojson.md) constructs a utf8 character json value that preserves the original name-value insertion order

- [wjson](doc/ref/wjson.md) constructs a wide character json value that sorts name-value members alphabetically

- [wojson](doc/ref/wojson.md) constructs a wide character json value that preserves the original name-value insertion order

## More examples

[Encode C++ data structures to JSON, decode JSON to C++ data structures](#E1)  

[Playing around with CBOR, JSON, and CSV](#E2)  

[Query CBOR with JSONPath](#E3)  

[Pull parser example](#E4)  

[Iterate over a json stream with staj iterators](#E5)  

[Dump json content into a larger document](#E6)  

<div id="E1"/> 

### Encode C++ data structures to JSON, decode JSON to C++ data structures

jsoncons supports conversion between C++ data structures and JSON. The functions [encode_json](doc/ref/encode_json.md) 
and [decode_json](doc/ref/decode_json.md) convert C++ data structures to JSON formatted strings or streams and back. 
Encode and decode work for all C++ classes that have 
[json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md) 
defined. The standard library containers are already supported, and you can specialize `json_type_traits`
for your own types in the `jsoncons` namespace. 

`JSONCONS_MEMBER_TRAITS_DECL` is a macro that simplifies the creation of the necessary boilerplate
from member data. It must be placed outside any namespace blocks.

```c++
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>

namespace ns {

    struct reputon
    {
        std::string rater;
        std::string assertion;
        std::string rated;
        double rating;

        friend bool operator==(const reputon& lhs, const reputon& rhs)
        {
            return lhs.rater == rhs.rater && lhs.assertion == rhs.assertion && 
                   lhs.rated == rhs.rated && lhs.rating == rhs.rating;
        }

        friend bool operator!=(const reputon& lhs, const reputon& rhs)
        {
            return !(lhs == rhs);
        };
    };

    class reputation_object
    {
        std::string application;
        std::vector<reputon> reputons;

        // Make json_type_traits specializations friends to give accesses to private members
        JSONCONS_TYPE_TRAITS_FRIEND;
    public:
        reputation_object()
        {
        }
        reputation_object(const std::string& application, const std::vector<reputon>& reputons)
            : application(application), reputons(reputons)
        {}

        friend bool operator==(const reputation_object& lhs, const reputation_object& rhs)
        {
            return (lhs.application == rhs.application) && (lhs.reputons == rhs.reputons);
        }

        friend bool operator!=(const reputation_object& lhs, const reputation_object& rhs)
        {
            return !(lhs == rhs);
        };
    };


} // namespace ns

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_MEMBER_TRAITS_DECL(ns::reputon, rater, assertion, rated, rating)
JSONCONS_MEMBER_TRAITS_DECL(ns::reputation_object, application, reputons)

using namespace jsoncons; // for convenience

int main()
{
    ns::reputation_object val("hiking", { ns::reputon{"HikingAsylum.example.com","strong-hiker","Marilyn C",0.90} });

    std::string s;
    encode_json(val, s, indenting::indent);
    std::cout << s << "\n";

    auto val2 = decode_json<ns::reputation_object>(s);

    assert(val2 == val);
}
```
Output:
```
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

See [examples](https://github.com/danielaparker/jsoncons/blob/master/doc/Examples.md#G1)

<div id="E2"/> 

### Playing around with CBOR, JSON, and CSV

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/csv/csv.hpp>

// For convenience
using namespace jsoncons;    

int main()
{
    // Construct some CBOR using the streaming API
    std::vector<uint8_t> b;
    cbor::cbor_bytes_encoder encoder(b);
    encoder.begin_array(); // indefinite length outer array
    encoder.begin_array(3); // a fixed length array
    encoder.string_value("foo");
    encoder.byte_string_value(byte_string{'P','u','s','s'}); // no suggested conversion
    encoder.string_value("-18446744073709551617", semantic_tag::bigint);
    encoder.end_array();
    encoder.end_array();
    encoder.flush();

    // Print bytes
    std::cout << "(1) ";
    for (auto c : b)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2)
                  << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";
/*
    9f -- Start indefinte length array
      83 -- Array of length 3
        63 -- String value of length 3
          666f6f -- "foo" 
        44 -- Byte string value of length 4
          50757373 -- 'P''u''s''s'
        c3 -- Tag 3 (negative bignum)
          49 -- Byte string value of length 9
            010000000000000000 -- Bytes content
      ff -- "break" 
*/
    // Unpack bytes into a json variant value, and add some more elements
    json j = cbor::decode_cbor<json>(b);

    // Loop over the rows
    std::cout << "(2)\n";
    for (const json& row : j.array_range())
    {
        std::cout << row << "\n";
    }
    std::cout << "\n";

    // Get bignum value at position 0/2 using jsonpointer 
    json& v = jsonpointer::get(j, "/0/2");
    std::cout << "(3) " << v.as<std::string>() << "\n\n";

    // Print JSON representation with default options
    std::cout << "(4)\n";
    std::cout << pretty_print(j) << "\n\n";

    // Print JSON representation with different options
    json_options options;
    options.byte_string_format(byte_string_chars_format::base64)
           .bigint_format(bigint_chars_format::base64url);
    std::cout << "(5)\n";
    std::cout << pretty_print(j, options) << "\n\n";

    // Add some more elements

    json another_array = json::array(); 
    another_array.emplace_back(byte_string({'P','u','s','s'}),
                               semantic_tag::base64); // suggested conversion to base64
    another_array.emplace_back("273.15", semantic_tag::bigdec);
    another_array.emplace(another_array.array_range().begin(),"bar"); // place at front

    j.push_back(std::move(another_array));
    std::cout << "(6)\n";
    std::cout << pretty_print(j) << "\n\n";

    // Get big decimal value at position /1/2 using jsonpointer
    json& ref = jsonpointer::get(j, "/1/2");
    std::cout << "(7) " << ref.as<std::string>() << "\n\n";

#if (defined(__GNUC__) || defined(__clang__)) && (!defined(__STRICT_ANSI__) && defined(_GLIBCXX_USE_INT128))
    // e.g. if code compiled with GCC and std=gnu++11 (rather than std=c++11)
    __int128 i = j[1][2].as<__int128>();
#endif

    // Get byte string value at position /1/1 as a byte_string
    byte_string bs = j[1][1].as<byte_string>();
    std::cout << "(8) " << bs << "\n\n";

    // or alternatively as a std::vector<uint8_t>
    std::vector<uint8_t> u = j[1][1].as<std::vector<uint8_t>>();

    // Repack bytes
    std::vector<uint8_t> b2;
    cbor::encode_cbor(j, b2);

    // Print the repacked bytes
    std::cout << "(9) ";
    for (auto c : b2)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2)
                  << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";
/*
    82 -- Array of length 2
      83 -- Array of length 3
        63 -- String value of length 3
          666f6f -- "foo" 
        44 -- Byte string value of length 4
          50757373 -- 'P''u''s''s'
        c3 -- Tag 3 (negative bignum)
        49 -- Byte string value of length 9
          010000000000000000 -- Bytes content
      83 -- Another array of length 3
      63 -- String value of length 3
        626172 -- "bar"
      d6 - Expected conversion to base64
      44 -- Byte string value of length 4
        50757373 -- 'P''u''s''s'
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          21 -- -2
          19 6ab3 -- 27315
*/
    // Encode to CSV
    csv::csv_options csv_options;
    csv_options.column_names("Column 1,Column 2,Column 3");

    std::cout << "(10)\n";
    csv::encode_csv(j, std::cout, csv_options);
}

```
Output:
```
(1) 9f8363666f6f4450757373c349010000000000000000ff

(2)
["foo","UHVzcw","-18446744073709551617"]

(3) -18446744073709551617

(4)
[
    ["foo", "UHVzcw", "-18446744073709551617"]
]

(5)
[
    ["foo", "UHVzcw==", "~AQAAAAAAAAAA"]
]

(6)
[
    ["foo", "UHVzcw", "-18446744073709551617"],
    ["bar", "UHVzcw==", "273.15"]
]

(7) 273.15

(8) 50757373

(9) 828363666f6f4450757373c3490100000000000000008363626172d64450757373c48221196ab3

(10)
Column 1,Column 2,Column 3
foo,UHVzcw,-18446744073709551617
bar,UHVzcw==,273.15
```

<div id="E3"/> 

### Query CBOR with JSONPath
```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <iomanip>

using namespace jsoncons; // For convenience

int main()
{
    // Construct a json array of numbers
    json j = json::array();

    j.emplace_back(5.0);

    j.emplace_back(0.000071);

    j.emplace_back("-18446744073709551617",semantic_tag::bigint);

    j.emplace_back("1.23456789012345678901234567890", semantic_tag::bigdec);

    j.emplace_back("0x3p-1", semantic_tag::bigfloat);

    // Encode to JSON
    std::cout << "(1)\n";
    std::cout << pretty_print(j);
    std::cout << "\n\n";

    // as<std::string>() and as<double>()
    std::cout << "(2)\n";
    std::cout << std::dec << std::setprecision(15);
    for (const auto& item : j.array_range())
    {
        std::cout << item.as<std::string>() << ", " << item.as<double>() << "\n";
    }
    std::cout << "\n";

    // Encode to CBOR
    std::vector<uint8_t> v;
    cbor::encode_cbor(j,v);

    std::cout << "(3)\n";
    for (auto c : v)
    {
        std::cout << std::hex << std::setprecision(2) << std::setw(2)
                  << std::setfill('0') << static_cast<int>(c);
    }
    std::cout << "\n\n";
/*
    85 -- Array of length 5     
      fa -- float 
        40a00000 -- 5.0
      fb -- double 
        3f129cbab649d389 -- 0.000071
      c3 -- Tag 3 (negative bignum)
        49 -- Byte string value of length 9
          010000000000000000
      c4 -- Tag 4 (decimal fraction)
        82 -- Array of length 2
          38 -- Negative integer of length 1
            1c -- -29
          c2 -- Tag 2 (positive bignum)
            4d -- Byte string value of length 13
              018ee90ff6c373e0ee4e3f0ad2
      c5 -- Tag 5 (bigfloat)
        82 -- Array of length 2
          20 -- -1
          03 -- 3   
*/

    // Decode back to json
    json other = cbor::decode_cbor<json>(v);
    assert(other == j);

    // Query with JSONPath
    std::cout << "(4)\n";
    json result = jsonpath::json_query(other,"$[?(@ < 1.5)]");
    std::cout << pretty_print(result) << "\n\n";
```
Output:
```
(1)
[
    5.0,
    7.1e-05,
    "-18446744073709551617",
    "1.23456789012345678901234567890",
    [-1, 3]
]

(2)
5.0, 5
7.1e-05, 7.1e-05
-18446744073709551617, -1.84467440737096e+19
1.23456789012345678901234567890, 1.23456789012346
1.5, 1.5

(3)
85fa40a00000fb3f129cbab649d389c349010000000000000000c482381cc24d018ee90ff6c373e0ee4e3f0ad2c5822003

(4)
[
    7.1e-05,
    "-18446744073709551617",
    "1.23456789012345678901234567890"
]
```

<div id="E4"/> 

### Pull parser example

A typical pull parsing application will repeatedly process the `current()` 
event and call `next()` to advance to the next event, until `done()` 
returns `true`.

The example JSON text, `book_catalog.json`, is used by the examples below.

```json
[ 
  { 
      "author" : "Haruki Murakami",
      "title" : "Hard-Boiled Wonderland and the End of the World",
      "isbn" : "0679743464",
      "publisher" : "Vintage",
      "date" : "1993-03-02",
      "price": 18.90
  },
  { 
      "author" : "Graham Greene",
      "title" : "The Comedians",
      "isbn" : "0099478374",
      "publisher" : "Vintage Classics",
      "date" : "2005-09-21",
      "price": 15.74
  }
]
```

#### Reading the JSON stream
```c++
std::ifstream is("book_catalog.json");

json_cursor reader(is);

for (; !reader.done(); reader.next())
{
    const auto& event = reader.current();
    switch (event.event_type())
    {
        case staj_event_type::begin_array:
            std::cout << "begin_array\n";
            break;
        case staj_event_type::end_array:
            std::cout << "end_array\n";
            break;
        case staj_event_type::begin_object:
            std::cout << "begin_object\n";
            break;
        case staj_event_type::end_object:
            std::cout << "end_object\n";
            break;
        case staj_event_type::name:
            // If underlying type is string, can return as string_view
            std::cout << "name: " << event.as<jsoncons::string_view>() << "\n";
            break;
        case staj_event_type::string_value:
            std::cout << "string_value: " << event.as<jsoncons::string_view>() << "\n";
            break;
        case staj_event_type::null_value:
            std::cout << "null_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::bool_value:
            std::cout << "bool_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::int64_value:
            std::cout << "int64_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::uint64_value:
            std::cout << "uint64_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::double_value:
            // Return as string, could also use event.as<double>()
            std::cout << "double_value: " << event.as<std::string>() << "\n";
            break;
        default:
            std::cout << "Unhandled event type\n";
            break;
    }
}
```
Output:
```
begin_array
begin_object
name: author
string_value: Haruki Murakami
name: title
string_value: Hard-Boiled Wonderland and the End of the World
name: isbn
string_value: 0679743464
name: publisher
string_value: Vintage
name: date
string_value: 1993-03-02
name: price
double_value: 18.90
end_object
begin_object
name: author
string_value: Graham Greene
name: title
string_value: The Comedians
name: isbn
string_value: 0099478374
name: publisher
string_value: Vintage Classics
name: date
string_value: 2005-09-21
name: price
double_value: 15.74
end_object
end_array
```

<div id="E5"/> 

#### Implementing a staj_filter

```c++
// A stream filter to filter out all events except name 
// and restrict name to "author"

class author_filter : public staj_filter
{
    bool accept_next_ = false;
public:
    bool accept(const staj_event& event, const ser_context&) override
    {
        if (event.event_type()  == staj_event_type::name &&
            event.as<jsoncons::string_view>() == "author")
        {
            accept_next_ = true;
            return false;
        }
        else if (accept_next_)
        {
            accept_next_ = false;
            return true;
        }
        else
        {
            accept_next_ = false;
            return false;
        }
    }
};
```

#### Filtering the JSON stream

```c++
std::ifstream is("book_catalog.json");

author_filter filter;
json_cursor reader(is, filter);

for (; !reader.done(); reader.next())
{
    const auto& event = reader.current();
    switch (event.event_type())
    {
        case staj_event_type::string_value:
            std::cout << event.as<jsoncons::string_view>() << "\n";
            break;
    }
}
```
Output:
```
Haruki Murakami
Graham Greene
```

See [json_cursor](doc/ref/json_cursor.md) 

### Iterate over a json stream with staj iterators

```c++
const std::string example = R"(
[ 
  { 
      "employeeNo" : "101",
      "name" : "Tommy Cochrane",
      "title" : "Supervisor"
  },
  { 
      "employeeNo" : "102",
      "name" : "Bill Skeleton",
      "title" : "Line manager"
  }
]
)";

int main()
{
    std::istringstream is(example);

    json_cursor reader(is);

    staj_array_iterator<json> it(reader);

    for (const auto& j : it)
    {
        std::cout << pretty_print(j) << "\n";
    }
    std::cout << "\n\n";
}
```
Output:
```
{
    "employeeNo": "101",
    "name": "Tommy Cochrane",
    "title": "Supervisor"
}
{
    "employeeNo": "102",
    "name": "Bill Skeleton",
    "title": "Line manager"
}
```
See [staj_array_iterator](doc/ref/staj_array_iterator.md) and [staj_object_iterator](doc/ref/staj_object_iterator.md)

<div id="E6"/> 

### Dump json content into a larger document

```c++
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    const json some_books = json::parse(R"(
    [
        {
            "title" : "Kafka on the Shore",
            "author" : "Haruki Murakami",
            "price" : 25.17
        },
        {
            "title" : "Women: A Novel",
            "author" : "Charles Bukowski",
            "price" : 12.00
        }
    ]
    )");

    const json more_books = json::parse(R"(
    [
        {
            "title" : "A Wild Sheep Chase: A Novel",
            "author" : "Haruki Murakami",
            "price" : 9.01
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer",
            "price" : 8.00
        }
    ]
    )");

    json_encoder encoder(std::cout, jsoncons::indenting::indent); // pretty print
    serializer.begin_array();
    for (const auto& book : some_books.array_range())
    {
        book.dump(encoder);
    }
    for (const auto& book : more_books.array_range())
    {
        book.dump(encoder);
    }
    serializer.end_array();
    serializer.flush();
}
```
Output:
```json
[
    {
        "author": "Haruki Murakami",
        "price": 25.17,
        "title": "Kafka on the Shore"
    },
    {
        "author": "Charles Bukowski",
        "price": 12.0,
        "title": "Women: A Novel"
    },
    {
        "author": "Haruki Murakami",
        "price": 9.01,
        "title": "A Wild Sheep Chase: A Novel"
    },
    {
        "author": "Ivan Passer",
        "price": 8.0,
        "title": "Cutter's Way"
    }
]
```

## Building the test suite and examples with CMake

[CMake](https://cmake.org/) is a cross-platform build tool that generates makefiles and solutions for the compiler environment of your choice. On Windows you can download a [Windows Installer package](https://cmake.org/download/). On Linux it is usually available as a package, e.g., on Ubuntu,
```
sudo apt-get install cmake
```
Once cmake is installed, you can build the tests:
```
mkdir build
cd build
cmake ../ -DBUILD_TESTS=ON
cmake --build . --target test_jsoncons --config Release
```
Run from the jsoncons tests directory:

On Windows:
```
..\build\tests\Release\test_jsoncons
```

On UNIX:
```
../build/tests/Release/test_jsoncons
```

## Acknowledgements

The jsoncons platform dependent binary configuration draws on to the excellent MIT licensed [tinycbor](https://github.com/intel/tinycbor).

A big thanks to Milo Yip, author of [RapidJSON](http://rapidjson.org/), for raising the quality of JSON libraries across the board, by publishing [the benchmarks](https://github.com/miloyip/nativejson-benchmark), and contacting this project (among others) to share the results.

The jsoncons implementation of the Grisu3 algorithm for printing floating-point numbers follows Florian Loitsch's MIT licensed [grisu3_59_56 implementation](http://florian.loitsch.com/publications), with minor modifications. 

The macro `JSONCONS_MEMBER_TRAITS_DECL` was inspired by Martin York's [ThorsSerializer](https://github.com/Loki-Astari/ThorsSerializer)

Special thanks to our [contributors](https://github.com/danielaparker/jsoncons/blob/master/acknowledgements.md)

