# jsoncons: a C++ library for json construction

[Preliminaries](#A1)

[Reading JSON text from a file](#A2)

[Constructing json values in C++](#A3)

[Conversion between JSON and C++ data structures](#A4)

[Converting CSV files to json](#A5  )

[Pretty print](#A6)

[Filters](#A7)

[JSONPath](#A8)

[About jsoncons::json](#A9)

[Wide character support](#A10)

[ojson and wojson](#A11)

<div id="A1"/>
### Preliminaries

jsoncons is a C++, header-only library for constructing [JSON](http://www.json.org) and JSON-like
data formats such as [CBOR](http://cbor.io/). It supports 

- Parsing JSON-like text or binary formats into a tree model
  that defines an interface for accessing and modifying that data (covers bignum and byte string values.)

- Serializing the tree model into different JSON-like text or binary formats.

- Converting from JSON-like text or binary formats to C++ data structures and back via [json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md).

- Streaming JSON read and write events, somewhat analogously to SAX (push parsing) and StAX (pull parsing) in the XML world. 

The jsoncons library is header-only: it consists solely of header files containing templates and inline functions, and requires no separately-compiled library binaries when linking. It has no dependence on other libraries. 

To install the librray, download the [latest release](https://github.com/danielaparker/jsoncons/releases) and unpack the zip file. Copy the directory `include/jsoncons` to your `include` directory. If you wish to use extensions, copy `include/jsoncons_ext` as well. 

Or, download the latest code on [master](https://github.com/danielaparker/jsoncons/archive/master.zip).

Compared to other JSON libraries, jsoncons has been designed to handle very large JSON texts. At its heart are
SAX style parsers and serializers. Its [json parser](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_parser.md) is an 
incremental parser that can be fed its input in chunks, and does not require an entire file to be loaded in memory at one time. 
Its tree model is more compact than most, and can be made more compact still with a user-supplied
allocator. It also supports memory efficient parsing of very large JSON texts with a [pull parser](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_cursor.md),
built on top of its incremental parser.  

The [jsoncons data model](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/data-model.md) supports the familiar JSON types - nulls,
booleans, numbers, strings, arrays, objects - plus byte strings. In addition, jsoncons 
supports semantic tagging of date-time values, timestamp values, big integers, 
big decimals, bigfloats and binary encodings. This allows it to preserve these type semantics when parsing 
JSON-like data formats such as CBOR that have them.

The jsoncons classes and functions are in namespace `jsoncons`. You need to include the header file
```c++ 
#include <jsoncons/json.hpp>
```
and, for convenience,
```c++
using jsoncons::json;
```

<div id="A2"/>
### Reading JSON text from a file

Example file (`books.json`):
```c++
[
    {
        "title" : "Kafka on the Shore",
        "author" : "Haruki Murakami",
        "price" : 25.17
    },
    {
        "title" : "Women: A Novel",
        "author" : "Charles Bukowski",
        "price" : 12.0
    },
    {
        "title" : "Cutter's Way",
        "author" : "Ivan Passer"
    }
]
```
It consists of an array of book elements, each element is an object with members title, author, and price.

Read the JSON text into a `json` value,
```c++
std::ifstream is("books.json");
json books = json::parse(is);
```
Loop through the book array elements, using a range-based for loop
```c++
for (const auto& book : books.array_range())
{
    std::string author = book["author"].as<std::string>();
    std::string title = book["title"].as<std::string>();
    std::cout << author << ", " << title << std::endl;
}
```
or begin-end iterators
```c++
for (auto it = books.array_range().begin(); 
     it != books.array_range().end();
     ++it)
{
    std::string author = (*it)["author"].as<std::string>();
    std::string title = (*it)["title"].as<std::string>();
    std::cout << author << ", " << title << std::endl;
} 
```
or a traditional for loop
```c++
for (size_t i = 0; i < books.size(); ++i)
{
    json& book = books[i];
    std::string author = book["author"].as<std::string>();
    std::string title = book["title"].as<std::string>();
    std::cout << author << ", " << title << std::endl;
}
```
Output:
```
Haruki Murakami, Kafka on the Shore
Charles Bukowski, Women: A Novel
Ivan Passer, Cutter's Way
```

Loop through the members of the third book element, using a range-based for loop

```c++
for (const auto& member : books[2].object_range())
{
    std::cout << member.key() << "=" 
              << member.value() << std::endl;
}
```

or begin-end iterators:

```c++
for (auto it = books[2].object_range().begin(); 
     it != books[2].object_range().end();
     ++it)
{
    std::cout << (*it).key() << "=" 
              << (*it).value() << std::endl;
} 
```
Output:
```
author=Ivan Passer
title=Cutter's Way
```

Note that the third book, Cutter's Way, is missing a price.

You have a choice of object member accessors:

- `book["price"]` will throw `std::out_of_range` if there is no price
- `book.get_with_default("price",std::string("n/a"))` will return the price converted to the
   default's data type, `std::string`, or `"n/a"` if there is no price.

So if you want to show "n/a" for the missing price, you can use this accessor
```c++
std::string price = book.get_with_default("price","n/a");
```
Or you can check if book has a member "price" with the method `contains`, and output accordingly,
```c++
if (book.contains("price"))
{
    double price = book["price"].as<double>();
    std::cout << price;
}
else
{
    std::cout << "n/a";
}
```
<div id="A3"/>
### Constructing json values in C++

The default `json` constructor produces an empty json object. For example 
```c++
json image_sizing;
std::cout << image_sizing << std::endl;
```
produces
```json
{}
```
To construct a json object with members, take an empty json object and set some name-value pairs
```c++
image_sizing.insert_or_assign("Resize To Fit",true);  // a boolean 
image_sizing.insert_or_assign("Resize Unit", "pixels");  // a string
image_sizing.insert_or_assign("Resize What", "long_edge");  // a string
image_sizing.insert_or_assign("Dimension 1",9.84);  // a double
image_sizing.insert_or_assign("Dimension 2",json::null());  // a null value
```

Or, use an object initializer-list:
```c++
json file_settings = json::object{
    {"Image Format", "JPEG"},
    {"Color Space", "sRGB"},
    {"Limit File Size", true},
    {"Limit File Size To", 10000}
};
```

To construct a json array, initialize with the array type 
```c++
json color_spaces = json::array();
```
and add some elements
```c++
color_spaces.push_back("sRGB");
color_spaces.push_back("AdobeRGB");
color_spaces.push_back("ProPhoto RGB");
```

Or, use an array initializer-list:
```c++
json image_formats = json::array{"JPEG","PSD","TIFF","DNG"};
```

The `operator[]` provides another way for setting name-value pairs.
```c++
json file_export;
file_export["File Format Options"]["Color Spaces"] = 
    std::move(color_spaces);
file_export["File Format Options"]["Image Formats"] = 
    std::move(image_formats);
file_export["File Settings"] = std::move(file_settings);
file_export["Image Sizing"] = std::move(image_sizing);
```
Note that if `file_export["File Format Options"]` doesn't exist, 
```c++
file_export["File Format Options"]["Color Spaces"] = 
    std::move(color_spaces)
```
creates `"File Format Options"` as an object and puts `"Color Spaces"` in it.

Serializing
```c++
std::cout << pretty_print(file_export) << std::endl;
```
produces
```json
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
        "Dimension 2": null,
        "Resize To Fit": true,
        "Resize Unit": "pixels",
        "Resize What": "long_edge"
    }
}
```
<div id="A4"/>
### Conversion between JSON and C++ data structures

jsoncons supports conversion between JSON text and C++ data structures. The functions [decode_json](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/decode_json.md) 
and [encode_json](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/encode_json.md) convert JSON formatted strings or streams to C++ data structures and back. 
Decode and encode work for all C++ classes that have 
[json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md) 
defined. The standard library containers are already supported, and you can specialize `json_type_traits`
for your own types in the `jsoncons` namespace. 

`JSONCONS_MEMBER_TRAITS_DECL` is a macro that simplifies the creation of the necessary boilerplate
for your own types.

```c++
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>

using namespace jsoncons;

namespace ns {

    struct reputon
    {
        std::string rater;
        std::string assertion;
        std::string rated;
        double rating;

        friend bool operator==(const reputon& lhs, const reputon& rhs)
        {
            return lhs.rater == rhs.rater &&
                lhs.assertion == rhs.assertion &&
                lhs.rated == rhs.rated &&
                lhs.rating == rhs.rating;
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
            if (lhs.application != rhs.application)
            {
                return false;
            }
            if (lhs.reputons.size() != rhs.reputons.size())
            {
                return false;
            }
            for (size_t i = 0; i < lhs.reputons.size(); ++i)
            {
                if (lhs.reputons[i] != rhs.reputons[i])
                {
                    return false;
                }
            }
            return true;
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

<div id="A5"/>
### Converting CSV files to json

Example CSV file (tasks.csv):

```
project_id, task_name, task_start, task_finish
4001,task1,01/01/2003,01/31/2003
4001,task2,02/01/2003,02/28/2003
4001,task3,03/01/2003,03/31/2003
4002,task1,04/01/2003,04/30/2003
4002,task2,05/01/2003,
```

You can read the `CSV` file into a `json` value with the `decode_csv` function.

```c++
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>
#include <jsoncons_ext/csv/csv_encoder.hpp>

using namespace jsoncons;

int main()
{
    std::ifstream is("input/tasks.csv");

    csv::csv_options options;
    options.assume_header(true)
           .trim(true)
           .ignore_empty_values(true) 
           .column_types("integer,string,string,string");
    ojson tasks = csv::decode_csv<ojson>(is, options);

    std::cout << "(1)\n" << pretty_print(tasks) << "\n\n";

    std::cout << "(2)\n";
    csv::encode_csv(tasks, std::cout);
}
```
Output:
```json
(1)
[
    {
        "project_id": 4001,
        "task_name": "task1",
        "task_start": "01/01/2003",
        "task_finish": "01/31/2003"
    },
    {
        "project_id": 4001,
        "task_name": "task2",
        "task_start": "02/01/2003",
        "task_finish": "02/28/2003"
    },
    {
        "project_id": 4001,
        "task_name": "task3",
        "task_start": "03/01/2003",
        "task_finish": "03/31/2003"
    },
    {
        "project_id": 4002,
        "task_name": "task1",
        "task_start": "04/01/2003",
        "task_finish": "04/30/2003"
    },
    {
        "project_id": 4002,
        "task_name": "task2",
        "task_start": "05/01/2003"
    }
]
```
There are a few things to note about the effect of the parameter settings.
- `assume_header` `true` tells the csv parser to parse the first line of the file for column names, which become object member names.
- `trim` `true` tells the parser to trim leading and trailing whitespace, in particular, to remove the leading whitespace in the column names.
- `ignore_empty_values` `true` causes the empty last value in the `task_finish` column to be omitted.
- The `column_types` setting specifies that column one ("project_id") contains integers and the remaining columns strings.

<div id="A6"/>
### Pretty print

The `pretty_print` function applies stylistic formatting to JSON text. For example

```c++
    json val;

    val["verts"] = json::array{1, 2, 3};
    val["normals"] = json::array{1, 0, 1};
    val["uvs"] = json::array{0, 0, 1, 1};

    std::cout << pretty_print(val) << std::endl;
```
produces

```json
{
    "normals": [1,0,1],
    "uvs": [0,0,1,1],
    "verts": [1,2,3]
}
```
By default, within objects, arrays of scalar values are displayed on the same line.

The `pretty_print` function takes an optional second parameter, [json_options](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_options.md), that allows custom formatting of output.
To display the array scalar values on a new line, set the `object_array_line_splits` property to `line_split_kind::new_line`. The code
```c++
json_options options;
format.object_array_line_splits(line_split_kind::new_line);
std::cout << pretty_print(val,options) << std::endl;
```
produces
```json
{
    "normals": [
        1,0,1
    ],
    "uvs": [
        0,0,1,1
    ],
    "verts": [
        1,2,3
    ]
}
```
To display the elements of array values on multiple lines, set the `object_array_line_splits` property to `line_split_kind::multi_line`. The code
```c++
json_options options;
format.object_array_line_splits(line_split_kind::multi_line);
std::cout << pretty_print(val,options) << std::endl;
```
produces
```json
{
    "normals": [
        1,
        0,
        1
    ],
    "uvs": [
        0,
        0,
        1,
        1
    ],
    "verts": [
        1,
        2,
        3
    ]
}
```
<div id="A7"/>
### Filters

You can rename object member names with the built in filter [rename_object_member_filter](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/rename_object_member_filter.md)

```c++
#include <sstream>
#include <jsoncons/json.hpp>
#include <jsoncons/json_filter.hpp>

using namespace jsoncons;

int main()
{
    std::string s = R"({"first":1,"second":2,"fourth":3,"fifth":4})";    

    json_encoder encoder(std::cout);

    // Filters can be chained
    rename_object_member_filter filter2("fifth", "fourth", encoder);
    rename_object_member_filter filter1("fourth", "third", filter2);

    // A filter can be passed to any function that takes
    // a json_content_handler ...
    std::cout << "(1) ";
    std::istringstream is(s);
    json_reader reader(is, filter1);
    reader.read();
    std::cout << std::endl;

    // or a json_content_handler    
    std::cout << "(2) ";
    ojson j = ojson::parse(s);
    j.dump(filter1);
    std::cout << std::endl;
}
```
Output:
```json
(1) {"first":1,"second":2,"third":3,"fourth":4}
(2) {"first":1,"second":2,"third":3,"fourth":4}
```
Or define and use your own filters. See [json_filter](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_filter.md) for details.
<div id="A8"/>
### JSONPath

[Stefan Goessner's JSONPath](http://goessner.net/articles/JsonPath/) is an XPATH inspired query language for selecting parts of a JSON structure.

Example JSON file (booklist.json):
```json
{ "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      },
      { "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
      }
    ]
  }
}
```
JSONPath examples:
```c++    
#include <jsoncons_ext/jsonpath/json_query.hpp>

using jsoncons::jsonpath::json_query;

std::ifstream is("./input/booklist.json");
json booklist = json::parse(is);

// The authors of books that are cheaper than $10
json result1 = json_query(booklist, "$.store.book[?(@.price < 10)].author");
std::cout << "(1) " << result1 << std::endl;

// The number of books
json result2 = json_query(booklist, "$..book.length");
std::cout << "(2) " << result2 << std::endl;

// The third book
json result3 = json_query(booklist, "$..book[2]");
std::cout << "(3)\n" << pretty_print(result3) << std::endl;

// All books whose author's name starts with Evelyn
json result4 = json_query(booklist, "$.store.book[?(@.author =~ /Evelyn.*?/)]");
std::cout << "(4)\n" << pretty_print(result4) << std::endl;

// The titles of all books that have isbn number
json result5 = json_query(booklist, "$..book[?(@.isbn)].title");
std::cout << "(5) " << result5 << std::endl;

// All authors and titles of books
json result6 = json_query(booklist, "$['store']['book']..['author','title']");
std::cout << "(6)\n" << pretty_print(result6) << std::endl;
```
Output:
```json
(1) ["Nigel Rees","Herman Melville"]
(2) [4]
(3)
[
    {
        "author": "Herman Melville",
        "category": "fiction",
        "isbn": "0-553-21311-3",
        "price": 8.99,
        "title": "Moby Dick"
    }
]
(4)
[
    {
        "author": "Evelyn Waugh",
        "category": "fiction",
        "price": 12.99,
        "title": "Sword of Honour"
    }
]
(5) ["Moby Dick","The Lord of the Rings"]
(6)
[
    "Nigel Rees",
    "Sayings of the Century",
    "Evelyn Waugh",
    "Sword of Honour",
    "Herman Melville",
    "Moby Dick",
    "J. R. R. Tolkien",
    "The Lord of the Rings"
]
```
<div id="A9"/>
### About jsoncons::json

The [json](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json.md) class is an instantiation of the `basic_json` class template that uses `char` as the character type
and sorts object members in alphabetically order.
```c++
typedef basic_json<char,
                   ImplementationPolicy = sorted_policy,
                   Allocator = std::allocator<char>> json;
```
If you prefer to retain the original insertion order, use [ojson](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/ojson.md) instead.

The library includes an instantiation for wide characters as well, [wjson](https://github.com/danielaparker/jsoncons/blob/master/ref/doc/wjson.md)
```c++
typedef basic_json<wchar_t,
                   ImplementationPolicy = sorted_policy,
                   Allocator = std::allocator<wchar_t>> wjson;
```
If you prefer to retain the original insertion order, use [wojson](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/wojson.md) instead.

Note that the allocator type allows you to supply a custom allocator. For example, you can use the boost [fast_pool_allocator](http://www.boost.org/doc/libs/1_60_0/libs/pool/doc/html/boost/fast_pool_allocator.html):
```c++
#include <boost/pool/pool_alloc.hpp>
#include <jsoncons/json.hpp>

typedef jsoncons::basic_json<char, boost::fast_pool_allocator<char>> myjson;

myjson o;

o.insert_or_assign("FirstName","Joe");
o.insert_or_assign("LastName","Smith");
```
This results in a json value being constucted with all memory being allocated from the boost memory pool. (In this particular case there is no improvement in performance over `std::allocator`.)

Note that the underlying memory pool used by the `boost::fast_pool_allocator` is never freed. 

<div id="A10"/>
### Wide character support

jsoncons supports wide character strings and streams with `wjson` and `wjson_reader`. It supports `UTF16` encoding if `wchar_t` has size 2 (Windows) and `UTF32` encoding if `wchar_t` has size 4. You can construct a `wjson` value in exactly the same way as a `json` value, for instance:
```c++
using jsoncons::wjson;

wjson root;
root[L"field1"] = L"test";
root[L"field2"] = 3.9;
root[L"field3"] = true;

std::wcout << root << L"\n";
```
which prints
```c++
{"field1":"test","field2":3.9,"field3":true}
```
<div id="A11"/>
### ojson and wojson

The [ojson](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/ojson.md) ([wojson](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/wojson.md)) class is an instantiation of the `basic_json` class template that uses `char` (`wchar_t`) as the character type and keeps object members in their original order. 
```c++
ojson o = ojson::parse(R"(
{
    "street_number" : "100",
    "street_name" : "Queen St W",
    "city" : "Toronto",
    "country" : "Canada"
}
)");

std::cout << pretty_print(o) << std::endl;
```
Output:
```json
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "country": "Canada"
}
```
Insert "postal_code" at end
```c++
o.insert_or_assign("postal_code", "M5H 2N2");

std::cout << pretty_print(o) << std::endl;
```
Output:
```json
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "country": "Canada",
    "postal_code": "M5H 2N2"
}
```
Insert "province" before "country"
```c++
auto it = o.find("country");
o.insert_or_assign(it,"province","Ontario");

std::cout << pretty_print(o) << std::endl;
```
Output:
```json
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "province": "Ontario",
    "country": "Canada",
    "postal_code": "M5H 2N2"
}
```

For more information, consult the latest [examples](https://github.com/danielaparker/jsoncons/blob/master/doc/Examples.md), [documentation](https://github.com/danielaparker/jsoncons/blob/master/doc/Home.md) and [roadmap](https://github.com/danielaparker/jsoncons/blob/master/Roadmap.md). 

