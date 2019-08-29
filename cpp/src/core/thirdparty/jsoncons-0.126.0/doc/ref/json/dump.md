### `jsoncons::json::dump`

```c++
template <class SAllocator>
void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s) const; // (1)

template <class SAllocator>
void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s, 
          indenting line_indent) const; // (2)

template <class SAllocator>
void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s, 
          const json_options& options) const; // (3)

template <class SAllocator>
void dump(std::basic_string<char_type,char_traits_type,SAllocator>& s,
          const basic_json_options<char_type>& options, 
          indenting line_indent) const; // (4)

void dump(std::ostream& os) const; // (5)

void dump(std::ostream<CharT> os, indenting line_indent) const; // (6)

void dump(std::ostream<CharT> os, const json_options& options) const; // (7)

void dump(std::ostream<CharT> os, const json_options& options, indenting line_indent) const; // (8)

void dump(basic_json_content_handler<char_type>& content_handler) const; // (9)
```

(1) Dumps json value to string using default json_options.

(2) Dumps json value to string using default serialization options and the specified [indenting](../indenting.md). 

(3) Dumps json value to string using specified [json_options](../json_options.md). 

(4) Dumps json value to string using the specified [json_options](../json_options.md) and [indenting](../indenting.md). 

(5) Dumps json value to stream with default serialization options. 

(6) Dumps json value to stream using default serialization options and the specified [indenting](../indenting.md). 

(7) Dumps json value to stream using specified [json_options](../json_options.md). 

(8) Dumps json value to stream using the specified [json_options](../json_options.md) and [indenting](../indenting.md). 

(9) Emits json value to the [json_content_handler](../json_content_handler.md).

### Examples

#### Dump json value to csv file

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_encoder.hpp>

using namespace jsoncons;

int main()
{
    const json books = json::parse(R"(
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
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer"
        }
    ]
    )");

    csv_options options;
    options.column_names("author,title,price");

    csv_encoder encoder(std::cout, options);

    books.dump(encoder);
}
```

Output:

```csv
author,title,price
Haruki Murakami,Kafka on the Shore,25.17
Charles Bukowski,Women: A Novel,12.0
Ivan Passer,Cutter's Way,
```

#### Dump json content into a larger document

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
    encoder.begin_array();
    for (const auto& book : some_books.array_range())
    {
        book.dump(encoder);
    }
    for (const auto& book : more_books.array_range())
    {
        book.dump(encoder);
    }
    encoder.end_array();
    encoder.flush();
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
