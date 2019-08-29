### jsoncons::csv::encode_csv

Encodes a C++ data structure into the CSV data format.

#### Header
```c++
#include <jsoncons_ext/csv/csv_encoder.hpp>

template <class T,class CharT>
void encode_csv(const T& val, 
                std::basic_string<CharT>& s, 
                const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())); // (1)

template <class T, class CharT>
void encode_csv(const T& val, 
                std::basic_ostream<CharT>& os, 
                const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())); // (2)
```

(1) Writes a value of type T into a string in the CSV data format. Type T must be an instantiation of [basic_json](../json.md) 
or support [json_type_traits](../json_type_traits.md). Uses the [encode options](csv_options.md)
supplied or defaults.

(2) Writes a value of type T into an output stream in the CSV data format. Type T must be an instantiation of [basic_json](../json.md) 
or support [json_type_traits](../json_type_traits.md). Uses the [encode options](csv_options.md)
supplied or defaults.

### Examples

#### Write a json value to a CSV output stream

```c++
#include <iostream>
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

    csv::encode_csv(books, std::cout);
}
```
Output:
```json
author,price,title
Haruki Murakami,00,Kafka on the Shore
Charles Bukowski,00,Women: A Novel
Ivan Passer,,Cutter's Way
```


