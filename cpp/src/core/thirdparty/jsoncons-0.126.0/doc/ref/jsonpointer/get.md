### jsoncons::jsonpointer::get

Selects a `json` value.

#### Header
```c++
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template<class J>
typename std::enable_if<is_accessible_by_reference<J>::value,J&>::type
get(J& root, const typename J::string_view_type& path); // (1)

template<class J>
typename std::enable_if<is_accessible_by_reference<J>::value,const J&>::type
get(const J& root, const typename J::string_view_type& path); // (2)

template<class J>
typename std::enable_if<!is_accessible_by_reference<J>::value,J>::type
get(const J& root, const typename J::string_view_type& path); // (3)

template<class J>
typename std::enable_if<is_accessible_by_reference<J>::value,J&>::type
get(J& root, const typename J::string_view_type& path, std::error_code& ec); // (4)

template<class J>
typename std::enable_if<is_accessible_by_reference<J>::value,const J&>::type
get(const J& root, const typename J::string_view_type& path, std::error_code& ec); // (5)

template<class J>
typename std::enable_if<!is_accessible_by_reference<J>::value,J>::type
get(const J& root, const typename J::string_view_type& path, std::error_code& ec); // (6)
```

#### Return value

(1) On success, returns the selected item by reference. 

    json j = json::array{"baz","foo"};
    json& item = jsonpointer::get(j,"/0"); // "baz"

(2) On success, returns the selected item by const reference.  

    const json j = json::array{"baz","foo"};
    const json& item = jsonpointer::get(j,"/1"); // "foo"

(3) On success, returns the selected item by reference, otherwise an undefined item by reference.  

    json j = json::array{"baz","foo"};
    std::error_code ec;
    json& item = jsonpointer::get(j,"/1",ec); // "foo"

(4) On success, returns the selected item by const reference, otherwise an undefined item by const reference.  

    const json j = json::array{"baz","foo"};
    std::error_code ec;
    const json& item = jsonpointer::get(j,"/0",ec); // "baz"

### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if get fails.

(2) Throws a [jsonpointer_error](jsonpointer_error.md) if get fails.

(3) Throws a [jsonpointer_error](jsonpointer_error.md) if get fails.
 
(4) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if get fails. 
 
(5) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if get fails. 
 
(6) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if get fails. 

#### Requirements

The type J satisfies the requirements for `jsonpointer::get` if it defines the following types

name              |type                  |notes
------------------|----------------------|---------------
`value_type`      |`J::value_type`       |
`reference`       |`J::reference`        |
`const_reference` |`J::const_reference`  |
`pointer`         |`J::pointer`          |
`const_pointer`   |`J::const_pointer`    |
`string_type`     |`J::string_type`      |
`string_view_type`|`J::string_view_type` |

and given 

- a value `index` of type `size_t`
- a value `key` of type `string_view_type` 
- an rvalue expression `j` of type `J`

the following expressions are valid

expression     |return type                |effects
---------------|---------------------------|---------------
is_array()     |`bool`                     |
is_object()    |`bool`                     |
size()         |`size_t`                   |
contains(key)   |`bool`                     |
at(index)      |`reference` or `value_type`|
at(key)        |`reference` or `value_type`|

### See also

[basic_address](address.md)

### Examples

#### Select author from second book

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto j = jsoncons::json::parse(R"(
    [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ]
    )");

    // Using exceptions to report errors
    try
    {
        jsoncons::json result = jp::get(j, "/1/author");
        std::cout << "(1) " << result << std::endl;
    }
    catch (const jp::jsonpointer_error& e)
    {
        std::cout << e.what() << std::endl;
    }

    // Using error codes to report errors
    std::error_code ec;
    const jsoncons::json& result = jp::get(j, "/0/title", ec);

    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << "(2) " << result << std::endl;
    }
}
```
Output:
```json
(1) "Evelyn Waugh"
(2) "Sayings of the Century"
```

#### Using addresses with jsonpointer::get 

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto j = jsoncons::json::parse(R"(
       {
          "a/b": ["bar", "baz"],
          "m~n": ["foo", "qux"]
       }
    )");

    jp::address addr;
    addr /= "m~n";
    addr /= "1";

    std::cout << "(1) " << addr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& item : addr)
    {
        std::cout << item << "\n";
    }
    std::cout << "\n";

    jsoncons::json item = jp::get(j, addr);
    std::cout << "(3) " << item << "\n";
}
```
Output:
```
(1) /m~0n/1

(2)
m~n
1

(3) "qux"
```

#### Examples from [RFC6901](https://tools.ietf.org/html/rfc6901)

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto j = jsoncons::json::parse(R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )");

    try
    {
        const jsoncons::json& result1 = jp::get(j, "");
        std::cout << "(1) " << result1 << std::endl;
        const jsoncons::json& result2 = jp::get(j, "/foo");
        std::cout << "(2) " << result2 << std::endl;
        const jsoncons::json& result3 = jp::get(j, "/foo/0");
        std::cout << "(3) " << result3 << std::endl;
        const jsoncons::json& result4 = jp::get(j, "/");
        std::cout << "(4) " << result4 << std::endl;
        const jsoncons::json& result5 = jp::get(j, "/a~1b");
        std::cout << "(5) " << result5 << std::endl;
        const jsoncons::json& result6 = jp::get(j, "/c%d");
        std::cout << "(6) " << result6 << std::endl;
        const jsoncons::json& result7 = jp::get(j, "/e^f");
        std::cout << "(7) " << result7 << std::endl;
        const jsoncons::json& result8 = jp::get(j, "/g|h");
        std::cout << "(8) " << result8 << std::endl;
        const jsoncons::json& result9 = jp::get(j, "/i\\j");
        std::cout << "(9) " << result9 << std::endl;
        const jsoncons::json& result10 = jp::get(j, "/k\"l");
        std::cout << "(10) " << result10 << std::endl;
        const jsoncons::json& result11 = jp::get(j, "/ ");
        std::cout << "(11) " << result11 << std::endl;
        const jsoncons::json& result12 = jp::get(j, "/m~0n");
        std::cout << "(12) " << result12 << std::endl;
    }
    catch (const jp::jsonpointer_error& e)
    {
        std::cerr << e.what() << std::endl;
    }
}
```
Output:
```json
(1) {"":0," ":7,"a/b":1,"c%d":2,"e^f":3,"foo":["bar","baz"],"g|h":4,"i\\j":5,"k\"l":6,"m~n":8}
(2) ["bar","baz"]
(3) "bar"
(4) 0
(5) 1
(6) 2
(7) 3
(8) 4
(9) 5
(10) 6
(11) 7
(12) 8
```


