### jsoncons::decode_json

Decodes a JSON data format to a C++ data structure. `decode_json` will 
work for all C++ classes that have [json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md) defined.

#### Header

```c++
#include <jsoncons/json.hpp>

template <class T, class CharT>
T decode_json(std::basic_istream<CharT>& is,
              const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options()); // (1)

template <class T, class CharT>
T decode_json(const std::basic_string<CharT>& s,
              const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options()); // (2)


template <class T, class CharT, class ImplementationPolicy, class Allocator>
T decode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
              std::basic_istream<CharT>& is,
              const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options()); // (3)

template <class T, class CharT, class ImplementationPolicy, class Allocator>
T decode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
              const std::basic_string<CharT>& s,
              const basic_json_decode_options<CharT>& options = basic_json_options<CharT>::default_options()); // (4)
```

(1) Reads a JSON string value into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

(2) Reads a JSON input stream into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

Functions (1)-(2) perform encodings using the default json type `basic_json<CharT>`.
Functions (3)-(4) are the same but perform encodings using the supplied `basic_json`.

### Examples

#### Map with string-tuple pairs

```c++
#include <iostream>
#include <map>
#include <tuple>
#include <jsoncons/json.hpp>

int main()
{
    typedef std::map<std::string,std::tuple<std::string,std::string,double>> employee_collection;

    std::string s = R"(
    {
        "Jane Doe": ["Commission","Sales",20000.0],
        "John Smith": ["Hourly","Software Engineer",10000.0]
    }
    )";

    employee_collection employees = jsoncons::decode_json<employee_collection>(s);

    for (const auto& pair : employees)
    {
        std::cout << pair.first << ": " << std::get<1>(pair.second) << std::endl;
    }
}
```
Output:
```
Jane Doe: Sales
John Smith: Software Engineer
```

#### See also

- [encode_json](encode_json.md)

