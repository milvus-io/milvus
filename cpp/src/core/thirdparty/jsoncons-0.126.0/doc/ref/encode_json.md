### jsoncons::encode_json

Encode a C++ data structure to a JSON formatted string or stream. `encode_json` will work for all types that
have [json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md) defined.

#### Header
```c++
#include <jsoncons/json.hpp>

template <class T, class CharT>
void encode_json(const T& val,
                 std::basic_ostream<CharT>& os, 
                 const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
                 indenting line_indent = indenting::no_indent); // (1)

template <class T, class CharT>
void encode_json(const T& val, 
                 std::basic_ostream<CharT>& os, 
                 indenting line_indent); // (2)

template <class T, class CharT>
void encode_json(const T& val,
                 std::basic_string<CharT>& s, 
                 const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
                 indenting line_indent = indenting::no_indent); // (3)

template <class T, class CharT>
void encode_json(const T& val, 
                 std::basic_string<CharT>& s, 
                 indenting line_indent); // (4)

template <class T, class CharT>
void encode_json(const T& val, 
                 basic_json_content_handler<CharT>& receiver); // (5)

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_ostream<CharT>& os, 
                 const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
                 indenting line_indent = indenting::no_indent); // (6)

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_ostream<CharT>& os, 
                 indenting line_indent); // (7)

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_string<CharT>& s, 
                 const basic_json_encode_options<CharT>& options = basic_json_options<CharT>::default_options(), 
                 indenting line_indent = indenting::no_indent); // (8)

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT,ImplementationPolicy,Allocator>& j,
                 const T& val,
                 std::basic_string<CharT>& s, 
                 indenting line_indent); // (9)

template <class T, class CharT, class ImplementationPolicy, class Allocator>
void encode_json(const basic_json<CharT, ImplementationPolicy, Allocator>& j, 
                 const T& val,
                 basic_json_content_handler<CharT>& receiver); // (10)
```

(1) Encode `val` to output stream with the specified options and line indenting.

(2) Encode `val` to output stream with the specified line indenting.

(3) Encode `val` to string with the specified options and line indenting.

(4) Encode `val` to string with the specified line indenting.

(5) Convert `val` to json events and stream through content handler.

Functions (1)-(5) perform encodings using the default json type `basic_json<CharT>`.
Functions (6)-(10) are the same but perform encodings using the supplied `basic_json`.

#### Parameters

<table>
  <tr>
    <td>val</td>
    <td>C++ data structure</td> 
  </tr>
  <tr>
    <td>handler</td>
    <td>JSON output handler</td> 
  </tr>
  <tr>
    <td>options</td>
    <td>Serialization options</td> 
  </tr>
  <tr>
    <td>os</td>
    <td>Output stream</td> 
  </tr>
  <tr>
    <td>indenting</td>
    <td><code>indenting::indent</code> to pretty print, <code>indenting::no_indent</code> for compact output</td> 
  </tr>
</table>

#### Return value

None 

#### See also

- [json_content_handler](json_content_handler.md)
- [json_options](json_options.md)
    
### Examples

#### Map with string-tuple pairs

```c++
#include <iostream>
#include <map>
#include <tuple>
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    typedef std::map<std::string,std::tuple<std::string,std::string,double>> employee_collection;

    employee_collection employees = 
    { 
        {"John Smith",{"Hourly","Software Engineer",10000}},
        {"Jane Doe",{"Commission","Sales",20000}}
    };

    std::cout << "(1)\n" << std::endl; 
    encode_json(employees,std::cout);
    std::cout << "\n\n";

    std::cout << "(2) Again, with pretty print\n" << std::endl; 
    encode_json(employees, std::cout, jsoncons::indenting::indent);
}
```
Output:
```
(1)

{"Jane Doe":["Commission","Sales",20000.0],"John Smith":["Hourly","Software Engineer",10000.0]}

(2) Again, with pretty print

{
    "Jane Doe": ["Commission","Sales",20000.0],
    "John Smith": ["Hourly","Software Engineer",10000.0]
}
```
    
#### Contain JSON output in an object

```c++
#include <iostream>
#include <map>
#include <tuple>
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::map<std::string,std::tuple<std::string,std::string,double>> employees = 
    { 
        {"John Smith",{"Hourly","Software Engineer",10000}},
        {"Jane Doe",{"Commission","Sales",20000}}
    };

    json_encoder encoder(std::cout, jsoncons::indenting::indent); 

    encoder.begin_object();       
    encoder.write_name("Employees");       
    encode_json(employees, encoder);
    encoder.end_object();       
    encoder.flush();       
}
```
Output:
```json
{
    "Employees": {
        "Jane Doe": ["Commission","Sales",20000.0],
        "John Smith": ["Hourly","Software Engineer",10000.0]
    }
}
```

#### See also

- [decode_json](decode_json.md)


