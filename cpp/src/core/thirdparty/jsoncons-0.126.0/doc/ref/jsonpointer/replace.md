### jsoncons::jsonpointer::replace

Replace a `json` element or member.

#### Header
```c++
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template<class J>
void replace(J& target, const typename J::string_view_type& path, const J& value); 

template<class J>
void replace(J& target, const typename J::string_view_type& path, const J& value, std::error_code& ec); 
```

Replaces the value at the location specified by `path` with a new value. 

#### Return value

None

#### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `replace` fails.
 
(2) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if `replace` fails. 

### Examples

#### Replace an object value

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto target = jsoncons::json::parse(R"(
        {
          "baz": "qux",
          "foo": "bar"
        }
    )");

    std::error_code ec;
    jp::replace(target, "/baz", jsoncons::json("boo"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << target << std::endl;
    }
}
```
Output:
```json
{
    "baz": "boo",
    "foo": "bar"
}
```

#### Replace an array value

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto target = jsoncons::json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jp::replace(target, "/foo/1", jsoncons::json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << std::endl;
    }
    else
    {
        std::cout << pretty_print(target) << std::endl;
    }
}
```
Output:
```json
{
    "foo": ["bar","qux"]
}
```


