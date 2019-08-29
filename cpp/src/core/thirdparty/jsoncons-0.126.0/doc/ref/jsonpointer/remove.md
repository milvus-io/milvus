### jsoncons::jsonpointer::remove

Removes a `json` element.

#### Header
```c++
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template<class J>
void remove(J& target, const typename J::string_view_type& path); // (1)

template<class J>
void remove(J& target, const typename J::string_view_type& path, std::error_code& ec); // (2)
```

Removes the value at the location specifed by `path`.

#### Return value

None

### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `remove` fails.
 
(2) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if `remove` fails. 

### Examples

#### Remove an object member

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto target = jsoncons::json::parse(R"(
        { "foo": "bar", "baz" : "qux"}
    )");

    std::error_code ec;
    jp::remove(target, "/baz", ec);
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
{"foo":"bar"}
```

#### Remove an array element

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto target = jsoncons::json::parse(R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )");

    std::error_code ec;
    jp::remove(target, "/foo/1", ec);
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
{"foo":["bar","baz"]}
```


