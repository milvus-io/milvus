### jsoncons::jsonpointer::insert

Adds a `json` value.

#### Header
```c++
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template<class J>
void insert(J& target, const typename J::string_view_type& path, const J& value); // (1) 

template<class J>
void insert(J& target, const typename J::string_view_type& path, const J& value, std::error_code& ec); // (2) 
```

Inserts a value into the target at the specified path, if the path doesn't specify an object member that already has the same key.

- If `path` specifies an array index, a new value is inserted into the array at the specified index.

- If `path` specifies an object member that does not already exist, a new member is added to the object.

#### Return value

None

### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `insert` fails.
 
(2) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if `insert` fails. 

### Examples

#### Add a member to a target location that does not already exist

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto target = jsoncons::json::parse(R"(
        { "foo": "bar"}
    )");

    std::error_code ec;
    jp::insert(target, "/baz", jsoncons::json("qux"), ec);
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
{"baz":"qux","foo":"bar"}
```

#### Insert an element to the second position in an array

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
    jp::insert(target, "/foo/1", jsoncons::json("qux"), ec);
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
{"foo":["bar","qux","baz"]}
```

#### Insert a value at the end of an array

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
    jp::insert(target, "/foo/-", jsoncons::json("qux"), ec);
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
{"foo":["bar","baz","qux"]}
```

#### Attempt to insert object member at a location that already exists

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jp = jsoncons::jsonpointer;

int main()
{
    auto target = jsoncons::json::parse(R"(
        { "foo": "bar", "baz" : "abc"}
    )");

    std::error_code ec;
    jp::insert(target, "/baz", jsoncons::json("qux"), ec);
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
```
Key already exists
```

#### Attempt to insert value to a location in an array that exceeds the size of the array

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
    jp::insert_or_assign(target, "/foo/3", jsoncons::json("qux"), ec);
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
```
Index exceeds array size
```

