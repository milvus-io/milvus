### jsoncons::jsonpointer::insert_or_assign

Adds a `json` value.

#### Header
```c++
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

template<class J>
void insert_or_assign(J& target, const typename J::string_view_type& path, const J& value); // (1)

template<class J>
void insert_or_assign(J& target, const typename J::string_view_type& path, const J& value, std::error_code& ec); // (2)
```

Inserts a value into the target at the specified path, or if the path specifies an object member that already has the same key, assigns the new value to that member

- If `path` specifies an array index, a new value is inserted into the array at the specified index.

- If `path` specifies an object member that does not already exist, a new member is added to the object.

- If `path` specifies an object member that does exist, that member's value is replaced.

#### Return value

None

### Exceptions

(1) Throws a [jsonpointer_error](jsonpointer_error.md) if `insert_or_assign` fails.
 
(2) Sets the `std::error_code&` to the [jsonpointer_error_category](jsonpointer_errc.md) if `insert_or_assign` fails. 

### Examples

#### Insert or assign an object member at a location that already exists

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
    jp::insert_or_assign(target, "/baz", jsoncons::json("qux"), ec);
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


