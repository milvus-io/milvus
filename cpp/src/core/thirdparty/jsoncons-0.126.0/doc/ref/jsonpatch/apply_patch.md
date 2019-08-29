### jsoncons::jsonpatch::apply_patch

Applies a patch to a `json` document.

#### Header
```c++
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

template <class Json>
void apply_patch(Json& target, const Json& patch); // (1)

template <class Json>
void apply_patch(Json& target, const Json& patch, std::error_code& ec); // (2)
```

#### Return value

None

#### Exceptions

(1) Throws a [jsonpatch_error](jsonpatch_error.md) if `apply_patch` fails.
  
(2) Sets the `std::error_code&` to the [jsonpatch_error_category](jsonpatch_errc.md) if `apply_patch` fails. 

### Examples

#### Apply a JSON Patch with two add operations

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using namespace jsoncons::literals;
namespace jp = jsoncons::jsonpatch;

int main()
{
    jsoncons::json target = R"(
        { "foo": "bar"}
    )"_json;

    jsoncons::json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )"_json;

    std::error_code ec;
    jp::apply_patch(target,patch,ec);

    std::cout << pretty_print(target) << std::endl;
}
```
Output:
```
{
    "baz": "qux",
    "foo": ["bar","baz"]
}
```

#### Apply a JSON Patch with three add operations, the last one fails

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using namespace jsoncons::literals;
namespace jp = jsoncons::jsonpatch;

int main()
{
    jsoncons::json target = R"(
        { "foo": "bar"}
    )"_json;

    jsoncons::json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )"_json;

    std::error_code ec;
    jp::apply_patch(target, patch, ec);

    std::cout << "(1) " << ec.message() << std::endl;
    std::cout << "(2) " << target << std::endl;
}
```
Output:
```
(1) JSON Patch add operation failed
(2) {"foo":"bar"}
```
Note that all JSON Patch operations have been rolled back, and target is in its original state.

