### jsoncons::jsonpatch::from_diff

Create a JSON Patch from a diff of two json documents.

#### Header
```c++
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

template <class Json>
Json from_diff(const Json& source, const Json& target)
```

#### Return value

Returns a JSON Patch.  

### Examples

#### Create a JSON Patch

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using namespace jsoncons::literals;
namespace jp = jsoncons::jsonpatch;

int main()
{
    jsoncons::json source = R"(
        {"/": 9, "foo": "bar"}
    )"_json;

    jsoncons::json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    auto patch = jp::from_diff(source, target);

    std::error_code ec;
    jp::apply_patch(source, patch, ec);

    std::cout << "(1) " << pretty_print(patch) << std::endl;
    std::cout << "(2) " << pretty_print(source) << std::endl;
}
```
Output:
```
(1) 
[
    {
        "op": "remove",
        "path": "/~1"
    },
    {
        "op": "replace",
        "path": "/foo",
        "value": ["bar","baz"]
    },
    {
        "op": "add",
        "path": "/baz",
        "value": "qux"
    }
]
(2) 
{
    "baz": "qux",
    "foo": ["bar","baz"]
}
```

