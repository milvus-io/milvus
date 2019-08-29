### jsonpatch extension

The jsonpatch extension implements the IETF standard [JavaScript Object Notation (JSON) Patch](https://tools.ietf.org/html/rfc6902)

<table border="0">
  <tr>
    <td><a href="apply_patch.md">apply_patch</a></td>
    <td>Apply JSON Patch operations to a JSON document.</td> 
  </tr>
  <tr>
    <td><a href="from_diff.md">from_diff</a></td>
    <td>Create a JSON patch from a diff of two JSON documents.</td> 
  </tr>
</table>

The JSON Patch IETF standard requires that the JSON Patch method is atomic, so that if any JSON Patch operation results in an error, the target document is unchanged.
The patch function implements this requirement by generating the inverse commands and building an undo stack, which is executed if any part of the patch fails.

### Examples

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using namespace jsoncons;

int main()
{
    // Apply a JSON Patch

    json doc = R"(
        { "foo": "bar"}
    )"_json;

    json doc2 = doc;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )"_json;

    std::error_code ec;
    jsonpatch::apply_patch(doc, patch, ec);

    std::cout << "(1)\n" << pretty_print(doc) << std::endl;

    // Create a JSON Patch from two JSON documents

    auto patch2 = jsonpatch::from_diff(doc2,doc);

    std::cout << "(2)\n" << pretty_print(patch2) << std::endl;

    jsonpatch::apply_patch(doc2, patch2, ec);

    std::cout << "(3)\n" << pretty_print(doc2) << std::endl;
}
```
Output:
```
(1)
{
    "baz": "qux",
    "foo": ["bar","baz"]
}
(2)
[
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
(3)
{
    "baz": "qux",
    "foo": ["bar","baz"]
}
```


