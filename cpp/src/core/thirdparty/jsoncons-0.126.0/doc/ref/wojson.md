### jsoncons::wojson

```c++
typedef basic_json<wchar_t,
                   ImplementationPolicy = original_order_policy,
                   Allocator = std::allocator<wchar_t>> wojson
```
The `wojson` class is an instantiation of the `basic_json` class template that uses `wchar_t` as the character type. The original insertion order of an object's name/value pairs is preserved. 

The `jsoncons` library will always rebind the supplied allocator from the template parameter to internal data structures.

#### Header
```c++
#include <jsoncons/json.hpp>
```
#### Interface

The interface is the same as [wjson](wjson.md), substituting wide character instantiations of classes - `std::wstring`, `std::wistream`, etc. - for utf8 character ones.

- `wojson`, like `wjson`, supports object member `insert_or_assign` methods that take an `object_iterator` as the first parameter. But while with `wjson` that parameter is just a hint that allows optimization, with `wojson` it is the actual location where to insert the member.

- In `wojson`, the `insert_or_assign` members that just take a name and a value always insert the member at the end.

#### See also

- [wjson](wjson.md) constructs a wide character json value that sorts name-value members alphabetically

- [json](json.md) constructs a utf8 character json value that sorts name-value members alphabetically

- [ojson](ojson.md) constructs a utf8 character json value that preserves the original insertion order of an object's name/value pairs

