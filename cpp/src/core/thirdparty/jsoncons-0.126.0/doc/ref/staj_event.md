### jsoncons::staj_event

```c++
typedef basic_staj_event<char> staj_event;
```

#### Header
```c++
#include <jsoncons/staj_reader.hpp>

A JSON-like data event.
```

| Event type        | Sample data | Valid accessors |
|-------------------|------------------------|-----------------|
| begin_object      |                        | |            
| end_object        |                        | |
| begin_array       |                        | |
| end_array         |                        | |
| name              | "foo"                  | `as<std::string>()`, `as<jsoncons::string_view>`, `as<std::string_view>()` |
| string_value      | "1000"                 | `as<std::string>()`, `as<jsoncons::string_view>`, `as<std::string_view>()`, `as<int>()`, `as<unsigned>()` |
| byte_string_value | 0x660x6F0x6F           | `as<std::string>()`, `as<jsoncons::byte_string>()` |
| int64_value       | -1000                  | `as<std::string>()`, `as<int>()`, `as<long>`, `as<int64_t>()` |
| uint64_value      | 1000                   | `as<std::string>()`, `as<int>()`, `as<unsigned>()`, `as<int64_t>()`, `as<uint64_t>()` |
| double_value      | 125.72                 | `as<std::string>()`, `as<double>()` |
| bool_value        | true                   | `as<std::string>()`, `as<bool>()` |
| null_value        |                        | `as<std::string>()` |

#### Member functions

    staj_event_type event_type() const noexcept;
Returns a [staj_event_type](staj_event_type.md) for this event.

    semantic_tag get_semantic_tag() const noexcept;
Returns a [semantic_tag](semantic_tag.md) for this event.

    template <class T, class... Args>
    T as(Args&&... args) const;
Attempts to convert the json value to the template value type.

