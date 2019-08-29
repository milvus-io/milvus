### jsoncons::staj_event_type

#### Header
```c++
#include <jsoncons/staj_reader.hpp>
```

```c++
enum class staj_event_type
{
    begin_object,
    end_object,
    begin_array,
    end_array,
    name,
    string_value,
    byte_string_value,
    bigint_value,
    bigdec_value,
    int64_value,
    uint64_value,
    double_value,
    bool_value,
    null_value
};
```

