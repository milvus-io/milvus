### jsoncons::staj_filter

```c++
typedef basic_staj_filter<char> staj_filter;
```

#### Header
```c++
#include <jsoncons/staj_reader.hpp>
```

#### Destructor

    virtual ~basic_staj_filter() = default;

#### Member functions

    virtual bool accept(const staj_event& event, const ser_context& context) = 0;
Tests whether the [current event](staj_event.md) is part of the stream. Returns `true` if the filter accepts the event, `false` otherwise.

