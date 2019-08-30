### jsoncons::staj_reader

```c++
typedef basic_staj_reader<char> staj_reader
```

#### Header
```c++
#include <jsoncons/staj_reader.hpp>
```

The `staj_reader` interface supports forward, read-only, access to JSON and JSON-like data formats.

The `staj_reader` is designed to iterate over stream events until `done()` returns `true`.
The `next()` function causes the reader to advance to the next stream event. The `current()` function
returns the current stream event. The data can be accessed using the [staj_event](staj_event.md) 
interface. When `next()` is called, copies of data previously accessed may be invalidated.

#### Destructor

    virtual ~basic_staj_reader() = default;

#### Member functions

    virtual bool done() const = 0;
Check if there are no more events.

    virtual const staj_event& current() const = 0;
Returns the current [staj_event](staj_event.md).

    virtual void accept(json_content_handler& handler) = 0;
Sends the parse events from the current event to the
matching completion event to the supplied [handler](json_content_handler.md)
E.g., if the current event is `begin_object`, sends the `begin_object`
event and all inbetween events until the matching `end_object` event.
If a parsing error is encountered, throws a [ser_error](ser_error.md).

    virtual void accept(json_content_handler& handler,
                        std::error_code& ec) = 0;
Sends the parse events from the current event to the
matching completion event to the supplied [handler](json_content_handler.md)
E.g., if the current event is `begin_object`, sends the `begin_object`
event and all inbetween events until the matching `end_object` event.
If a parsing error is encountered, sets `ec`.

    virtual void next() = 0;
Get the next event. If a parsing error is encountered, throws a [ser_error](ser_error.md).

    virtual void next(std::error_code& ec) = 0;
Get the next event. If a parsing error is encountered, sets `ec`.

    virtual const ser_context& context() const = 0;
Returns the current [context](ser_context.md)

#### See also

- [staj_array_iterator](staj_array_iterator.md) 
- [staj_object_iterator](staj_object_iterator.md)

