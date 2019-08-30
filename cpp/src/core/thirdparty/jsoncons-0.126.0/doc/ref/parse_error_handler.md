### jsoncons::parse_error_handler

```c++
class parse_error_handler;
```

When parsing JSON text with [json_reader](json_reader.md), if you need to implement
customized error handling, you must implement this abstract class
and pass it in the constructor of [json_reader](json_reader.md). The `read` method 
will then report all warnings and errors through this interface.

#### Header

    #include <jsoncons/json_error_handler.hpp>

#### Destructor

    virtual ~json_error_handler()

#### Public interface methods

    void error(std::error_code ec,
               const ser_context& context) throw (ser_error) = 0
Called for recoverable errors. Calls `do_error`, if `do_error` returns `false`, throws a [ser_error](ser_error.md), otherwise an attempt is made to recover.

    void fatal_error(std::error_code ec,
                     const ser_context& context) throw (ser_error) = 0
Called for unrecoverable errors. Calls `do_fatal_error` and throws a [ser_error](ser_error.md).

#### Private virtual implementation methods

    virtual bool do_error(std::error_code ec,
                          const ser_context& context) = 0
Receive an error event, possibly recoverable. An [error_code](json_error_category.md) indicates the type of error. Contextual information including
line and column number is provided in the [context](ser_context.md) parameter. Returns `false` to fail, `true` to attempt recovery.

    virtual void do_fatal_error(std::error_code ec,
                                const ser_context& context) = 0
Receives a non recoverable error. An [error_code](json_error_category.md) indicates the type of error. Contextual information including
line and column number is provided in the [context](ser_context.md) parameter. 
    

