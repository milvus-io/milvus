### jsoncons::default_parse_error_handler

```c++
class default_parse_error_handler;
```

#### Header

    #include <jsoncons/parse_error_handler.hpp>

#### Base class

[parse_error_handler](parse_error_handler.md)  
  
##### Private virtual implementation methods

     bool do_error(std::error_code ec, const ser_context& context) override;

Returns `true` if `ec` indicates a comment, otherwise `false`
    

