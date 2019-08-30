### jsoncons::csv::csv_encode_options

An abstract class that defines accessors for CSV encode options.

#### Header
```c++
#include <jsoncons/csv/csv_options.hpp>
```

#### Implementing classes

[csv_options](csv_options.md)

#### Accessors

    virtual chars_format floating_point_format() const = 0;

    virtual int precision() const = 0;

    virtual std::vector<string_type> column_names() const = 0;

    virtual CharT field_delimiter() const = 0;

    virtual std::pair<CharT,bool> subfield_delimiter() const = 0;

    virtual string_type line_delimiter() const = 0;

    virtual CharT quote_char() const = 0;

    virtual CharT quote_escape_char() const = 0;

    virtual quote_style_type quote_style() const = 0;

