### jsoncons::json_parser

```c++
typedef basic_json_parser<char> json_parser
```
`json_parser` is an incremental json parser. It can be fed its input
in chunks, and does not require an entire file to be loaded in memory
at one time.

A buffer of text is supplied to the parser with a call to `update(buffer)`. 
If a subsequent call to `parse_some` reaches the end of the buffer in the middle of parsing, 
say after digesting the sequence 'f', 'a', 'l', member function `stopped()` will return `false` 
and `source_exhausted()` will return `true`. Additional JSON text can be supplied to the parser, 
`parse_some` called again, and parsing will resume from where it left off. 

A typical application will repeatedly call the `parse_some` function 
until `stopped()` returns true. A stopped state indicates that a content
handler function returned `false`, an error occured, or a complete JSON 
text has been consumed. If the latter, `done() `will return `true`.
  
As an alternative to repeatedly calling `parse_some()` until `stopped()`
returns `true`, when `source_exhausted()` is `true` and there is
no more input, `finish_parse` may be called.  
 
`check_done` can be called to check if the input has any unconsumed 
non-whitespace characters, which would normally be considered an error.  

`json_parser` is used by the push parser [json_reader](json_reader.md),
and by the pull parser [json_cursor](json_cursor.md).

`json_parser` is noncopyable and nonmoveable.

#### Header
```c++
#include <jsoncons/json_parser.hpp>
```
#### Constructors

    json_parser(); // (1)

    json_parser(const json_decode_options& options); // (2)

    json_parser(parse_error_handler& err_handler); // (3)

    json_parser(const json_decode_options& options, 
                parse_error_handler& err_handler); // (4)

(1) Constructs a `json_parser` that uses default [json_decode_options](json_decode_options.md)
and a default [parse_error_handler](parse_error_handler.md).

(2) Constructs a `json_parser` that uses the specified [json_decode_options](json_decode_options.md)
and a default [parse_error_handler](parse_error_handler.md).

(3) Constructs a `json_parser` that uses default [json_decode_options](json_decode_options.md)
and a specified [parse_error_handler](parse_error_handler.md).

(4) Constructs a `json_parser` that uses the specified [json_decode_options](json_decode_options.md)
and a specified [parse_error_handler](parse_error_handler.md).

Note: It is the programmer's responsibility to ensure that `json_reader` does not outlive any error handler passed in the constuctor.

#### Member functions

    void update(const string_view_type& sv)
    void update(const char* data, size_t length)
Update the parser with a chunk of JSON

    bool done() const
Returns `true` when the parser has consumed a complete JSON text, `false` otherwise

    bool stopped() const
Returns `true` if the parser is stopped, `false` otherwise.
The parser may enter a stopped state as a result of a content handler
function returning `false`, an error occurred,
or after having consumed a complete JSON text.

    bool finished() const
Returns `true` if the parser is finished parsing, `false` otherwise.

    bool source_exhausted() const
Returns `true` if the input in the source buffer has been exhausted, `false` otherwise

    void parse_some(json_content_handler& handler)
Parses the source until a complete json text has been consumed or the source has been exhausted.
Parse events are sent to the supplied `handler`.
Throws [ser_error](ser_error.md) if parsing fails.

    void parse_some(json_content_handler<CharT>& handler,
                    std::error_code& ec)
Parses the source until a complete json text has been consumed or the source has been exhausted.
Parse events are sent to the supplied `handler`.
Sets `ec` to a [json_errc](jsoncons::json_errc.md) if parsing fails.

    void finish_parse(json_content_handler<CharT>& handler)
Called after `source_exhausted()` is `true` and there is no more input. 
Repeatedly calls `parse_some(handler)` until `finished()` returns `true`
Throws [ser_error](ser_error.md) if parsing fails.

    void finish_parse(json_content_handler<CharT>& handler,
                   std::error_code& ec)
Called after `source_exhausted()` is `true` and there is no more input. 
Repeatedly calls `parse_some(handler)` until `finished()` returns `true`
Sets `ec` to a [json_errc](jsoncons::json_errc.md) if parsing fails.

    void skip_bom()
Reads the next JSON text from the stream and reports JSON events to a [json_content_handler](json_content_handler.md), such as a [json_decoder](json_decoder.md).
Throws [ser_error](ser_error.md) if parsing fails.

    void check_done()
Throws if there are any unconsumed non-whitespace characters in the input.
Throws [ser_error](ser_error.md) if parsing fails.

    void check_done(std::error_code& ec)
Sets `ec` to a [json_errc](jsoncons::json_errc.md) if parsing fails.

    size_t reset() const
Resets the state of the parser to its initial state. In this state
`stopped()` returns `false` and `done()` returns `false`.

    size_t restart() const
Resets the `stopped` state of the parser to `false`, allowing parsing
to continue.

### Examples

#### Incremental parsing

```c++
int main()
{
    json_parser parser;
    jsoncons::json_decoder<json> decoder;
    try
    {
        parser.update("10");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update(".5");
        parser.parse_some(decoder); // This is the end, but the parser can't tell
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder); // Indicates that this is the end
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done(); // Checks if there are any unconsumed 
                             // non-whitespace characters in the input
        std::cout << "(4) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        json j = decoder.get_result();
        std::cout << "(5) " << j << "\n";
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << std::endl;
    }
}
```

Output:

```
(1) done: false, source_exhausted: true

(2) done: false, source_exhausted: true

(3) done: true, source_exhausted: true

(4) done: true, source_exhausted: true

(5) 10.5
```

#### Incremental parsing with unconsumed non-whitespace characters

```c++
int main()
{
    json_parser parser;
    jsoncons::json_decoder<json> decoder;
    try
    {
        parser.update("[10");
        parser.parse_some(decoder);
        std::cout << "(1) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.update(".5]{}");
        parser.parse_some(decoder); // The parser reaches the end at ']'
        std::cout << "(2) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.finish_parse(decoder); // Indicates that this is the end
        std::cout << "(3) done: " << std::boolalpha << parser.done() << ", source_exhausted: " << parser.source_exhausted() << "\n\n";

        parser.check_done(); // Checks if there are any unconsumed 
                             // non-whitespace characters in the input
                             // (there are)
    }
    catch (const ser_error& e)
    {
        std::cout << "(4) " << e.what() << std::endl;
    }
}
```

Output:

```
(1) done: false, source_exhausted: true

(2) done: true, source_exhausted: false

(3) done: true, source_exhausted: false

(4) Unexpected non-whitespace character after JSON text at line 1 and column 7
```

#### nan, inf, and -inf substitition

```c++
int main()
{
    std::string s = R"(
        {
           "A" : "NaN",
           "B" : "Infinity",
           "C" : "-Infinity"
        }
    )";

    json_options options; // Implements json_decode_options
    options.nan_to_str("NaN")
           .inf_to_str("Infinity");

    json_parser parser(options);
    jsoncons::json_decoder<json> decoder;
    try
    {
        parser.update(s);
        parser.parse_some(decoder);
        parser.finish_parse(decoder);
        parser.check_done();
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << std::endl;
    }

    json j = decoder.get_result(); // performs move
    if (j["A"].is<double>())
    {
        std::cout << "A: " << j["A"].as<double>() << std::endl;
    }
    if (j["B"].is<double>())
    {
        std::cout << "B: " << j["B"].as<double>() << std::endl;
    }
    if (j["C"].is<double>())
    {
        std::cout << "C: " << j["C"].as<double>() << std::endl;
    }
}
```

Output:

```
A: nan
B: inf
C: -inf
```


