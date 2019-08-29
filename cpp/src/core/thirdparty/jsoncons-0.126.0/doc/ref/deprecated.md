## Deprecated Features

As the `jsoncons` library has evolved, names have sometimes changed. To ease transition, jsoncons deprecates the old names but continues to support many of them. The deprecated names can be suppressed by defining macro JSONCONS_NO_DEPRECATED, which is recommended for new code.

In the table, <em>&#x2713;</em> indicates that the old name is still supported.

Component or location|Old name, now deprecated|<em>&#x2713;</em>|New name
--------|-----------|--------------|------------------------
class parse_error|`parse_error`|<em>&#x2713;</em>|`serialization_error`
class basic_json|`object_iterator`|<em>&#x2713;</em>|`object_iterator_type`
class basic_json|`const_object_iterator`|<em>&#x2713;</em>|`const_object_iterator_type`
class basic_json|`array_iterator`|<em>&#x2713;</em>|`array_iterator_type`
class basic_json|`const_array_iterator`|<em>&#x2713;</em>|`const_array_iterator_type`
class basic_json|add(size_t index, const json& val)|<em>&#x2713;</em>|`insert(array_iterator pos, const json& val)`
class basic_json|add(size_t index, json&& val)|<em>&#x2713;</em>|`insert(array_iterator pos, json&& val)`
class basic_json|dump_body|<em>&#x2713;</em>|`dump`
class basic_json|remove_range(size_t from_index, size_t to_index)|<em>&#x2713;</em>|`erase(array_iterator first, array_iterator last)`
class basic_json|remove(const std::string& name)|<em>&#x2713;</em>|`erase(const string_view_type& name)`
class basic_json|parse_stream(std::istream& is)|<em>&#x2713;</em>|`parse(std::istream& is)`
class basic_json|parse_stream(std::istream& is, parse_error_handler& err_handler)|<em>&#x2713;</em>|`parse(std::istream& is, parse_error_handler& err_handler)`
class basic_json|as_int() const|<em>&#x2713;</em>|`as<int>`
class basic_json|as_uint() const|<em>&#x2713;</em>|`as<unsigned int>`
class basic_json|as_long() const|<em>&#x2713;</em>|`as<long>`
class basic_json|as_ulong() const|<em>&#x2713;</em>|`as<unsigned long>`
class basic_json|as_longlong() const|<em>&#x2713;</em>|`as<long long>`
class basic_json|as_ulonglong() const|<em>&#x2713;</em>|`as<unsigned long long>`
class basic_json|is_longlong() const|<em>&#x2713;</em>|is<long long>()
class basic_json|is_ulonglong() const|<em>&#x2713;</em>|is<unsigned long long>()
class basic_json|is_numeric() const|<em>&#x2713;</em>|`is_number()`
class basic_json|remove_member(const std::string& name)|<em>&#x2713;</em>|erase(const string_view_type& name)
class basic_json|const json& get(const std::string& name) const|<em>&#x2713;</em>|Use const json get(const std::string& name, T default_val) const with default `json::null_type()`
class basic_json|has_member(const std::string& name) const|<em>&#x2713;</em>|Use `contains(const string_view_type& name)`
class basic_json|has_key(const std::string& name) const|<em>&#x2713;</em>|Use `contains(const string_view_type& name)`
class basic_json|add|<em>&#x2713;</em>|`push_back`
class basic_json|set|<em>&#x2713;</em>|`insert_or_assign`
class basic_json|members()|<em>&#x2713;</em>|object_range()
class basic_json|elements()|<em>&#x2713;</em>|array_range()
class basic_json|begin_members()|<em>&#x2713;</em>|Use object_range().begin()
class basic_json|end_members()|<em>&#x2713;</em>|Use object_range().end()
class basic_json|begin_elements()|<em>&#x2713;</em>|Use array_range().begin()
class basic_json|end_elements()|<em>&#x2713;</em>|Use array_range().end()
class basic_json|is_empty() const|<em>&#x2713;</em>|`empty()`
class basic_json|parse_string(const std::string& s)|<em>&#x2713;</em>|parse(const std::string& s)
class basic_json|parse_string(const std::string& s,parse_error_handler& err_handler)|<em>&#x2713;</em>|Use parse(const std::string& s,parse_error_handler& err_handler)
class basic_json|resize_array(size_t n)|<em>&#x2713;</em>|resize(size_t n)
class basic_json|resize_array(size_t n, const json& val)|<em>&#x2713;</em>|resize(size_t n, const json& val)
class basic_json|to_stream|<em>&#x2713;</em>|Use dump
class basic_json|write|<em>&#x2713;</em>|Use dump
class basic_json|`json` initializer-list constructor||Construct from `json::array` with initializer-list
class basic_json_deserializer|json_deserializer|<em>&#x2713;</em>|Use json_decoder<json>`
class basic_json_deserializer|wjson_deserializer|<em>&#x2713;</em>|Use `json_decoder<wjson>`
class basic_json_deserializer|ojson_deserializer|<em>&#x2713;</em>|Use `json_decoder<ojson>`
class basic_json_deserializer|wojson_deserializer|<em>&#x2713;</em>|Use `json_decoder<wojson>`
class basic_json|owjson|<em>&#x2713;</em>|wojson`
class basic_json|member_type name()|<em>&#x2713;</em>|key()
class basic_json|rename_name_filter|<em>&#x2713;</em>|rename_object_member_filter`
class basic_json|any||removed
class basic_json|member_type|<em>&#x2713;</em>|key_value_pair_type
class basic_json|kvp_type|<em>&#x2713;</em>|key_value_pair_type
class basic_json|null||Constant removed. Use static member function `json::null()`
class basic_json|an_object||Constant removed. Use the default constructor `json()` instead.
class basic_json|an_array||Constant removed. Use assignment to `json::array()` or `json::make_array()` instead.
class json_decoder|root()|<em>&#x2713;</em>|get_result()
class json_content_handler|begin_json|<em>&#x2713;</em>|Removed
class json_content_handler|end_json|<em>&#x2713;</em>|`flush`
class json_content_handler|begin_document|<em>&#x2713;</em>|Removed
class json_content_handler|end_document|<em>&#x2713;</em>|`flush`
class json_content_handler|do_begin_json||Remove
class json_content_handler|do_end_json||Remove
class json_content_handler|do_begin_document||Remove
class json_content_handler|do_end_document||Remove
class output_format|`output_format`|<em>&#x2713;</em>|`json_serializing_options`
class serialization_options|`serialization_options`|<em>&#x2713;</em>|Use `json_serializing_options`
class json_reader|max_depth(),max_depth(value)|<em>&#x2713;</em>|Use `json_serializing_options::max_nesting_depth`
class json_reader|max_nesting_depth(),max_nesting_depth(value)|<em>&#x2713;</em>|Use `json_serializing_options::max_nesting_depth`
class json_reader|json_input_handler& parent()|<em>&#x2713;</em>|Use json_input_handler& input_handler()
json_input_handler class|do_longlong_value(long long value, const parsing_context& context)||Override do_integer_value(int64_t value, const parsing_context& context)
class json_reader|do_ulonglong_value(unsigned long long value, const parsing_context& context)||Removed, override do_uinteger_value(uint64_t value, const parsing_context& context)
class json_reader|do_double_value(double value, const basic_parsing_context<CharT>& context)||Removed, override do_double_value(double value, uint8_t precision, const basic_parsing_context<CharT>& context)
class json_reader|`value(value,context)`|&#160;|Use `string_value(value,context)`, `integer_value(value,context)`, `uinteger_value(value,context)`, `double_value(value,precision,context)`, `bool_value(value,context)`, `null_value(context)`
class json_output_handler class|do_longlong_value(long long value)||Removed, override do_integer_value(int64_t value)
&#160;|do_ulonglong_value(unsigned long long value)||Removed, override do_uinteger_value(uint64_t value)
&#160;|do_double_value(double value)||Removed, override do_double_value(double value, uint8_t precision)
&#160;|`value(value)`|<em>&#x2713;</em>|Use `string_value(value)`, `integer_value(value)`, `uinteger_value(value)`, `double_value(value,precision=0)`, `bool_value(value)`, `null_value(context)`
basic_parsing_context|last_char()|<em>&#x2713;</em>|Use current_char()
json_filter|parent()|<em>&#x2713;</em>|Use downstream_handler()
&#160;|input_handler()|<em>&#x2713;</em>|Use downstream_handler()
file `csv_parameters.hpp`|&#160;||Use `csv_options.hpp`
file `csv_serializing_options.hpp`|&#160;||Use `csv_serializing_options.hpp`
class `csv_parameters`|`csv_parameters`|&#160;|Use `csv_serializing_options`
class `csv_serializing_options`|`csv_parameters`|&#160;|Use `csv_options`
class `csv_options`|`header(std::string value)`|&#160;|Use `column_names(const std::string& value)`
class `csv_options`|`column_names(std::vector<std::string>> value)`|<em>&#x2713;</em>|Use `column_names(const std::string& value)`
class `csv_options`|`data_types(std::string value)`||Use `column_types(const std::string& value)`
class `csv_options`|`column_types(std::vector<std::string>> value)`|<em>&#x2713;</em>|Use `column_types(const std::string& value)`
class `csv_options`|`column_defaults(std::vector<std::string>> value)`|<em>&#x2713;</em>|Use `column_defaults(const std::string& value)`
file `output_format.hpp`|&#160;|&#160;|Use `json_serializing_options.hpp`
file `json_serializing_options`.hpp|&#160;|&#160;|Use `json_options.hpp`
class json_options|`array_array_block_option accessor and modifier` accessor and modifier|&#160;|Use `array_array_line_splits` accessor and modifier
class json_options|`array_object_block_option accessor and modifier`|&#160;|Use `array_object_line_splits` accessor and modifier
class json_options|`object_array_block_option accessor and modifier`|&#160;|Use `object_array_line_splits` accessor and modifier
class json_options|`object_object_line_splits accessor and modifier`|&#160;|Use `object_object_line_splits` accessor and modifier
class json_options|`array_array_line_splits accessor and modifier` accessor and modifier|<em>&#x2713;</em>|Use `array_array_line_splits` accessor and modifier
class json_options|`array_object_line_splits accessor and modifier`|<em>&#x2713;</em>|Use `array_object_line_splits` accessor and modifier
class json_options|`object_array_line_splits accessor and modifier`|<em>&#x2713;</em>|Use `object_array_line_splits` accessor and modifier
class json_options|`object_object_line_splits accessor and modifier`|<em>&#x2713;</em>|Use `object_object_line_splits` accessor and modifier
msgpack|`jsoncons_ext/msgpack/message_pack.hpp` header file|<em>&#x2713;</em>|Use `jsoncons_ext/msgpack/msgpack.hpp`
&#160;|`encode_message_pack`|<em>&#x2713;</em>|Use `encode_msgpack`
&#160;|`decode_message_pack`|<em>&#x2713;</em>|Use `decode_msgpack`

