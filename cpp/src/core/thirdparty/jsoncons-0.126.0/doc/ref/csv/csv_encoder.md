### jsoncons::csv::basic_csv_encoder

```c++
template<
    class CharT,
    class Result
    class Allocator=std::allocator<CharT>=std::allocator<CharT>>
> class basic_csv_encoder : public jsoncons::basic_json_content_handler<CharT>
```

`basic_csv_encoder` and `basic_json_compressed_encoder` are noncopyable and nonmoveable.

#### Header

    #include <jsoncons_ext/csv/csv_encoder.hpp>

![csv_encoder](./diagrams/csv_encoder.png)

Four specializations for common character types and result types are defined:

Type                       |Definition
---------------------------|------------------------------
csv_encoder            |basic_csv_encoder<char,jsoncons::stream_result<char>>
json_string_encoder     |basic_csv_encoder<char,jsoncons::string_result<std::string>>
wcsv_encoder           |basic_csv_encoder<wchar_t,jsoncons::stream_result<wchar_t>>
wjson_string_encoder    |basic_csv_encoder<wchar_t,jsoncons::string_result<std::wstring>>

#### Member types

Type                       |Definition
---------------------------|------------------------------
char_type                  |CharT
result_type                |Result
string_view_type           |

#### Constructors

    explicit basic_csv_encoder(result_type result)
Constructs a new csv encoder that is associated with the output adaptor `result`.

    basic_csv_encoder(result_type result, 
                         const basic_csv_options<CharT>& options)
Constructs a new csv encoder that is associated with the output adaptor `result` 
and uses the specified [csv options](csv_options.md). 

#### Destructor

    virtual ~basic_csv_encoder()

### Inherited from [basic_json_content_handler](../json_content_handler.md)

#### Member functions

    bool begin_object(semantic_tag tag=semantic_tag::none,
                      const ser_context& context=null_ser_context()); 

    bool begin_object(size_t length, 
                      semantic_tag tag=semantic_tag::none,
                      const ser_context& context=null_ser_context()); 

    bool end_object(const ser_context& context = null_ser_context())

    bool begin_array(semantic_tag tag=semantic_tag::none,
                     const ser_context& context=null_ser_context()); 

    bool begin_array(semantic_tag tag=semantic_tag::none,
                     const ser_context& context=null_ser_context()); 

    bool end_array(const ser_context& context=null_ser_context()); 

    bool name(const string_view_type& name, 
              const ser_context& context=null_ser_context()); 

    bool string_value(const string_view_type& value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context());

    bool byte_string_value(const byte_string_view& b, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context()); 

    bool byte_string_value(const uint8_t* p, size_t size, 
                           semantic_tag tag=semantic_tag::none, 
                           const ser_context& context=null_ser_context()); 

    bool int64_value(int64_t value, 
                     semantic_tag tag = semantic_tag::none, 
                     const ser_context& context=null_ser_context());

    bool uint64_value(uint64_t value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context()); 

    bool double_value(double value, 
                      semantic_tag tag = semantic_tag::none, 
                      const ser_context& context=null_ser_context()); 

    bool bool_value(bool value, 
                    semantic_tag tag = semantic_tag::none,
                    const ser_context& context=null_ser_context());  

    bool null_value(semantic_tag tag = semantic_tag::none,
                    const ser_context& context=null_ser_context());  

    void flush()

### Examples

### Serializing an array of json values to a comma delimted file

#### JSON input file 
```json
[
    ["country_code","name"],
    ["ABW","ARUBA"],
    ["ATF","FRENCH SOUTHERN TERRITORIES, D.R. OF"],
    ["VUT","VANUATU"],
    ["WLF","WALLIS & FUTUNA ISLANDS"]
]
```
Note 

- The third array element has a value that contains a comma, in the CSV file this value will be quoted.

#### Serializing the comma delimited file with csv_encoder
```c++
std::string in_file = "input/countries.json";
std::ifstream is(in_file);

json_decoder<json> decoder;
json_reader reader(is,decoder);
reader.read();
json countries = decoder.get_result();

csv_encoder encoder(std::cout);

countries.dump(encoder);
```
#### Output 
```
country_code,name
ABW,ARUBA
ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
```
### Serializing an array of json objects to a tab delimted file

#### JSON input file
```json
[
    {
        "dept":"sales",
        "employee-name":"Smith, Matthew",
        "employee-no":"00000001",
        "note":"",
        "salary":"150,000.00"
    },
    {
        "dept":"sales",
        "employee-name":"Brown, Sarah",
        "employee-no":"00000002",
        "note":"",
        "salary":"89,000.00"
    },
    {
        "dept":"finance",
        "employee-name":"Oberc, Scott",
        "employee-no":"00000003",
        "salary":"110,000.00"
    },
    {
        "dept":"sales",
        "employee-name":"Scott, Colette",
        "employee-no":"00000004",
        "note":"\"Exemplary\" employee\nDependable, trustworthy",
        "comment":"Team player",
        "salary":"75,000.00"
    }
]
```
Note 

- The names in the first object in the array will be used for the header row of the CSV file
- The fourth object has a `note` member whose value contains escaped quotes and an escaped new line character, in the CSV file, this value will be quoted, and it will contain a new line character and escaped quotes.

#### Dump json value to a tab delimited file
```c++
std::string in_file = "input/employees.json";
std::ifstream is(in_file);

json_decoder<json> decoder;
csv_options options;
options.field_delimiter = '\t';

json_reader reader(is,decoder);
reader.read();
json employees = decoder.get_result();

csv_encoder encoder(std::cout,options);

employees.dump(encoder);
```
#### Tab delimited output file
```
dept    employee-name   employee-no     note    salary
sales   Smith, Matthew  00000001                150,000.00
sales   Brown, Sarah    00000002                89,000.00
finance Oberc, Scott    00000003                110,000.00
sales   Scott, Colette  00000004        ""Exemplary"" employee
Dependable, trustworthy 75,000.00
```

#### Dump json value to csv file

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_encoder.hpp>

using namespace jsoncons;

int main()
{
    const json books = json::parse(R"(
    [
        {
            "title" : "Kafka on the Shore",
            "author" : "Haruki Murakami",
            "price" : 25.17
        },
        {
            "title" : "Women: A Novel",
            "author" : "Charles Bukowski",
            "price" : 12.00
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer"
        }
    ]
    )");

    csv_options options;
    options.column_names("author,title,price");

    csv_encoder encoder(std::cout, options);

    books.dump(encoder);
}
```

Output:

```csv
author,title,price
Haruki Murakami,Kafka on the Shore,25.17
Charles Bukowski,Women: A Novel,12.0
Ivan Passer,Cutter's Way,
```

