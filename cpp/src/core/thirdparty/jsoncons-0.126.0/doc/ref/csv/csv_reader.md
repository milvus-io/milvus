### jsoncons::csv::csv_reader

```c++
typedef basic_csv_reader<char,stream_source<char>> csv_reader
```

The `csv_reader` class is an instantiation of the `basic_csv_reader` class template that uses `char` as the character type
and `stream_source<char>` as the input source. It reads a [CSV file](http://tools.ietf.org/html/rfc4180) and produces JSON parse events.

`csv_reader` is noncopyable and nonmoveable.

#### Member types

Type                       |Definition
---------------------------|------------------------------
char_type                  |CharT
source_type                |Src

#### Header
```c++
#include <jsoncons_ext/csv/csv_reader.hpp>
```
#### Constructors

    template <class Source>
    basic_csv_reader(Source&& source,
                     basic_json_content_handler<CharT>& handler); // (1)


    template <class Source>
    basic_csv_reader(Source&& source,
                     basic_json_content_handler<CharT>& handler,
                     const basic_csv_options<CharT>& options); // (2)

    template <class Source>
    basic_csv_reader(Source&& source,
                     basic_json_content_handler<CharT>& handler,
                     parse_error_handler& err_handler); // (3)

    template <class Source>
    basic_csv_reader(Source&& source,
                     basic_json_content_handler<CharT>& handler,
                     const basic_csv_decode_options<CharT>& options,
                     parse_error_handler& err_handler); // (4)

(1) Constructs a `csv_reader` that reads from a character sequence or stream `source`
and a [json_content_handler](../json_content_handler.md) that receives
JSON events. Uses default [csv_options](csv_options.md).

(2) Constructs a `csv_reader` that  that reads from a character sequence or stream `source`, a [json_content_handler](../json_content_handler.md) that receives
JSON events, and [csv_options](csv_options.md).

(3) Constructs a `csv_reader` that reads from a character sequence or stream `source`, a [json_content_handler](../json_content_handler.md) that receives
JSON events and the specified [parse_error_handler](../parse_error_handler.md).
Uses default [csv_options](csv_options.md).

(4) Constructs a `csv_reader` that reads from a character sequence or stream `source`, a [json_content_handler](../json_content_handler.md) that receives
JSON events, [csv_options](csv_options.md),
and the specified [parse_error_handler](../parse_error_handler.md).

Note: It is the programmer's responsibility to ensure that `basic_csv_reader` does not outlive any source, 
content handler, and error handler passed in the constuctor, as `basic_csv_reader` holds pointers to but does not own these resources.

#### Parameters

`source` - a value from which a `jsoncons::basic_string_view<char_type>` is constructible, 
or a value from which a `source_type` is constructible. In the case that a `jsoncons::basic_string_view<char_type>` is constructible
from `source`, `source` is dispatched immediately to the parser. Otherwise, the `csv_reader` reads from a `source_type` in chunks. 

#### Member functions

    bool eof() const
Returns `true` when there is no more data to be read from the stream, `false` otherwise

    void read()
Reports JSON related events for JSON objects, arrays, object members and array elements to a [json_content_handler](../json_content_handler.md), such as a [json_decoder](json_decoder.md).
Throws [ser_error](../ser_error.md) if parsing fails.

    size_t buffer_length() const

    void buffer_length(size_t length)

### Examples

#### Reading a comma delimted file into an array of json values

#### Comma delimited input file 
```
country_code,name
ABW,ARUBA
ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
```
Note 

- The first record contains a header line, but we're going to ignore that and read the entire file as an array of arrays.
- The third record has a field value that contains an embedded comma, so it must be quoted.

#### Code
```c++
std::string in_file = "countries.csv";
std::ifstream is(in_file);

json_decoder<json> decoder;

csv_reader reader(is,decoder);
reader.read();
json countries = decoder.get_result();

std::cout << pretty_print(countries) << std::endl;
```
#### Output 
```json
[
    ["country_code","name"],
    ["ABW","ARUBA"],
    ["ATF","FRENCH SOUTHERN TERRITORIES, D.R. OF"],
    ["VUT","VANUATU"],
    ["WLF","WALLIS & FUTUNA ISLANDS"]
]
```
#### Reading a tab delimted file into an array of json objects

#### Tab delimited input file
```
employee-no employee-name   dept    salary  note
00000001    Smith, Matthew  sales   150,000.00      
00000002    Brown, Sarah    sales   89,000.00       
00000003    Oberc, Scott    finance 110,000.00      
00000004    Scott, Colette  sales   75,000.00       """Exemplary"" employee
Dependable, trustworthy"
```
Note 

- The first record is a header line, which will be used to associate data values with names
- The fifth record has a field value that contains embedded quotes and a new line character, so it must be quoted and the embedded quotes escaped.

#### Code
```c++
std::string in_file = "employees.txt";
std::ifstream is(in_file);

json_decoder<json> decoder;
csv_options options;
options.field_delimiter = '\t'
       .assume_header = true;

csv_reader reader(is,decoder,options);
reader.read();
json employees = decoder.get_result();

std::cout << pretty_print(employees) << std::endl;
```

#### Output

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
        "note":"",
        "salary":"110,000.00"
    },
    {
        "dept":"sales",
        "employee-name":"Scott, Colette",
        "employee-no":"00000004",
        "note":"\"Exemplary\" employee\nDependable, trustworthy",
        "salary":"75,000.00"
    }
]
```

#### Reading the comma delimited file as an array of objects with user supplied columns names

Note 

- The first record contains a header line, but we're going to ignore that and use our own names for the fields.

#### Code
```c++
std::string in_file = "countries.csv";
std::ifstream is(in_file);

json_decoder<json> decoder;

csv_options options;
options.column_names("Country Code,Name")
      .header_lines(1);

csv_reader reader(is,decoder,options);
reader.read();
json countries = decoder.get_result();

std::cout << pretty_print(countries) << std::endl;
```

#### Output 
```json
[
    {
        "Country Code":"ABW",
        "Name":"ARUBA"
    },
    {
        "Country Code":"ATF",
        "Name":"FRENCH SOUTHERN TERRITORIES, D.R. OF"
    },
    {
        "Country Code":"VUT",
        "Name":"VANUATU"
    },
    {
        "Country Code":"WLF",
        "Name":"WALLIS & FUTUNA ISLANDS"
    }
]
```


#### Reading a comma delimited file with different mapping options

#### Input

```csv
Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
```

#### Code

```c++
json_decoder<ojson> decoder;
csv_options options;
options.assume_header(true)
       .column_types("string,float,float,float,float");

options.mapping(mapping_type::n_rows);
std::istringstream is1("bond_yields.csv");
csv_reader reader1(is1,decoder,options);
reader1.read();
ojson val1 = decoder.get_result();
std::cout << "\n(1)\n"<< pretty_print(val1) << "\n";

options.mapping(mapping_type::n_objects);
std::istringstream is2("bond_yields.csv");
csv_reader reader2(is2,decoder,options);
reader2.read();
ojson val2 = decoder.get_result();
std::cout << "\n(2)\n"<< pretty_print(val2) << "\n";

options.mapping(mapping_type::m_columns);
std::istringstream is3("bond_yields.csv");
csv_reader reader3(is3, decoder, options);
reader3.read();
ojson val3 = decoder.get_result();
std::cout << "\n(3)\n" << pretty_print(val3) << "\n";
```

#### Output

```json
(1)
[
    ["Date","1Y","2Y","3Y","5Y"],
    ["2017-01-09",0.0062,0.0075,0.0083,0.011],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112]
]

(2)
[
    {
        "Date": "2017-01-09",
        "1Y": 0.0062,
        "2Y": 0.0075,
        "3Y": 0.0083,
        "5Y": 0.011
    },
    {
        "Date": "2017-01-08",
        "1Y": 0.0063,
        "2Y": 0.0076,
        "3Y": 0.0084,
        "5Y": 0.0112
    },
    {
        "Date": "2017-01-08",
        "1Y": 0.0063,
        "2Y": 0.0076,
        "3Y": 0.0084,
        "5Y": 0.0112
    }
]

(3)
{
    "Date": ["2017-01-09","2017-01-08","2017-01-08"],
    "1Y": [0.0062,0.0063,0.0063],
    "2Y": [0.0075,0.0076,0.0076],
    "3Y": [0.0083,0.0084,0.0084],
    "5Y": [0.011,0.0112,0.0112]
}
```

#### Convert CSV to json when last column repeats

```c++
int main()
{
    const std::string bond_yields = R"(Date,Yield
2017-01-09,0.0062,0.0075,0.0083,0.011,0.012
2017-01-08,0.0063,0.0076,0.0084,0.0112,0.013
2017-01-08,0.0063,0.0076,0.0084,0.0112,0.014
)";

    // array of arrays
    json_decoder<ojson> decoder1;
    csv_options options1;
    options1.header_lines(1);
    options1.assume_header(false);
    options1.column_types("string,float*");
    std::istringstream is1(bond_yields);
    csv_reader reader1(is1, decoder1, options1);
    reader1.read();
    ojson val1 = decoder1.get_result();
    std::cout << "\n(1)\n" << pretty_print(val1) << "\n";

    // array of objects
    json_decoder<ojson> decoder2;
    csv_options options2;
    options2.assume_header(true);
    options2.column_types("string,[float*]");
    std::istringstream is2(bond_yields);
    csv_reader reader2(is2, decoder2, options2);
    reader2.read();
    ojson val2 = decoder2.get_result();
    std::cout << "\n(2)\n" << pretty_print(val2) << "\n";
}
```

Output:
```
(1)
[
    ["2017-01-09",0.0062,0.0075,0.0083,0.011,0.012],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112,0.013],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112,0.014]
]

(2)
[
    {
        "Date": "2017-01-09",
        "Yield": [0.0062,0.0075,0.0083,0.011,0.012]
    },
    {
        "Date": "2017-01-08",
        "Yield": [0.0063,0.0076,0.0084,0.0112,0.013]
    },
    {
        "Date": "2017-01-08",
        "Yield": [0.0063,0.0076,0.0084,0.0112,0.014]
    }
]
```

#### Convert CSV to json when last two columns repeat

```c++
const std::string holidays = R"(1,CAD,2,UK,3,EUR,4,US + UK,5,US
38719,2-Jan-2006,40179,1-Jan-2010,38719,2-Jan-2006,38719,2-Jan-2006,39448,1-Jan-2008
38733,16-Jan-2006,40270,2-Apr-2010,38733,16-Jan-2006,38733,16-Jan-2006,39468,21-Jan-2008
)";

    json_decoder<ojson> decoder;
    csv_options options;
    options.column_types("[integer,string]*");

    // Default
    std::istringstream is1(holidays);
    csv_reader reader1(is1, decoder, options);
    reader1.read();
    ojson val1 = decoder.get_result();
    std::cout << pretty_print(val1) << "\n";
```

Output:
```
[
    [
        [1,"CAD"],
        [2,"UK"],
        [3,"EUR"],
        [4,"US + UK"],
        [5,"US"]
    ],
    [
        [38719,"2-Jan-2006"],
        [40179,"1-Jan-2010"],
        [38719,"2-Jan-2006"],
        [38719,"2-Jan-2006"],
        [39448,"1-Jan-2008"]
    ],
    [
        [38733,"16-Jan-2006"],
        [40270,"2-Apr-2010"],
        [38733,"16-Jan-2006"],
        [38733,"16-Jan-2006"],
        [39468,"21-Jan-2008"]
    ]
]
```
