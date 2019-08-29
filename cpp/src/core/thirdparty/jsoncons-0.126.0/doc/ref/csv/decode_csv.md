### jsoncons::csv::decode_csv

Decodes a [comma-separated variables (CSV)](https://en.wikipedia.org/wiki/Comma-separated_values) data format into a C++ data structure.

#### Header
```c++
#include <jsoncons_ext/csv/csv_reader.hpp>

template <class T,class CharT>
T decode_csv(const std::basic_string<CharT>& s, 
             const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())); // (1)

template <class T,class CharT>
T decode_csv(std::basic_istream<CharT>& is, 
             const basic_csv_options<CharT>& options = basic_csv_options<CharT>::default_options())); // (2)
```

(1) Reads a CSV string value into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

(2) Reads a CSV input stream into a type T if T is an instantiation of [basic_json](../json.md) 
or if T supports [json_type_traits](../json_type_traits.md).

#### Return value

Returns a value of type `T`.

#### Exceptions

Throws [ser_error](ser_error.md) if parsing fails.

### Examples

#### Decode a CSV file with type inference (default)

Example file (sales.csv)
```csv
customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,0272561313,01001,0.05,431.65
"Jane Doe",false,416-272-2561,55416,0.15,480.70
"Joe Bloggs",false,"4162722561","55416",0.15,300.70
"John Smith",FALSE,NULL,22313-1450,0.15,300.70
```

```c++
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>

using namespace jsoncons;

int main()
{
    csv::csv_options options;
    options.assume_header(true);

    options.mapping(csv::mapping_type::n_objects);
    std::ifstream is1("input/sales.csv");
    ojson j1 = csv::decode_csv<ojson>(is1,options);
    std::cout << "\n(1)\n"<< pretty_print(j1) << "\n";

    options.mapping(csv::mapping_type::n_rows);
    std::ifstream is2("input/sales.csv");
    ojson j2 = csv::decode_csv<ojson>(is2,options);
    std::cout << "\n(2)\n"<< pretty_print(j2) << "\n";

    options.mapping(csv::mapping_type::m_columns);
    std::ifstream is3("input/sales.csv");
    ojson j3 = csv::decode_csv<ojson>(is3,options);
    std::cout << "\n(3)\n"<< pretty_print(j3) << "\n";
}
```
Output:
```json
(1)
[
    {
        "customer_name": "John Roe",
        "has_coupon": true,
        "phone_number": "0272561313",
        "zip_code": "01001",
        "sales_tax_rate": 0.05,
        "total_amount": 431.65
    },
    {
        "customer_name": "Jane Doe",
        "has_coupon": false,
        "phone_number": "416-272-2561",
        "zip_code": 55416,
        "sales_tax_rate": 0.15,
        "total_amount": 480.7
    },
    {
        "customer_name": "Joe Bloggs",
        "has_coupon": false,
        "phone_number": "4162722561",
        "zip_code": "55416",
        "sales_tax_rate": 0.15,
        "total_amount": 300.7
    },
    {
        "customer_name": "John Smith",
        "has_coupon": false,
        "phone_number": null,
        "zip_code": "22313-1450",
        "sales_tax_rate": 0.15,
        "total_amount": 300.7
    }
]

(2)
[
    ["customer_name","has_coupon","phone_number","zip_code","sales_tax_rate","total_amount"],
    ["John Roe",true,"0272561313","01001",0.05,431.65],
    ["Jane Doe",false,"416-272-2561",55416,0.15,480.7],
    ["Joe Bloggs",false,"4162722561","55416",0.15,300.7],
    ["John Smith",false,null,"22313-1450",0.15,300.7]
]

(3)
{
    "customer_name": ["John Roe","Jane Doe","Joe Bloggs","John Smith"],
    "has_coupon": [true,false,false,false],
    "phone_number": ["0272561313","416-272-2561",4162722561,null],
    "zip_code": ["01001",55416,55416,"22313-1450"],
    "sales_tax_rate": [0.05,0.15,0.15,0.15],
    "total_amount": [431.65,480.7,300.7,300.7]
}
```

#### Decode a CSV string without type inference

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>

using namespace jsoncons;

int main()
{
    std::string s = R"(employee-no,employee-name,dept,salary
00000001,"Smith,Matthew",sales,150000.00
00000002,"Brown,Sarah",sales,89000.00
)";

    csv::csv_options options;
    options.assume_header(true)
          .infer_types(false);
    ojson j = csv::decode_csv<ojson>(s,options);

    std::cout << pretty_print(j) << std::endl;
}
```
Output:
```json
[
    {
        "employee-no": "00000001",
        "employee-name": "Smith,Matthew",
        "dept": "sales",
        "salary": "150000.00"
    },
    {
        "employee-no": "00000002",
        "employee-name": "Brown,Sarah",
        "dept": "sales",
        "salary": "89000.00"
    }
]
```

#### Decode a CSV string with specified types

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>

using namespace jsoncons;

int main()
{
    const std::string s = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

    csv::csv_options options;
    options.assume_header(true)
          .column_types("string,float,float,float,float");

    // mapping_type::n_objects
    options.mapping(csv::mapping_type::n_objects);
    ojson j1 = csv::decode_csv<ojson>(s,options);
    std::cout << "\n(1)\n"<< pretty_print(j1) << "\n";

    // mapping_type::n_rows
    options.mapping(csv::mapping_type::n_rows);
    ojson j2 = csv::decode_csv<ojson>(s,options);
    std::cout << "\n(2)\n"<< pretty_print(j2) << "\n";

    // mapping_type::m_columns
    options.mapping(csv::mapping_type::m_columns);
    ojson j3 = csv::decode_csv<ojson>(s,options);
    std::cout << "\n(3)\n" << pretty_print(j3) << "\n";
}
```
Output:
```json
(1)
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

(2)
[
    ["Date","1Y","2Y","3Y","5Y"],
    ["2017-01-09",0.0062,0.0075,0.0083,0.011],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112]
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
#### Decode a CSV string with multi-valued fields separated by subfield delimiters

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>

using namespace jsoncons;

int main()
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    json_options print_options;
    print_options.array_array_line_splits(line_split_kind::same_line);

    csv::csv_options options1;
    options1.assume_header(true)
           .subfield_delimiter(';');

    json j1 = csv::decode_csv<json>(s,options1);
    std::cout << "(1)\n" << pretty_print(j1,print_options) << "\n\n";

    csv::csv_options options2;
    options2.mapping(csv::mapping_type::n_rows)
           .subfield_delimiter(';');

    json j2 = csv::decode_csv<json>(s,options2);
    std::cout << "(2)\n" << pretty_print(j2,print_options) << "\n\n";

    csv::csv_options options3;
    options3.assume_header(true)
           .mapping(csv::mapping_type::m_columns)
           .subfield_delimiter(';');

    json j3 = csv::decode_csv<json>(s,options3);
    std::cout << "(3)\n" << pretty_print(j3,print_options) << "\n\n";
}
```
Output:
```json
(1)
[

    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    },
    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    }
]

(2)
[

    ["calculationPeriodCenters","paymentCenters","resetCenters"],
    [["NY","LON"],"TOR","LON"],
    ["NY","LON",["TOR","LON"]],
    [["NY","LON"],"TOR","LON"],
    ["NY","LON",["TOR","LON"]]
]

(3)
{
    "calculationPeriodCenters": [["NY","LON"],"NY",["NY","LON"],"NY"],
    "paymentCenters": ["TOR","LON","TOR","LON"],
    "resetCenters": ["LON",["TOR","LON"],"LON",["TOR","LON"]]
}
```

