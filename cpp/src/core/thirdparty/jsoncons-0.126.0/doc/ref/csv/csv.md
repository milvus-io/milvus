### csv extension

The csv extension implements decode from and encode to the [CSV format](https://www.rfc-editor.org/rfc/rfc4180.txt)
You can either parse into or serialize from a variant-like structure, [basic_json](../json.md), or your own
data structures, using [json_type_traits](../json_type_traits.md).

[decode_csv](decode_csv.md)

[encode_csv](encode_csv.md)

[csv_options](csv_options.md)

[csv_reader](csv_reader.md)

[csv_encoder](csv_encoder.md)

### Examples

#### Decode a CSV source to a basic_json value

Example file (sales.csv)
```csv
customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,0272561313,01001,0.05,431.65
"Jane Doe",false,416-272-2561,55416,0.15,480.70
"Joe Bloggs",false,"4162722561","55416",0.15,300.70
"John Smith",FALSE,NULL,22313-1450,0.15,300.70
```

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>
#include <fstream>

using namespace jsoncons;

int main()
{
    csv::csv_options options;
    options.assume_header(true)
           .mapping(csv::mapping_type::n_objects);

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

#### Convert a CSV source to a C++ data structure that satisfies [json_type_traits](../json_type_traits.md) requirements, and back

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>
#include <iostream>

using namespace jsoncons;

int main()
{
    const std::string input = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

    csv::csv_options ioptions;
    ioptions.header_lines(1)
           .mapping(csv::mapping_type::n_rows);

    typedef std::vector<std::tuple<std::string,double,double,double,double>> table_type;

    table_type table = csv::decode_csv<table_type>(input,ioptions);

    std::cout << "(1)\n";
    for (const auto& row : table)
    {
        std::cout << std::get<0>(row) << "," 
                  << std::get<1>(row) << "," 
                  << std::get<2>(row) << "," 
                  << std::get<3>(row) << "," 
                  << std::get<4>(row) << "\n";
    }
    std::cout << "\n";

    std::string output;

    csv::csv_options ooptions;
    ooptions.column_names("Date,1Y,2Y,3Y,5Y");
    csv::encode_csv<table_type>(table, output, ooptions);

    std::cout << "(2)\n";
    std::cout << output << "\n";
}
```
Output:
```
(1)
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.011
2017-01-08,0.0063,0.0076,0.0084,0.011

(2)
Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
```

