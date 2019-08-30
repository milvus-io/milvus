# Examples

### Parse and decode

[Parse JSON from a string](#A1)  
[Parse JSON from a file](#A2)  
[Parse numbers without loosing precision](#A8)  
[Validate JSON without incurring parse exceptions](#A3)  
[How to allow comments? How not to?](#A4)  
[Set a maximum nesting depth](#A5)  
[Prevent the alphabetic sort of the outputted JSON, retaining the original insertion order](#A6)  
[Parse a very large JSON file with json_cursor](#A7)  

### Encode

[Encode a json value to a string](#B1)  
[Encode a json value to a stream](#B2)  
[Escape all non-ascii characters](#B3)  
[Replace the representation of NaN, Inf and -Inf when serializing. And when reading in again.](#B4)

### Decode JSON to C++ data structures, encode C++ data structures to JSON

[Convert JSON to/from C++ data structures by specializing json_type_traits](#G1)  
[A simple example using JSONCONS_MEMBER_TRAITS_DECL to generate the json_type_traits](#G2)  
[A polymorphic example using JSONCONS_GETTER_CTOR_TRAITS_DECL to generate the json_type_traits](#G3)  
[Convert JSON numbers to/from boost multiprecision numbers](#G4)

### Construct

[Construct a json object](#C1)  
[Construct a json array](#C2)  
[Insert a new value in an array at a specific position](#C3)  
[Create arrays of arrays of arrays of ...](#C4)  
[Merge two json objects](#C5)  
[Construct a json byte string](#C6)  

### Access

[Use string_view to access the actual memory that's being used to hold a string](#E1)  
[Given a string in a json object that represents a decimal number, assign it to a double](#E2)  
[Retrieve a big integer that's been parsed as a string](#E3)  
[Look up a key, if found, return the value converted to type T, otherwise, return a default value of type T](#E4)  
[Retrieve a value in a hierarchy of JSON objects](#E5)  
[Retrieve a json value as a byte string](#E6)

### Iterate

[Iterate over a json array](#D1)  
[Iterate over a json object](#D2)  

### Search and Replace

[Search for and repace an object member key](#F1)  
[Search for and replace a value](#F2)  

### Parse and decode

<div id="A1"/> 

#### Parse JSON from a string

```
std::string s = R"({"first":1,"second":2,"fourth":3,"fifth":4})";    

json j = json::parse(s);
```

or

```c++
using namespace jsoncons::literals;

json j = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" : "2020-12-30"          
}
)"_json;
```

<div id="A2"/> 

#### Parse JSON from a file

```
std::ifstream is("myfile.json");    

json j = json::parse(is);
```

<div id="A8"/> 

#### Parse numbers without loosing precision

By default, jsoncons parses a number with an exponent or fractional part
into a double precision floating point number. If you wish, you can
keep the number as a string with semantic tagging `bigdec`, 
using the `lossless_number` option. You can then put it into a `float`, 
`double`, a boost multiprecision number, or whatever other type you want. 

```c++
#include <jsoncons/json.hpp>

int main()
{
    std::string s = R"(
    {
        "a" : 12.00,
        "b" : 1.23456789012345678901234567890
    }
    )";

    // Default
    json j = json::parse(s);

    std::cout.precision(15);

    // Access as string
    std::cout << "(1) a: " << j["a"].as<std::string>() << ", b: " << j["b"].as<std::string>() << "\n"; 
    // Access as double
    std::cout << "(2) a: " << j["a"].as<double>() << ", b: " << j["b"].as<double>() << "\n\n"; 

    // Using lossless_number option
    json_options options;
    options.lossless_number(true);

    json j2 = json::parse(s, options);
    // Access as string
    std::cout << "(3) a: " << j2["a"].as<std::string>() << ", b: " << j2["b"].as<std::string>() << "\n";
    // Access as double
    std::cout << "(4) a: " << j2["a"].as<double>() << ", b: " << j2["b"].as<double>() << "\n\n"; 
}
```
Output:
```
(1) a: 12.0, b: 1.2345678901234567
(2) a: 12, b: 1.23456789012346

(3) a: 12.00, b: 1.23456789012345678901234567890
(4) a: 12, b: 1.23456789012346
```

<div id="A3"/> 

#### Validate JSON without incurring parse exceptions
```c++
std::string s = R"(
{
    "StartDate" : "2017-03-01",
    "MaturityDate" "2020-12-30"          
}
)";

json_reader reader(s);

// or,
// std::stringstream is(s);
// json_reader reader(is);

std::error_code ec;
reader.read(ec);

if (ec)
{
    std::cout << ec.message() 
              << " on line " << reader.line()
              << " and column " << reader.column()
              << std::endl;
}
```
Output:
```
Expected name separator ':' on line 4 and column 20
```

<div id="A4"/> 

#### How to allow comments? How not to?

jsoncons, by default, accepts and ignores C-style comments

```c++
std::string s = R"(
{
    // Single line comments
    /*
        Multi line comments 
    */
}
)";

// Default
json j = json::parse(s);
std::cout << "(1) " << j << std::endl;

// Strict
try
{
    strict_parse_error_handler err_handler;
    json j = json::parse(s, err_handler);
}
catch (const ser_error& e)
{
    std::cout << "(2) " << e.what() << std::endl;
}
```
Output:
```
(1) {}
(2) Illegal comment at line 3 and column 10
```

<div id="A5"/> 

#### Set a maximum nesting depth

Like this,
```c++
std::string s = "[[[[[[[[[[[[[[[[[[[[[\"Too deep\"]]]]]]]]]]]]]]]]]]]]]";
try
{
    json_options options;
    options.max_nesting_depth(20);
    json j = json::parse(s, options);
}
catch (const ser_error& e)
{
     std::cout << e.what() << std::endl;
}
```
Output:
```
Maximum JSON depth exceeded at line 1 and column 21
```

<div id="A6"/> 

#### Prevent the alphabetic sort of the outputted JSON, retaining the original insertion order

Use `ojson` instead of `json` (or `wojson` instead of `wjson`) to retain the original insertion order. 

```c++
ojson j = ojson::parse(R"(
{
    "street_number" : "100",
    "street_name" : "Queen St W",
    "city" : "Toronto",
    "country" : "Canada"
}
)");
std::cout << "(1)\n" << pretty_print(j) << std::endl;

// Insert "postal_code" at end
j.insert_or_assign("postal_code", "M5H 2N2");
std::cout << "(2)\n" << pretty_print(j) << std::endl;

// Insert "province" before "country"
auto it = j.find("country");
j.insert_or_assign(it,"province","Ontario");
std::cout << "(3)\n" << pretty_print(j) << std::endl;
```
Output:
```
(1)
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "country": "Canada"
}
(2)
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "country": "Canada",
    "postal_code": "M5H 2N2"
}
(3)
{
    "street_number": "100",
    "street_name": "Queen St W",
    "city": "Toronto",
    "province": "Ontario",
    "country": "Canada",
    "postal_code": "M5H 2N2"
}
```

<div id="A7"/> 

### Parse a very large JSON file with json_cursor


A typical pull parsing application will repeatedly process the `current()` 
event and call `next()` to advance to the next event, until `done()` 
returns `true`.

The example JSON text, `book_catalog.json`, is used by the examples below.

```json
[ 
  { 
      "author" : "Haruki Murakami",
      "title" : "Hard-Boiled Wonderland and the End of the World",
      "isbn" : "0679743464",
      "publisher" : "Vintage",
      "date" : "1993-03-02",
      "price": 18.90
  },
  { 
      "author" : "Graham Greene",
      "title" : "The Comedians",
      "isbn" : "0099478374",
      "publisher" : "Vintage Classics",
      "date" : "2005-09-21",
      "price": 15.74
  }
]
```

#### Reading the JSON stream
```c++
std::ifstream is("book_catalog.json");

json_cursor reader(is);

for (; !reader.done(); reader.next())
{
    const auto& event = reader.current();
    switch (event.event_type())
    {
        case staj_event_type::begin_array:
            std::cout << "begin_array\n";
            break;
        case staj_event_type::end_array:
            std::cout << "end_array\n";
            break;
        case staj_event_type::begin_object:
            std::cout << "begin_object\n";
            break;
        case staj_event_type::end_object:
            std::cout << "end_object\n";
            break;
        case staj_event_type::name:
            // If underlying type is string, can return as string_view
            std::cout << "name: " << event.as<jsoncons::string_view>() << "\n";
            break;
        case staj_event_type::string_value:
            std::cout << "string_value: " << event.as<jsoncons::string_view>() << "\n";
            break;
        case staj_event_type::null_value:
            std::cout << "null_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::bool_value:
            std::cout << "bool_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::int64_value:
            std::cout << "int64_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::uint64_value:
            std::cout << "uint64_value: " << event.as<std::string>() << "\n";
            break;
        case staj_event_type::double_value:
            // Return as string, could also use event.as<double>()
            std::cout << "double_value: " << event.as<std::string>() << "\n";
            break;
        default:
            std::cout << "Unhandled event type\n";
            break;
    }
}
```
Output:
```
begin_array
begin_object
name: author
string_value: Haruki Murakami
name: title
string_value: Hard-Boiled Wonderland and the End of the World
name: isbn
string_value: 0679743464
name: publisher
string_value: Vintage
name: date
string_value: 1993-03-02
name: price
double_value: 18.90
end_object
begin_object
name: author
string_value: Graham Greene
name: title
string_value: The Comedians
name: isbn
string_value: 0099478374
name: publisher
string_value: Vintage Classics
name: date
string_value: 2005-09-21
name: price
double_value: 15.74
end_object
end_array
```

#### Implementing a staj_filter

```c++
// A stream filter to filter out all events except name 
// and restrict name to "author"

class author_filter : public staj_filter
{
    bool accept_next_ = false;
public:
    bool accept(const staj_event& event, const ser_context&) override
    {
        if (event.event_type()  == staj_event_type::name &&
            event.as<jsoncons::string_view>() == "author")
        {
            accept_next_ = true;
            return false;
        }
        else if (accept_next_)
        {
            accept_next_ = false;
            return true;
        }
        else
        {
            accept_next_ = false;
            return false;
        }
    }
};
```

#### Filtering the JSON stream

```c++
std::ifstream is("book_catalog.json");

author_filter filter;
json_cursor reader(is, filter);

for (; !reader.done(); reader.next())
{
    const auto& event = reader.current();
    switch (event.event_type())
    {
        case staj_event_type::string_value:
            std::cout << event.as<jsoncons::string_view>() << "\n";
            break;
    }
}
```
Output:
```
Haruki Murakami
Graham Greene
```

See [json_cursor](doc/ref/json_cursor.md) 

<div id="G0"/>

### Decode JSON to C++ data structures, encode C++ data structures to JSON

<div id="G1"/>

#### Convert JSON to/from C++ data structures by specializing json_type_traits
jsoncons supports conversion between JSON text and C++ data structures. The functions [decode_json](doc/ref/decode_json.md) 
and [encode_json](doc/ref/encode_json.md) convert JSON formatted strings or streams to C++ data structures and back. 
Decode and encode work for all C++ classes that have 
[json_type_traits](https://github.com/danielaparker/jsoncons/blob/master/doc/ref/json_type_traits.md) 
defined. The standard library containers are already supported, and you can specialize `json_type_traits`
for your own types in the `jsoncons` namespace. 

```c++
#include <iostream>
#include <jsoncons/json.hpp>
#include <vector>
#include <string>

namespace ns {
    struct book
    {
        std::string author;
        std::string title;
        double price;
    };
} // namespace ns

namespace jsoncons {

    template<class Json>
    struct json_type_traits<Json, ns::book>
    {
        typedef typename Json::allocator_type allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_object() && j.contains("author") && 
                   j.contains("title") && j.contains("price");
        }
        static ns::book as(const Json& j)
        {
            ns::book val;
            val.author = j.at("author").template as<std::string>();
            val.title = j.at("title").template as<std::string>();
            val.price = j.at("price").template as<double>();
            return val;
        }
        static Json to_json(const ns::book& val, 
                            allocator_type allocator=allocator_type())
        {
            Json j(allocator);
            j.try_emplace("author", val.author);
            j.try_emplace("title", val.title);
            j.try_emplace("price", val.price);
            return j;
        }
    };
} // namespace jsoncons
```

To save typing and enhance readability, the jsoncons library defines macros, 
so we could have written

```c++
JSONCONS_MEMBER_TRAITS_DECL(ns::book, author, title, price)
```

which expands to the code above.

```c++
using namespace jsoncons; // for convenience

int main()
{
    const std::string s = R"(
    [
        {
            "author" : "Haruki Murakami",
            "title" : "Kafka on the Shore",
            "price" : 25.17
        },
        {
            "author" : "Charles Bukowski",
            "title" : "Pulp",
            "price" : 22.48
        }
    ]
    )";

    std::vector<ns::book> book_list = decode_json<std::vector<ns::book>>(s);

    std::cout << "(1)\n";
    for (const auto& item : book_list)
    {
        std::cout << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }

    std::cout << "\n(2)\n";
    encode_json(book_list, std::cout, indenting::indent);
    std::cout << "\n\n";
}
```
Output:
```
(1)
Haruki Murakami, Kafka on the Shore, 25.17
Charles Bukowski, Pulp, 22.48

(2)
[
    {
        "author": "Haruki Murakami",
        "price": 25.17,
        "title": "Kafka on the Shore"
    },
    {
        "author": "Charles Bukowski",
        "price": 22.48,
        "title": "Pulp"
    }
]
```

<div id="G2"/>

#### A simple example using JSONCONS_MEMBER_TRAITS_DECL to generate the json_type_traits 

`JSONCONS_MEMBER_TRAITS_DECL` is a macro that can be used to generate the `json_type_traits` boilerplate
from member data.

```c++
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>

namespace ns {

    struct reputon
    {
        std::string rater;
        std::string assertion;
        std::string rated;
        double rating;

        friend bool operator==(const reputon& lhs, const reputon& rhs)
        {
            return lhs.rater == rhs.rater && lhs.assertion == rhs.assertion && 
                   lhs.rated == rhs.rated && lhs.rating == rhs.rating;
        }

        friend bool operator!=(const reputon& lhs, const reputon& rhs)
        {
            return !(lhs == rhs);
        };
    };

    class reputation_object
    {
        std::string application;
        std::vector<reputon> reputons;

        // Make json_type_traits specializations friends to give accesses to private members
        JSONCONS_TYPE_TRAITS_FRIEND;
    public:
        reputation_object()
        {
        }
        reputation_object(const std::string& application, const std::vector<reputon>& reputons)
            : application(application), reputons(reputons)
        {}

        friend bool operator==(const reputation_object& lhs, const reputation_object& rhs)
        {
            return (lhs.application == rhs.application) && (lhs.reputons == rhs.reputons);
        }

        friend bool operator!=(const reputation_object& lhs, const reputation_object& rhs)
        {
            return !(lhs == rhs);
        };
    };

} // namespace ns

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_MEMBER_TRAITS_DECL(ns::reputon, rater, assertion, rated, rating)
JSONCONS_MEMBER_TRAITS_DECL(ns::reputation_object, application, reputons)

using namespace jsoncons; // for convenience

int main()
{
    ns::reputation_object val("hiking", { ns::reputon{"HikingAsylum.example.com","strong-hiker","Marilyn C",0.90} });

    std::string s;
    encode_json(val, s, indenting::indent);
    std::cout << s << "\n";

    auto val2 = decode_json<ns::reputation_object>(s);

    assert(val2 == val);
}
```
Output:
```
{
    "application": "hiking",
    "reputons": [
        {
            "assertion": "strong-hiker",
            "rated": "Marilyn C",
            "rater": "HikingAsylum.example.com",
            "rating": 0.9
        }
    ]
}
```

<div id="G3"/>

#### A polymorphic example using JSONCONS_GETTER_CTOR_TRAITS_DECL to generate the json_type_traits

`JSONCONS_GETTER_CTOR_TRAITS_DECL` is a macro that can be used to generate the `json_type_traits` boilerplate
from getter functions and a constructor.

```c++
#include <cassert>
#include <iostream>
#include <vector>
#include <jsoncons/json.hpp>

using namespace jsoncons;

namespace ns {

class Employee
{
    std::string firstName_;
    std::string lastName_;
public:
    Employee(const std::string& firstName, const std::string& lastName)
        : firstName_(firstName), lastName_(lastName)
    {
    }
    virtual ~Employee() = default;

    virtual double calculatePay() const = 0;

    const std::string& firstName() const {return firstName_;}
    const std::string& lastName() const {return lastName_;}
};

class HourlyEmployee : public Employee
{
    double wage_;
    unsigned hours_;
public:
    HourlyEmployee(const std::string& firstName, const std::string& lastName, 
                   double wage, unsigned hours)
        : Employee(firstName, lastName), 
          wage_(wage), hours_(hours)
    {
    }
    HourlyEmployee(const HourlyEmployee&) = default;
    HourlyEmployee(HourlyEmployee&&) = default;
    HourlyEmployee& operator=(const HourlyEmployee&) = default;
    HourlyEmployee& operator=(HourlyEmployee&&) = default;

    double wage() const {return wage_;}

    unsigned hours() const {return hours_;}

    double calculatePay() const override
    {
        return wage_*hours_;
    }
};

class CommissionedEmployee : public Employee
{
    double baseSalary_;
    double commission_;
    unsigned sales_;
public:
    CommissionedEmployee(const std::string& firstName, const std::string& lastName, 
                         double baseSalary, double commission, unsigned sales)
        : Employee(firstName, lastName), 
          baseSalary_(baseSalary), commission_(commission), sales_(sales)
    {
    }
    CommissionedEmployee(const CommissionedEmployee&) = default;
    CommissionedEmployee(CommissionedEmployee&&) = default;
    CommissionedEmployee& operator=(const CommissionedEmployee&) = default;
    CommissionedEmployee& operator=(CommissionedEmployee&&) = default;

    double baseSalary() const
    {
        return baseSalary_;
    }

    double commission() const
    {
        return commission_;
    }

    unsigned sales() const
    {
        return sales_;
    }

    double calculatePay() const override
    {
        return baseSalary_ + commission_*sales_;
    }
};
} // ns

JSONCONS_GETTER_CTOR_TRAITS_DECL(ns::HourlyEmployee, firstName, lastName, wage, hours)
JSONCONS_GETTER_CTOR_TRAITS_DECL(ns::CommissionedEmployee, firstName, lastName, baseSalary, commission, sales)

namespace jsoncons {

template<class Json>
struct json_type_traits<Json, std::shared_ptr<ns::Employee>> 
{
    static bool is(const Json& j) noexcept
    { 
        return j.is<ns::HourlyEmployee>() || j.is<ns::CommissionedEmployee>();
    }
    static std::shared_ptr<ns::Employee> as(const Json& j)
    {   
        if (j.at("type").as<std::string>() == "Hourly")
        {
            return std::make_shared<ns::HourlyEmployee>(j.as<ns::HourlyEmployee>());
        }
        else if (j.at("type").as<std::string>() == "Commissioned")
        {
            return std::make_shared<ns::CommissionedEmployee>(j.as<ns::CommissionedEmployee>());
        }
        else
        {
            throw std::runtime_error("Not an employee");
        }
    }
    static Json to_json(const std::shared_ptr<ns::Employee>& ptr)
    {
        if (ns::HourlyEmployee* p = dynamic_cast<ns::HourlyEmployee*>(ptr.get()))
        {
            Json j(*p);
            j.try_emplace("type","Hourly");
            return j;
        }
        else if (ns::CommissionedEmployee* p = dynamic_cast<ns::CommissionedEmployee*>(ptr.get()))
        {
            Json j(*p);
            j.try_emplace("type","Commissioned");
            return j;
        }
        else
        {
            throw std::runtime_error("Not an employee");
        }
    }
};

} // jsoncons

int main()
{
    std::vector<std::shared_ptr<ns::Employee>> v;

    v.push_back(std::make_shared<ns::HourlyEmployee>("John", "Smith", 40.0, 1000));
    v.push_back(std::make_shared<ns::CommissionedEmployee>("Jane", "Doe", 30000, 0.25, 1000));

    json j(v);
    std::cout << pretty_print(j) << "\n\n";

    assert(j[0].is<ns::HourlyEmployee>());
    assert(!j[0].is<ns::CommissionedEmployee>());
    assert(!j[1].is<ns::HourlyEmployee>());
    assert(j[1].is<ns::CommissionedEmployee>());


    for (size_t i = 0; i < j.size(); ++i)
    {
        auto p = j[i].as<std::shared_ptr<ns::Employee>>();
        assert(p->firstName() == v[i]->firstName());
        assert(p->lastName() == v[i]->lastName());
        assert(p->calculatePay() == v[i]->calculatePay());
    }
}
```
Output:
```
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "type": "Hourly",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000,
        "type": "Commissioned"
    }
]
```

<div id="G4"/>

#### Convert JSON numbers to/from boost multiprecision numbers

```
#include <jsoncons/json.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>

namespace jsoncons 
{
    template <class Json, class Backend>
    struct json_type_traits<Json,boost::multiprecision::number<Backend>>
    {
        typedef boost::multiprecision::number<Backend> multiprecision_type;

        static bool is(const Json& val) noexcept
        {
            if (!(val.is_string() && val.semantic_tag() == semantic_tag::bigdec))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        static multiprecision_type as(const Json& val)
        {
            return multiprecision_type(val.template as<std::string>());
        }

        static Json to_json(multiprecision_type val)
        {
            return Json(val.str(), semantic_tag::bigdec);
        }
    };
}

int main()
{
    typedef boost::multiprecision::number<boost::multiprecision::cpp_dec_float_50> multiprecision_type;

    std::string s = "[100000000000000000000000000000000.1234]";
    json_options options;
    options.lossless_number(true);
    json j = json::parse(s, options);

    multiprecision_type x = j[0].as<multiprecision_type>();

    std::cout << "(1) " << std::setprecision(std::numeric_limits<multiprecision_type>::max_digits10)
        << x << "\n";

    json j2 = json::array{x};
    std::cout << "(2) " << j2[0].as<std::string>() << "\n";
}
```
Output:
```
(1) 100000000000000000000000000000000.1234
(2) 100000000000000000000000000000000.1234
```

### Encode

<div id="B1"/>

#### Encode a json value to a string

```
std::string s;

j.dump(s); // compressed

j.dump(s, indenting::indent); // pretty print
```

<div id="B2"/>

#### Encode a json value to a stream

```
j.dump(std::cout); // compressed

j.dump(std::cout, indenting::indent); // pretty print
```
or
```
std::cout << j << std::endl; // compressed

std::cout << pretty_print(j) << std::endl; // pretty print
```

<div id="B3"/>

#### Escape all non-ascii characters

```
json_options options;
options.escape_all_non_ascii(true);

j.dump(std::cout, options); // compressed

j.dump(std::cout, options, indenting::indent); // pretty print
```
or
```
std::cout << print(j, options) << std::endl; // compressed

std::cout << pretty_print(j, options) << std::endl; // pretty print
```

<div id="B4"/>

#### Replace the representation of NaN, Inf and -Inf when serializing. And when reading in again.

Set the serializing options for `nan` and `inf` to distinct string values.

```c++
json j;
j["field1"] = std::sqrt(-1.0);
j["field2"] = 1.79e308 * 1000;
j["field3"] = -1.79e308 * 1000;

json_options options;
options.nan_to_str("NaN")
       .inf_to_str("Inf"); 

std::ostringstream os;
os << pretty_print(j, options);

std::cout << "(1)\n" << os.str() << std::endl;

json j2 = json::parse(os.str(),options);

std::cout << "\n(2) " << j2["field1"].as<double>() << std::endl;
std::cout << "(3) " << j2["field2"].as<double>() << std::endl;
std::cout << "(4) " << j2["field3"].as<double>() << std::endl;

std::cout << "\n(5)\n" << pretty_print(j2,options) << std::endl;
```

Output:
```json
(1)
{
    "field1": "NaN",
    "field2": "Inf",
    "field3": "-Inf"
}

(2) nan
(3) inf
(4) -inf

(5)
{
    "field1": "NaN",
    "field2": "Inf",
    "field3": "-Inf"
}
```

### Construct

<div id="C1"/>

#### Construct a json object

Start with an empty json object and insert some name-value pairs,

```c++
json image_sizing;
image_sizing.insert_or_assign("Resize To Fit",true);  // a boolean 
image_sizing.insert_or_assign("Resize Unit", "pixels");  // a string
image_sizing.insert_or_assign("Resize What", "long_edge");  // a string
image_sizing.insert_or_assign("Dimension 1",9.84);  // a double
image_sizing.insert_or_assign("Dimension 2",json::null());  // a null value
```

or use an object initializer-list,

```c++
json file_settings = json::object{
    {"Image Format", "JPEG"},
    {"Color Space", "sRGB"},
    {"Limit File Size", true},
    {"Limit File Size To", 10000}
};
```

<div id="C2"/>

#### Construct a json array

```c++
json color_spaces = json::array(); // an empty array
color_spaces.push_back("sRGB");
color_spaces.push_back("AdobeRGB");
color_spaces.push_back("ProPhoto RGB");
```

or use an array initializer-list,
```c++
json image_formats = json::array{"JPEG","PSD","TIFF","DNG"};
```

<div id="C3"/>

#### Insert a new value in an array at a specific position

```c++
json cities = json::array(); // an empty array
cities.push_back("Toronto");  
cities.push_back("Vancouver");
// Insert "Montreal" at beginning of array
cities.insert(cities.array_range().begin(),"Montreal");  

std::cout << cities << std::endl;
```
Output:
```
["Montreal","Toronto","Vancouver"]
```

<div id="C4"/>

#### Create arrays of arrays of arrays of ...

Like this:

```c++
json j = json::make_array<3>(4, 3, 2, 0.0);
double val = 1.0;
for (size_t i = 0; i < a.size(); ++i)
{
    for (size_t j = 0; j < j[i].size(); ++j)
    {
        for (size_t k = 0; k < j[i][j].size(); ++k)
        {
            j[i][j][k] = val;
            val += 1.0;
        }
    }
}
std::cout << pretty_print(j) << std::endl;
```
Output:
```json
[
    [
        [1.0,2.0],
        [3.0,4.0],
        [5.0,6.0]
    ],
    [
        [7.0,8.0],
        [9.0,10.0],
        [11.0,12.0]
    ],
    [
        [13.0,14.0],
        [15.0,16.0],
        [17.0,18.0]
    ],
    [
        [19.0,20.0],
        [21.0,22.0],
        [23.0,24.0]
    ]
]
```

<div id="C5"/>

#### Merge two json objects

[json::merge](ref/json/merge.md) inserts another json object's key-value pairs into a json object,
unless they already exist with an equivalent key.

[json::merge_or_update](ref/json/merge_or_update.md) inserts another json object's key-value pairs 
into a json object, or assigns them if they already exist.

The `merge` and `merge_or_update` functions perform only a one-level-deep shallow merge,
not a deep merge of nested objects.

```c++
json another = json::parse(R"(
{
    "a" : "2",
    "c" : [4,5,6]
}
)");

json j = json::parse(R"(
{
    "a" : "1",
    "b" : [1,2,3]
}
)");

j.merge(std::move(another));
std::cout << pretty_print(j) << std::endl;
```
Output:
```json
{
    "a": "1",
    "b": [1,2,3],
    "c": [4,5,6]
}
```

<div id="C6"/>

#### Construct a json byte string

```c++
#include <jsoncons/json.hpp>

namespace jc=jsoncons;

int main()
{
    byte_string bs = {'H','e','l','l','o'};

    // default suggested encoding (base64url)
    json j1(bs);
    std::cout << "(1) "<< j1 << "\n\n";

    // base64 suggested encoding
    json j2(bs, semantic_tag::base64);
    std::cout << "(2) "<< j2 << "\n\n";

    // base16 suggested encoding
    json j3(bs, semantic_tag::base16);
    std::cout << "(3) "<< j3 << "\n\n";
}
```

Output:
```
(1) "SGVsbG8"

(2) "SGVsbG8="

(3) "48656C6C6F"
```

### Iterate

<div id="D1"/>

#### Iterate over a json array

```c++
json j = json::array{1,2,3,4};

for (auto val : j.array_range())
{
    std::cout << val << std::endl;
}
```

<div id="D2"/>

#### Iterate over a json object

```c++
json j = json::object{
    {"author", "Haruki Murakami"},
    {"title", "Kafka on the Shore"},
    {"price", 25.17}
};

for (const auto& member : j.object_range())
{
    std::cout << member.key() << "=" 
              << member.value() << std::endl;
}
```

### Access

<div id="E1"/>

#### Use string_view to access the actual memory that's being used to hold a string

You can use `j.as<jsoncons::string_view>()`, e.g.
```c++
json j = json::parse("\"Hello World\"");
auto sv = j.as<jsoncons::string_view>();
```
`jsoncons::string_view` supports the member functions of `std::string_view`, including `data()` and `size()`. 

If your compiler supports `std::string_view`, you can also use `j.as<std::string_view>()`.

<div id="E2"/>

#### Given a string in a json object that represents a decimal number, assign it to a double

```c++
json j = json::object{
    {"price", "25.17"}
};

double price = j["price"].as<double>();
```

<div id="E3"/>
 
#### Retrieve a big integer that's been parsed as a string

If an integer exceeds the range of an `int64_t` or `uint64_t`, jsoncons parses it as a string 
with semantic tagging `bigint`.

```c++
#include <jsoncons/json.hpp>
#include <iostream>
#include <iomanip>

using jsoncons::json;

int main()
{
    std::string input = "-18446744073709551617";

    json j = json::parse(input);

    // Access as string
    std::string s = j.as<std::string>();
    std::cout << "(1) " << s << "\n\n";

    // Access as double
    double d = j.as<double>();
    std::cout << "(2) " << std::setprecision(17) << d << "\n\n";

    // Access as jsoncons::bignum
    jsoncons::bignum bn = j.as<jsoncons::bignum>();
    std::cout << "(3) " << bn << "\n\n";

    // If your compiler supports extended integral types for which std::numeric_limits is specialized 
#if (defined(__GNUC__) || defined(__clang__)) && (!defined(__STRICT_ANSI__) && defined(_GLIBCXX_USE_INT128))
    __int128 i = j.as<__int128>();
    std::cout << "(4) " << i << "\n\n";
#endif
}
```
Output:
```
(1) -18446744073709551617

(2) -1.8446744073709552e+19

(3) -18446744073709551617

(4) -18446744073709551617
```

<div id="E4"/>

#### Look up a key, if found, return the value converted to type T, otherwise, return a default value of type T.
 
```c++
json j = json::object{
    {"price", "25.17"}
};

double price = j.get_with_default("price", 25.00); // returns 25.17

double sale_price = j.get_with_default("sale_price", 22.0); // returns 22.0
```

<div id="E5"/>
 
#### Retrieve a value in a hierarchy of JSON objects

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

int main()
{
    json j = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
       {
           "rater": "HikingAsylum.example.com",
           "assertion": "strong-hiker",
           "rated": "Marilyn C",
           "rating": 0.90
         }
       ]
    }
    )");

    // Using index or `at` accessors
    std::string result1 = j["reputons"][0]["rated"].as<std::string>();
    std::cout << "(1) " << result1 << std::endl;
    std::string result2 = j.at("reputons").at(0).at("rated").as<std::string>();
    std::cout << "(2) " << result2 << std::endl;

    // Using JSON Pointer
    std::string result3 = jsonpointer::get(j, "/reputons/0/rated").as<std::string>();
    std::cout << "(3) " << result3 << std::endl;

    // Using JSONPath
    json result4 = jsonpath::json_query(j, "$.reputons.0.rated");
    if (result4.size() > 0)
    {
        std::cout << "(4) " << result4[0].as<std::string>() << std::endl;
    }
    json result5 = jsonpath::json_query(j, "$..0.rated");
    if (result5.size() > 0)
    {
        std::cout << "(5) " << result5[0].as<std::string>() << std::endl;
    }
}
```

<div id="E6"/>
 
#### Retrieve a json value as a byte string

```c++
#include <jsoncons/json.hpp>

namespace jc=jsoncons;

int main()
{
    json j;
    j["ByteString"] = byte_string({'H','e','l','l','o'});
    j["EncodedByteString"] = json("SGVsbG8=", semantic_tag::base64);

    std::cout << "(1)\n";
    std::cout << pretty_print(j) << "\n\n";

    // Retrieve a byte string as a jsoncons::byte_string
    byte_string bs1 = j["ByteString"].as<byte_string>();
    std::cout << "(2) " << bs1 << "\n\n";

    // or alternatively as a std::vector<uint8_t>
    std::vector<uint8_t> v = j["ByteString"].as<std::vector<uint8_t>>();

    // Retrieve a byte string from a text string containing base64 character values
    byte_string bs2 = j["EncodedByteString"].as<byte_string>();
    std::cout << "(3) " << bs2 << "\n\n";

    // Retrieve a byte string view  to access the memory that's holding the byte string
    byte_string_view bsv3 = j["ByteString"].as<byte_string_view>();
    std::cout << "(4) " << bsv3 << "\n\n";

    // Can't retrieve a byte string view of a text string 
    try
    {
        byte_string_view bsv4 = j["EncodedByteString"].as<byte_string_view>();
    }
    catch (const std::exception& e)
    {
        std::cout << "(5) "<< e.what() << "\n\n";
    }
}
```
Output:
```
(1)
{
    "ByteString": "SGVsbG8",
    "EncodedByteString": "SGVsbG8="
}

(2) 48656c6c6f

(3) 48656c6c6f

(4) 48656c6c6f

(5) Not a byte string
```

### Search and Replace
 
<div id="F1"/>

#### Search for and repace an object member key

You can rename object members with the built in filter [rename_object_member_filter](ref/rename_object_member_filter.md)

```c++
#include <sstream>
#include <jsoncons/json.hpp>
#include <jsoncons/json_filter.hpp>

using namespace jsoncons;

int main()
{
    std::string s = R"({"first":1,"second":2,"fourth":3,"fifth":4})";    

    json_encoder encoder(std::cout);

    // Filters can be chained
    rename_object_member_filter filter2("fifth", "fourth", encoder);
    rename_object_member_filter filter1("fourth", "third", filter2);

    // A filter can be passed to any function that takes a json_content_handler ...
    std::cout << "(1) ";
    std::istringstream is(s);
    json_reader reader(is, filter1);
    reader.read();
    std::cout << std::endl;

    // or 
    std::cout << "(2) ";
    ojson j = ojson::parse(s);
    j.dump(filter1);
    std::cout << std::endl;
}
```
Output:
```json
(1) {"first":1,"second":2,"third":3,"fourth":4}
(2) {"first":1,"second":2,"third":3,"fourth":4}
```
 
<div id="F2"/>

#### Search for and replace a value

You can use [json_replace](ref/jsonpath/json_replace.md) in the `jsonpath` extension

```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

using namespace jsoncons;

int main()
{
    json j = json::parse(R"(
        { "store": {
            "book": [ 
              { "category": "reference",
                "author": "Nigel Rees",
                "title": "Sayings of the Century",
                "price": 8.95
              },
              { "category": "fiction",
                "author": "Evelyn Waugh",
                "title": "Sword of Honour",
                "price": 12.99
              },
              { "category": "fiction",
                "author": "Herman Melville",
                "title": "Moby Dick",
                "isbn": "0-553-21311-3",
                "price": 8.99
              }
            ]
          }
        }
    )");

    // Change the price of "Moby Dick" from $8.99 to $10
    jsonpath::json_replace(j,"$.store.book[?(@.isbn == '0-553-21311-3')].price",10.0);
    std::cout << pretty_print(booklist) << std::endl;
}

