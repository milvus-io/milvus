### `jsoncons::json::json`

```c++
json(); // (1)

json(const allocator_type& allocator); // (2)

json(std::initializer_list<json> list, const allocator_type& allocator); // (3)

json(const json& val); // (4)

json(const json& val, const allocator_type& allocator); // (5)

json(json&& val) noexcept; // (6)

json(json&& val, const allocator_type& allocator) noexcept; // (7)

json(const array& val); // (8)

json(array&& val) noexcept; // (9)

json(const object& val); // (10)

json(object&& val) noexcept; // (11)

template <class T>
json(const T& val); // (12)

template <class T>
json(const T& val, const allocator_type& allocator); // (13)

json(const char* val); // (14)

json(const char* val, const allocator_type& allocator); // (15)

json(const byte_string_view& bs,
     semantic_tag tag = semantic_tag::none); // (16)

json(const byte_string_view& bs, 
     semantic_tag tag = semantic_tag::none, 
     const allocator_type& allocator); // (17)

json(const bignum& n); // (18)

json(const bignum& n, const allocator_type& allocator); // (19)
```

(1) Constructs a `json` value that holds an empty json object. 

(2) Constructs a `json` value that holds a json object. 

(3) Constructs a `json` array with the elements of the initializer-list `init`. 

(4) Constructs a copy of val

(5) Copy with allocator

(6) Acquires the contents of val, leaving val a `null` value

(7) Move with allocator

(8) Constructs a `json` value from a json array

(9) Acquires the contents of a json array

(10) Constructs a `json` value from a json object

(11) Acquires the contents of a json object

(12) Constructs a `json` value for types supported in [json_type_traits](json_type_traits.md).

(13) Constructs a `json` value for types supported in [json_type_traits](json_type_traits.md) with allocator.

(14) Constructs a `json` value for a text string.

(15) Constructs a `json` value for a text string with supplied allocator.

(16) Constructs a `json` value for a [byte_string](../byte_string.md).

(17) Constructs a `json` value for a [byte_string](../byte_string.md) with supplied allocator.

(18) Constructs a `json` value for a [bignum](../bignum.md).

(19) Constructs a `json` value for a [bignum](../bignum.md) with supplied allocator.

### Examples

```c++
#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <jsoncons/json.hpp>

using namespace jsoncons;
int main()
{
    json j1; // An empty object
    std::cout << "(1) " << j1 << std::endl;

    json j2 = json::object({{"baz", "qux"}, {"foo", "bar"}}); // An object 
    std::cout << "(2) " << j2 << std::endl;

    json j3 = json::array({"bar", "baz"}); // An array 
    std::cout << "(3) " << j3 << std::endl;
  
    json j4(json::null()); // A null value
    std::cout << "(4) " << j4 << std::endl;
    
    json j5(true); // A boolean value
    std::cout << "(5) " << j5 << std::endl;

    double x = 1.0/7.0;

    json j6(x); // A double value
    std::cout << "(6) " << j6 << std::endl;

    json j7(x,4); // A double value with specified precision
    std::cout << "(7) " << j7 << std::endl;

    json j8("Hello"); // A text string
    std::cout << "(8) " << j8 << std::endl;

    const uint8_t bs[] = {'H','e','l','l','o'};
    json j9(byte_string("Hello")); // A byte string
    std::cout << "(9) " << j9 << std::endl;

    std::vector<int> v = {10,20,30};
    json j10 = v; // From a sequence container
    std::cout << "(10) " << j10 << std::endl;

    std::map<std::string, int> m{ {"one", 1}, {"two", 2}, {"three", 3} };
    json j11 = m; // From an associative container
    std::cout << "(11) " << j11 << std::endl;
}
```

```
(1) {}
(2) {"baz":"qux","foo":"bar"}
(3) ["bar","baz"]
(4) null
(5) true
(6) 0.142857142857143
(7) 0.1429
(8) "Hello"
(9) "SGVsbG8_"
(10) [10,20,30]
(11) {"one":1,"three":3,"two":2}
```
