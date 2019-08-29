### jsoncons::json::is

```c++
template <class T, class... Args>
bool is(Args&&... args) const noexcept; // (1)

bool is_null() const noexcept; // (2)

bool is_string() const noexcept; // (3)

bool is_int64() const noexcept; // (4)

bool is_uint64() const noexcept; // (5)

bool is_double() const noexcept; // (6)

bool is_number() const noexcept; // (7)

bool is_bool() const noexcept; // (8)

bool is_byte_string() const; // (9)

bool is_bignum() const; // (10)

bool is_array() const noexcept; // (11)

bool is_object() const noexcept; // (12)
```

(1) Generic `is` equivalent to type `T`. Returns `true` if the json value is the same as type `T` according to [json_type_traits](../json_type_traits.md), `false` otherwise.  

    bool is<X<T>> const noexcept
If the type `X` is not `std::basic_string` but otherwise satisfies [SequenceContainer](http://en.cppreference.com/w/cpp/concept/SequenceContainer), `is<X<T>>()` returns `true` if the json value is an array and each element is the "same as" type `T` according to [json_type_traits](json_type_traits.md), `false` otherwise.
    bool is<X<std::string,T>> const noexcept
If the type 'X' satisfies [AssociativeContainer](http://en.cppreference.com/w/cpp/concept/AssociativeContainer) or [UnorderedAssociativeContainer](http://en.cppreference.com/w/cpp/concept/UnorderedAssociativeContainer), `is<X<T>>()` returns `true` if the json value is an object and each mapped value is the "same as" `T` according to [json_type_traits](json_type_traits.md), `false` otherwise.

(2) Same as `is<jsoncons::null_type>()`.  
Returns `true` if the json value is null, `false` otherwise.  

(3) Same as `is<std::string>()`.  
Returns `true` if the json value is of string type, `false` otherwise.  

(4) Same as `is<int64_t>()`.  
Returns `true` if the json value is integral and within the range of `int64_t`, `false` otherwise.  

(5) Same as `is<uint64_t>()`.  
Returns `true` if the json value is integral and within the range of `uint64_t`, `false` otherwise.  

(6) Same as `is<double>()`.  
Returns `true` if the json value is floating point and within the range of `double`, `false` otherwise.  

(7) Same as `is<int64_t>() || is<uint64_t>() || is<double>()`.

(8) Same as `is<bool>()`.  
Returns `true` if the json value is of boolean type, `false` otherwise.  

(10) Same as `is<jsoncons::bignum>()`.  
Returns `true` if `is<int64_t>() || is<uint64_t>() is `true`, or if
`is<std::string>()` is `true` and the string holds an integer value,
otherwise `false`.

(11) Same as `is<json::array>()`.  
Returns `true` if the json value is an array, `false` otherwise.  

(12) Same as `is<json::object>()`.  
Returns `true` if the json value is an object, `false` otherwise.  

### Examples

```c++
json j = json::parse(R"(
{
    "k1" : 2147483647,
    "k2" : 2147483648,
    "k3" : -10,
    "k4" : 10.5,
    "k5" : true,
    "k6" : "10.5"
}
)");

std::cout << std::boolalpha << "(1) " << j["k1"].is<int32_t>() << '\n';
std::cout << std::boolalpha << "(2) " << j["k2"].is<int32_t>() << '\n';
std::cout << std::boolalpha << "(3) " << j["k2"].is<long long>() << '\n';
std::cout << std::boolalpha << "(4) " << j["k3"].is<signed char>() << '\n';
std::cout << std::boolalpha << "(5) " << j["k3"].is<uint32_t>() << '\n';
std::cout << std::boolalpha << "(6) " << j["k4"].is<int32_t>() << '\n';
std::cout << std::boolalpha << "(7) " << j["k4"].is<double>() << '\n';
std::cout << std::boolalpha << "(8) " << j["k5"].is<int>() << '\n';
std::cout << std::boolalpha << "(9) " << j["k5"].is<bool>() << '\n';
std::cout << std::boolalpha << "(10) " << j["k6"].is<double>() << '\n';

```
Output:
```
(1) true
(2) false
(3) true
(4) true
(5) false
(6) false
(7) true
(8) false
(9) true
(10) false


