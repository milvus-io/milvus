### jsoncons::json::as

```c++
template <class T, class... Args>
T as(Args&&... args) const; // (1)

bool as_bool() const; // (2)

template <class T>
T as_integer() const; // (3)

double as_double() const; // (4)

string_view_type as_string_view() const; // (5)

std::string as_string() const; // (6)

byte_string as_byte_string() const; // (7)

bignum as_bignum() const; // (8)
```

(1) Generic get `as` type `T`. Attempts to convert the json value to the template value type using [json_type_traits](../json_type_traits.md).

    std::string as<std::string>() const noexcept
    std::string as<std::string>(const char_allocator& allocator) const noexcept
If value is string, returns value, otherwise returns result of [dump](dump.md).

    as<X<T>>()
If the type `X` is not `std::basic_string` but otherwise satisfies [SequenceContainer](http://en.cppreference.com/w/cpp/concept/SequenceContainer), `as<X<T>>()` returns the `json` value as an `X<T>` if the `json` value is an array and each element is convertible to type `T`, otherwise throws.

    as<X<std::string,T>>()
If the type 'X' satisfies [AssociativeContainer](http://en.cppreference.com/w/cpp/concept/AssociativeContainer) or [UnorderedAssociativeContainer](http://en.cppreference.com/w/cpp/concept/UnorderedAssociativeContainer), `as<X<std::string,T>>()` returns the `json` value as an `X<std::string,T>` if the `json` value is an object and if each member value is convertible to type `T`, otherwise throws.

(2) Same as `as<bool>()`.  
Returns `true` if value is `bool` and `true`, or if value is integral and non-zero, or if value is floating point and non-zero, or if value is string and parsed value evaluates as `true`. 
Returns `false` if value is `bool` and `false`, or if value is integral and zero, or if value is floating point and zero, or if value is string and parsed value evaluates as `false`. 
Otherwise throws `std::runtime_exception`

(3) Same as `as<T>()` for integral type T.  
Returns integer value if value is integral, performs cast if value has double type, returns 1 or 0 if value has bool type, attempts conversion if value is string, otherwise throws.

(4) Same as `as<double>()`.  
Returns value cast to double if value is integral, returns `NaN` if value is `null`, attempts conversion if value is string, otherwise throws.

### Examples

#### Accessing integral, floating point, and boolean values

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

std::cout << "(1) " << j["k1"].as<int32_t>() << '\n';
std::cout << "(2) " << j["k2"].as<int32_t>() << '\n';
std::cout << "(3) " << j["k2"].as<long long>() << '\n';
std::cout << "(4) " << j["k3"].as<signed char>() << '\n';
std::cout << "(5) " << j["k3"].as<uint32_t>() << '\n';
std::cout << "(6) " << j["k4"].as<int32_t>() << '\n';
std::cout << "(7) " << j["k4"].as<double>() << '\n';
std::cout << std::boolalpha << "(8) " << j["k5"].as<int>() << '\n';
std::cout << std::boolalpha << "(9) " << j["k5"].as<bool>() << '\n';
std::cout << "(10) " << j["k6"].as<double>() << '\n';

```
Output:
```

(1) 2147483647
(2) -2147483648
(3) 2147483648
(4) ÷
(5) 4294967286
(6) 10
(7) 10.5
(8) 1
(9) true
(10) 10.5
```

#### Accessing a `json` array value as a `std::vector`
```c++
std::string s = "{\"my-array\" : [1,2,3,4]}";
json val = json::parse(s);
std::vector<int> v = val["my-array"].as<std::vector<int>>();
for (size_t i = 0; i < v.size(); ++i)
{
    if (i > 0)
    {
        std::cout << ",";
    }
    std::cout << v[i]; 
}
std::cout << std::endl;
```
Output:
```
1,2,3,4
```

#### Accessing a `json` byte string as a [byte_string](../byte_string.md)
```c++
json j(byte_string("Hello"));
byte_string bs = j.as<byte_string>();

std::cout << bs << std::endl;
```
Output:
```
0x480x650x6c0x6c0x6f
```

