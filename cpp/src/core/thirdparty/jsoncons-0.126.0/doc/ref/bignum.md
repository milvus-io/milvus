### jsoncons::bignum

```c++
typedef basic_bignum<Allocator = std::allocator<uint8_t>> bignum;
```
The `bignum` class is an instantiation of the `basic_bignum` class template that uses `std::allocator<uint8_t>` as the allocator type.

An arbitrary-precision integer.

#### Header
```c++
#include <jsoncons/bignum.hpp>
```

#### Constructor

    bignum();

    explicit bignum(const Allocator& alloc);

    explicit bignum(const char* str);
Constructs a bignum from the decimal string representation of a bignum. 

    explicit bignum(const char* data, size_t length);
Constructs a bignum from the decimal string representation of a bignum. 

    explicit bignum(const char* str, const Allocator& alloc);

    bignum(int signum, std::initializer_list<uint8_t> magnitude);
Constructs a bignum from the sign-magnitude representation. 
The magnitude is an unsigned integer `n` encoded as a byte string data item in big-endian byte-order.
If the value of signum is 1, the value of the bignum is `n`. 
If the value of signum is -1, the value of the bignum is `-1 - n`. 
An empty list means a zero value.

    bignum(int signum, std::initializer_list<uint8_t> magnitude, const Allocator& alloc);

    bignum(const bignum& s); 

    bignum(bignum&& s); 

#### Assignment

    bignum& operator=(const bignum& s);

    bignum& operator=(bignum&& s);

#### Accessors

    template <typename Ch, typename Traits, typename Alloc>
    void dump(std::basic_string<Ch,Traits,Alloc>& data) const

    template <typename Alloc>
    void dump(int& signum, std::vector<uint8_t,Alloc>& data) const

#### Arithmetic operators

    explicit operator bool() const

    explicit operator int64_t() const

    explicit operator uint64_t() const

    explicit operator double() const

    explicit operator long double() const

    bignum operator-() const

    bignum& operator+=( const bignum& y )

    bignum& operator-=( const bignum& y )

    bignum& operator*=( int64_t y )

    bignum& operator*=( uint64_t y )

    bignum& operator*=( bignum y )

    bignum& operator/=( const bignum& divisor )

    bignum& operator%=( const bignum& divisor )

    bignum& operator<<=( uint64_t k )

    bignum& operator>>=(uint64_t k)

    bignum& operator++()

    bignum operator++(int)

    bignum& operator--()

    bignum operator--(int)

    bignum& operator|=( const bignum& a )

    bignum& operator^=( const bignum& a )

    bignum& operator&=( const bignum& a )

#### Non-member functions

    bool operator==(const bignum& lhs, const bignum& rhs);

    bool operator!=(const bignum& lhs, const bignum& rhs);

    template <class CharT>
    friend std::basic_ostream<CharT>& operator<<(std::basic_ostream<CharT>& os, const bignum& o);

#### Global arithmetic operators

    bool operator==( const bignum& x, const bignum& y )

    bool operator==( const bignum& x, int y )

    bool operator!=( const bignum& x, const bignum& y )

    bool operator!=( const bignum& x, int y )

    bool operator<( const bignum& x, const bignum& y )

    bool operator<( const bignum& x, int64_t y )

    bool operator>( const bignum& x, const bignum& y )

    bool operator>( const bignum& x, int y )

    bool operator<=( const bignum& x, const bignum& y )

    bool operator<=( const bignum& x, int y )

    bool operator>=( const bignum& x, const bignum& y )

    bool operator>=( const bignum& x, int y )

    bignum operator+( bignum x, const bignum& y )

    bignum operator+( bignum x, int64_t y )

    bignum operator-( bignum x, const bignum& y )

    bignum operator-( bignum x, int64_t y )

    bignum operator*( int64_t x, const bignum& y )

    bignum operator*( bignum x, const bignum& y )

    bignum operator*( bignum x, int64_t y )

    bignum operator/( bignum x, const bignum& y )

    bignum operator/( bignum x, int y )

    bignum operator%( bignum x, const bignum& y )

    bignum operator<<( bignum u, unsigned k )

    bignum operator<<( bignum u, int k )

    bignum operator>>( bignum u, unsigned k )

    bignum operator>>( bignum u, int k )

    bignum operator|( bignum x, const bignum& y )

    bignum operator|( bignum x, int y )

    bignum operator|( bignum x, unsigned y )

    bignum operator^( bignum x, const bignum& y )

    bignum operator^( bignum x, int y )

    bignum operator^( bignum x, unsigned y )

    bignum operator&( bignum x, const bignum& y )

    bignum operator&( bignum x, int y )

    bignum operator&( bignum x, unsigned y )

    bignum abs( const bignum& a )

    bignum power( bignum x, unsigned n )

    bignum sqrt( const bignum& a )

### Examples


### Examples

#### Initializing with bignum

```c++
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::string s = "-18446744073709551617";

    json j(bignum(s.c_str()));

    std::cout << "(1) " << j.as<bignum>() << "\n\n";

    std::cout << "(2) " << j.as<std::string>() << "\n\n";

    std::cout << "(3) ";
    j.dump(std::cout);
    std::cout << "\n\n";

    std::cout << "(4) ";
    json_options options1;
    options1.bigint_format(bigint_chars_format::number);
    j.dump(std::cout, options1);
    std::cout << "\n\n";

    std::cout << "(5) ";
    json_options options2;
    options2.bigint_format(bigint_chars_format::base64url);
    j.dump(std::cout, options2);
    std::cout << "\n\n";
}
```
Output:
```
(1) -18446744073709551617

(2) -18446744073709551617

(3) "-18446744073709551617"

(4) -18446744073709551617

(5) "~AQAAAAAAAAAB"
```

#### Integer overflow during parsing

```c++
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    std::string s = "-18446744073709551617";

    json j = json::parse(s);

    std::cout << "(1) ";
    j.dump(std::cout);
    std::cout << "\n\n";

    std::cout << "(2) ";
    json_options options1;
    options1.bigint_format(bigint_chars_format::number);
    j.dump(std::cout, options1);
    std::cout << "\n\n";

    std::cout << "(3) ";
    json_options options2;
    options2.bigint_format(bigint_chars_format::base64url);
    j.dump(std::cout, options2);
    std::cout << "\n\n";
}
```
Output:
```
(1) "-18446744073709551617"

(2) -18446744073709551617

(3) "~AQAAAAAAAAAB"
```

