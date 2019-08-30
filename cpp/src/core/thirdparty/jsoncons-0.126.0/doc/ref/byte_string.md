### jsoncons::byte_string

```c++
typedef basic_byte_string<Allocator = std::allocator<uint8_t>> byte_string;
```
The `byte_string` class is an instantiation of the `basic_byte_string` class template that uses `std::allocator<uint8_t>` as the allocator type.

#### Header
```c++
#include <jsoncons/byte_string.hpp>
```

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`const_iterator`|
`iterator`|Same as `const_iterator`
`size_type`|std::size_t

#### Constructor

    byte_string();

    explicit byte_string(const Allocator& alloc);

    byte_string(std::initializer_list<uint8_t> init);

    byte_string(std::initializer_list<uint8_t> init, const Allocator& alloc);

    explicit byte_string(const byte_string_view& v);

    byte_string(const byte_string_view& v, const Allocator& alloc);

    byte_string(const char* s);

    byte_string(const byte_string& s); 

    byte_string(byte_string&& s); 

#### Assignment

    byte_string& operator=(const byte_string& s);

    byte_string& operator=(byte_string&& s);

#### Iterators

    const_iterator begin() const noexcept;

    const_iterator end() const noexcept;

#### Element access

    const uint8_t* data() const;

    uint8_t operator[](size_type pos) const; 

    operator byte_string_view() const noexcept;

#### Capacity

    size_t size() const;

    size_t length() const;

#### Non-member functions

    bool operator==(const byte_string& lhs, const byte_string& rhs);

    bool operator!=(const byte_string& lhs, const byte_string& rhs);

    template <class CharT>
    friend std::ostream& operator<<(std::ostream& os, const byte_string& o);

### Examples

#### Byte string from initializer list

```c++
json j(byte_string({'H','e','l','l','o'}));
byte_string bs = j.as<byte_string>();

std::cout << "(1) "<< bs << "\n\n";

std::cout << "(2) ";
for (auto b : bs)
{
    std::cout << (char)b;
}
std::cout << "\n\n";

std::cout << "(3) " << j << std::endl;
```

Output:
```
(1) 0x480x650x6c0x6c0x6f

(2) Hello

(3) "SGVsbG8_"
```

#### Byte string from char array

```c++
json j(byte_string("Hello"));
byte_string bs = j.as<byte_string>();

std::cout << "(1) "<< bs << "\n\n";

std::cout << "(2) ";
for (auto b : bs)
{
    std::cout << (char)b;
}
std::cout << "\n\n";

std::cout << "(3) " << j << std::endl;
```

Output:
```
(1) 0x480x650x6c0x6c0x6f

(2) Hello

(3) "SGVsbG8_"
```
