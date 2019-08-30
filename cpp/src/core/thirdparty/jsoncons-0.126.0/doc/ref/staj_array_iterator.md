### jsoncons::staj_array_iterator

```c++
template <class T>
using staj_array_iterator = basic_staj_array_iterator<T,char,basic_json<char>>;
```

#### Header
```c++
#include <jsoncons/staj_iterator.hpp>
```

A `staj_array_iterator` is an [InputIterator](https://en.cppreference.com/w/cpp/named_req/InputIterator) that
accesses the individual stream events from a [staj_reader](staj_reader.md) and, provided that when it is constructed
the current stream event has type `staj_event_type::begin_array`, it retrieves the elements of the JSON array
as items of type `T`. If when it is constructed the current stream event does not have type `staj_event_type::begin_array`,
it becomes equal to the default-constructed iterator.

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`char_type`|char
`value_type`|`T`
`difference_type`|`std::ptrdiff_t`
`pointer`|`value_type*`
`reference`|`value_type&`
`iterator_category`|[std::input_iterator_tag](https://en.cppreference.com/w/cpp/iterator/iterator_tags)

#### Constructors

    staj_array_iterator() noexcept; // (1)

    staj_array_iterator(basic_staj_reader<char_type>& reader); // (2)

    staj_array_iterator(basic_staj_reader<char_type>& reader,
                        std::error_code& ec); // (3)

(1) Constructs the end iterator

(2) Constructs a `staj_array_iterator` that refers to the first element of the array
    following the current stream event `begin_array`. If there is no such element,
    returns the end iterator. If a parsing error is encountered, throws a 
    [ser_error](ser_error.md).

(3) Constructs a `staj_array_iterator` that refers to the first member of the array
    following the current stream event `begin_array`. If there is no such element,
    returns the end iterator. If a parsing error is encountered, returns the end iterator 
    and sets `ec`.

#### Member functions

    const T& operator*() const

    const T* operator->() const

    staj_array_iterator& operator++()
    staj_array_iterator& increment(std::error_code& ec)
    staj_array_iterator operator++(int) 
Advances the iterator to the next array element.

#### Non-member functions

    template <class T>
    bool operator==(const staj_array_iterator<T>& a, const staj_array_iterator<T>& b)

    template <class T>
    bool operator!=(const staj_array_iterator<T>& a, const staj_array_iterator<T>& b)

    template <class T>
    staj_array_iterator<T> begin(staj_array_iterator<T> iter) noexcept; // (1)

    template <class T>
    staj_array_iterator<T> end(const staj_array_iterator<T>&) noexcept; // (2)

(1) Returns iter unchanged

(2) Returns a default-constructed `stax_array_iterator`, which serves as an end iterator. The argument is ignored.

The `begin` and `end` non-member functions enable the use of `stax_array_iterators` with range-based for loops.

### Examples

#### Iterate over a JSON array, returning json values  

```c++
const std::string example = R"(
[ 
  { 
      "employeeNo" : "101",
      "name" : "Tommy Cochrane",
      "title" : "Supervisor"
  },
  { 
      "employeeNo" : "102",
      "name" : "Bill Skeleton",
      "title" : "Line manager"
  }
]
)";

int main()
{
    std::istringstream is(example);

    json_cursor reader(is);

    staj_array_iterator<json> it(reader);

    for (const auto& j : it)
    {
        std::cout << pretty_print(j) << "\n";
    }
    std::cout << "\n\n";
}
```
Output:
```
{
    "employeeNo": "101",
    "name": "Tommy Cochrane",
    "title": "Supervisor"
}
{
    "employeeNo": "102",
    "name": "Bill Skeleton",
    "title": "Line manager"
}
```

#### Iterate over the JSON array, returning employee values 

```c++
struct employee
{
    std::string employeeNo;
    std::string name;
    std::string title;
};

namespace jsoncons
{
    template<class Json>
    struct json_type_traits<Json, employee>
    {
    template<class Json>
    struct json_type_traits<Json, employee>
    {
        static bool is(const Json& j) noexcept
        {
            return j.is_object() && j.contains("employeeNo") && j.contains("name") && j.contains("title");
        }
        static employee as(const Json& j)
        {
            employee val;
            val.employeeNo = j["employeeNo"].template as<std::string>();
            val.name = j["name"].template as<std::string>();
            val.title = j["title"].template as<std::string>();
            return val;
        }
        static Json to_json(const employee& val)
        {
            Json j;
            j["employeeNo"] = val.employeeNo;
            j["name"] = val.name;
            j["title"] = val.title;
            return j;
        }
    };
}
      
int main()
{
    std::istringstream is(example);

    json_cursor reader(is);

    staj_array_iterator<employee> it(reader);

    for (const auto& val : it)
    {
        std::cout << val.employeeNo << ", " << val.name << ", " << val.title << "\n";
    }
    std::cout << "\n\n";
}
```
Output:
```
101, Tommy Cochrane, Supervisor
102, Bill Skeleton, Line manager
```

