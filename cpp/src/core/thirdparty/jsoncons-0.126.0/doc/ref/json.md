### jsoncons::json

```c++
typedef basic_json<char,
                   ImplementationPolicy = sorted_policy,
                   Allocator = std::allocator<char>> json
```
The class `json` is an instantiation of the `basic_json` class template that uses `char` as the character type. The order of an object's name/value pairs is not preserved, they are sorted alphabetically by name. If you want to preserve the original insertion order, use [ojson](ojson.md) instead.

The class `json` resembles a union. An instance of `json` holds a data item of one of its alternative types:

- null
- bool
- int64
- uint64
- double
- string
- byte string
- array
- object

The data item may be tagged with a [semantic_tag](semantic_tag.md) that provides additional
information about the data item.

When assigned a new value, the old value is overwritten. The type of the new value may be different from the old value. 

The `jsoncons` library will rebind the supplied allocator from the template parameter to internal data structures.

#### Header
```c++
#include <jsoncons/json.hpp>
```

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`json_type`|json
`reference`|json&
`const_reference`|const json&
`pointer`|json*
`const_pointer`|const json*
`allocator_type`|Allocator type
`char_allocator`|String allocator type
`array_allocator`|Array allocator type
`object_allocator`|Object allocator 
`string_view_type`|A non-owning view of a string, holds a pointer to character data and length. Supports conversion to and from strings. Will be typedefed to the C++ 17 [string view](http://en.cppreference.com/w/cpp/string/basic_string_view) if `JSONCONS_HAS_STRING_VIEW` is defined in `jsoncons_config.hpp`, otherwise proxied.  
`key_value_type`|[key_value_type](json/key_value.md) is a class that stores a name and a json value
`object`|json object type
`array`|json array type
`object_iterator`|A [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to [key_value_type](json/key_value.md)
`const_object_iterator`|A const [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to const [key_value_type](json/key_value.md)
`array_iterator`|A [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to `json`
`const_array_iterator`|A const [RandomAccessIterator](http://en.cppreference.com/w/cpp/concept/RandomAccessIterator) to `const json`

### Static member functions

<table border="0">
  <tr>
    <td><a href="json/parse.md">parse</a></td>
    <td>Parses JSON.</td> 
  </tr>
  <tr>
    <td><a href="json/make_array.md">make_array</a></td>
    <td>Makes a multidimensional json array.</td> 
  </tr>
  <tr>
    <td><a>const json& null()</a></td>
    <td>Returns a null value</td> 
  </tr>
</table>

### Member functions
<table border="0">
  <tr>
    <td><a href="json/constructor.md">(constructor)</a></td>
    <td>constructs the json value</td> 
  </tr>
  <tr>
    <td><a href="json/destructor.md">(destructor)</a></td>
    <td>destructs the json value</td> 
  </tr>
  <tr>
    <td><a href="json/operator=.md">operator=</a></td>
    <td>assigns values</td> 
  </tr>
</table>

    allocator_type get_allocator() const
Returns the allocator associated with the json value.

#### Ranges and Iterators

<table border="0">
  <tr>
    <td><a href="json/array_range.md">array_range</a></td>
    <td>Returns a "range" that supports a range-based for loop over the elements of a `json` array.</td> 
  </tr>
  <tr>
    <td><a href="json/object_range.md">obect_range</a></td>
    <td>Returns a "range" that supports a range-based for loop over the key-value pairs of a `json` object.</td> 
  </tr>
</table>

#### Capacity

<table border="0">
  <tr>
    <td><a>size_t size() const noexcept</a></td>
    <td>Returns the number of elements in a json array, or the number of members in a json object, or `zero`</td> 
  </tr>
  <tr>
    <td><a>bool empty() const noexcept</a></td>
    <td>Returns <code>true</code> if a json string, object or array has no elements, otherwise <code>false</code></td> 
  </tr>
  <tr>
    <td><a>size_t capacity() const</a></td>
    <td>Returns the size of the storage space currently allocated for a json object or array</td> 
  </tr>
  <tr>
    <td><a>void reserve(size_t n)</a></td>
    <td>Increases the capacity of a json object or array to allow at least `n` members or elements</td> 
  </tr>
  <tr>
    <td><a>void resize(size_t n)</a></td>
    <td>Resizes a json array so that it contains `n` elements</td> 
  </tr>
  <tr>
    <td><a>void resize(size_t n, const json& val)</a></td>
    <td>Resizes a json array so that it contains `n` elements that are initialized to `val`</td> 
  </tr>
  <tr>
    <td><a>void shrink_to_fit()</a></td>
    <td>Requests the removal of unused capacity</td> 
  </tr>
</table>

#### Accessors

<table border="0">
  <tr>
    <td><code>bool contains(const string_view_type& key) const</code></td>
    <td>Returns <code>true</code> if an object has a member with the given `key`, otherwise <code>false</code></td> 
  </tr>
  <tr>
    <td><code>bool count(const string_view_type& key) const</code></td>
    <td>Returns the number of object members that match `key`</td> 
  </tr>
  <tr>
    <td><a href="json/is.md">is</a></td>
    <td>Checks if a json value matches a type.</td> 
  </tr>
  <tr>
    <td><a href="json/as.md">as</a></td>
    <td>Attempts to convert a json value to a value of a type.</td> 
  </tr>
</table>

    semantic_tag get_semantic_tag() const
Returns the [semantic_tag](semantic_tag.md) associated with this value

    json& operator[](size_t i)
    const json& operator[](size_t i) const
Returns a reference to the value at position i in a json object or array.
Throws `std::runtime_error` if not an object or array.

    json& operator[](const string_view_type& name)
Returns a proxy to a keyed value. If written to, inserts or updates with the new value. If read, evaluates to a reference to the keyed value, if it exists, otherwise throws. 
Throws `std::runtime_error` if not an object.
If read, throws `std::out_of_range` if the object does not have a member with the specified name.  

    const json& operator[](const string_view_type& name) const
If `name` matches the name of a member in the json object, returns a reference to the json object, otherwise throws.
Throws `std::runtime_error` if not an object.
Throws `std::out_of_range` if the object does not have a member with the specified name.  

    object_iterator find(const string_view_type& name)
    const_object_iterator find(const string_view_type& name) const
Returns an object iterator to a member whose name compares equal to `name`. If there is no such member, returns `end_member()`.
Throws `std::runtime_error` if not an object.

    json& at(const string_view_type& name)
    const json& at(const string_view_type& name) const
Returns a reference to the value with the specifed name in a json object.
Throws `std::runtime_error` if not an object.
Throws `std::out_of_range` if the object does not have a member with the specified name.  

    json& at(size_t i)
    const json& at(size_t i) const
Returns a reference to the element at index `i` in a json array.  
Throws `std::runtime_error` if not an array.
Throws `std::out_of_range` if the index is outside the bounds of the array.  

    template <class T>
    T get_with_default(const string_view_type& name, 
                       const T& default_val) const
If `name` matches the name of a member in the json object, returns the member value converted to the default's data type, otherwise returns `default_val`.
Throws `std::runtime_error` if not an object.

    template <class T = std::string>
    T get_with_default(const string_view_type& name, 
                       const char_type* default_val) const
Make `get_with_default` do the right thing for string literals. 
Throws `std::runtime_error` if not an object. 

#### Modifiers

<table border="0">
  <tr>
    <td><a>void clear()</a></td>
    <td>Remove all elements from an array or members from an object, otherwise do nothing</td> 
  </tr>
  <tr>
    <td><a href="json/erase.md">erase</a></td>
    <td>Erases array elements and object members</td> 
  </tr>
  <tr>
    <td><a href="json/push_back.md">push_back</a></td>
    <td>Adds a value to the end of a json array</td> 
  </tr>
  <tr>
    <td><a href="json/insert.md">insert</a></td>
    <td>Inserts elements</td> 
  </tr>
  <tr>
    <td><a href="json/emplace_back.md">emplace_back</a></td>
    <td>Constructs a value in place at the end of a json array</td> 
  </tr>
  <tr>
    <td><a href="json/emplace.md">emplace</a></td>
    <td>Constructs a value in place before a specified position in a json array</td> 
  </tr>
  <tr>
    <td><a href="json/try_emplace.md">try_emplace</a></td>
    <td>Constructs a key-value pair in place in a json object if the key does not exist, does nothing if the key exists</td> 
  </tr>
  <tr>
    <td><a href="json/insert_or_assign.md">insert_or_assign</a></td>
    <td>Inserts a key-value pair in a json object if the key does not exist, or assigns a new value if the key already exists</td> 
  </tr>
  <tr>
    <td><a href="json/merge.md">merge</a></td>
    <td>Inserts another json object's key-value pairs into a json object, if they don't already exist.</td>
  </tr>
  <tr>
    <td><a href="json/merge_or_update.md">merge_or_update</a></td>
    <td>Inserts another json object's key-value pairs into a json object, or assigns them if they already exist.</td>
  </tr>
  <tr>
    <td><a>void swap(json& val)</a></td>
    <td>Exchanges the content of the <code>json</code> value with the content of <code>val</code>, which is another <code>json</code> value</td>
  </tr>
</table>

#### Serialization

<table border="0">
  <tr>
    <td><a href="json/dump.md"</a>dump</td>
    <td>Serializes json value to a string, stream, or output handler.</td> 
  </tr>
</table>

#### Non member functions

<table border="0">
  <tr>
    <td><code>bool operator==(const json& lhs, const json& rhs)</code></td>
    <td>Returns <code>true</true> if two json objects compare equal, <code>false</true> otherwise.</td> 
  </tr>
  <tr>
    <td><code>bool operator!=(const json& lhs, const json& rhs)</code></td>
    <td>Returns <code>true</true> if two json objects do not compare equal, <code>false</true> otherwise.</td> 
  </tr>
  <tr>
    <td><code>bool operator<(const json& lhs, const json& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator<=(const json& lhs, const json& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator>(const json& lhs, const json& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator>=(const json& lhs, const json& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
</table>

    std::istream& operator>> (std::istream& os, json& val)
Reads a `json` value from a stream.

    std::ostream& operator<< (std::ostream& os, const json& val)
Inserts json value into stream.

    std::ostream& print(const json& val)  
    std::ostream& print(const json& val, const json_options<CharT>& options)  
Inserts json value into stream using the specified [json_options](json_options.md) if supplied.

    std::ostream& pretty_print(const json& val)  
    std::ostream& pretty_print(const json& val, const json_options<CharT>& options)  
Inserts json value into stream using the specified [json_options](json_options.md) if supplied.

    void swap(json& a, json& b)
Exchanges the values of `a` and `b`

#### Deprecated names

As the `jsoncons` library has evolved, names have sometimes changed. To ease transition, jsoncons deprecates the old names but continues to support many of them. See the [deprecated list](deprecated.md) for the status of old names. The deprecated names can be suppressed by defining macro JSONCONS_NO_DEPRECATED, which is recommended for new code.

#### See also

- [ojson](ojson.md) constructs a json value that preserves the original name-value insertion order

- [wjson](wjson.md) constructs a wide character json value that sorts name-value members alphabetically

- [wojson](wojson.md) constructs a wide character json value that preserves the original name-value insertion order

### Examples
  
#### Accessors and defaults
```c++
json val;

val["field1"] = 1;
val["field3"] = "Toronto";

double x1 = obj.contains("field1") ? val["field1"].as<double>() : 10.0;
double x2 = obj.contains("field2") ? val["field2"].as<double>() : 20.0;

std::string x3 = obj.get_with_default("field3","Montreal");
std::string x4 = obj.get_with_default("field4","San Francisco");

std::cout << "x1=" << x1 << '\n';
std::cout << "x2=" << x2 << '\n';
std::cout << "x3=" << x3 << '\n';
std::cout << "x4=" << x4 << '\n';
```
Output:
```c++
x1=1
x2=20
x3=Toronto
x4=San Francisco
```
#### Nulls
```c++
json obj;
obj["field1"] = json::null();
std::cout << obj << std::endl;
```
Output: 
```json
{"field1":null}
```
#### Constructing json structures
```c++
json root;

root["persons"] = json::array();

json person;
person["first_name"] = "John";
person["last_name"] = "Smith";
person["birth_date"] = "1972-01-30";

json address;
address["city"] = "Toronto";
address["country"] = "Canada";

person["address"] = std::move(address);

root["persons"].push_back(std::move(person));

std::cout << pretty_print(root) << std::endl;
```
Output:
```c++
{
    "persons":
    [
        {
            "address":
            {
                "city":"Toronto",
                "country":"Canada"
            },
            "birth_date":"1972-01-30",
            "first_name":"John",
            "last_name":"Smith"
        }
    ]
}
```
