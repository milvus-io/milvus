### jsoncons::json::key_value 

```c++
template <class KeyT, class ValueT>
class key_value
```

`key_value` stores a key (name) and a json value

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`string_view_type`|A non-owning view of a string, holds a pointer to character data and length. Supports conversion to and from strings. Will be typedefed to the C++ 17 [string view](http://en.cppreference.com/w/cpp/string/basic_string_view) if `JSONCONS_HAS_STRING_VIEW` is defined in `jsoncons_config.hpp`, otherwise proxied.  

#### Accessors
    
    string_view_type key() const

    const json& value() const

    json& value()

#### Non member functions

<table border="0">
  <tr>
    <td><code>bool operator==(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Returns <code>true</true> if two key_value objects compare equal, <code>false</true> otherwise.</td> 
  </tr>
  <tr>
    <td><code>bool operator!=(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Returns <code>true</true> if two key_value objects do not compare equal, <code>false</true> otherwise.</td> 
  </tr>
  <tr>
    <td><code>bool operator<(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator<=(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator>(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
  <tr>
    <td><code>bool operator>=(const key_value& lhs, const key_value& rhs)</code></td>
    <td>Compares the contents of lhs and rhs lexicographically.</td> 
  </tr>
</table>


