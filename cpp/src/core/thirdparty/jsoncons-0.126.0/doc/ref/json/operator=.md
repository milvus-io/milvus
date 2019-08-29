### `jsoncons::json::operator=`

```c++
json& operator=(const json& rhs);
json& operator=(json&& rhs) noexcept; // (1)

template <class T>
json& operator=(const T& rhs); // (2)

json& operator=(const char_type* rhs); // (3)
```

(1) Assigns a new `json` value to a `json` variable, replacing it's current contents.

(2) Assigns the templated value to a `json` variable using [json_type_traits](json_type_traits.md).

