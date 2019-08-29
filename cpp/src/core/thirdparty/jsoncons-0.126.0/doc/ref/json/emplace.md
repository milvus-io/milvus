### jsoncons::json::emplace

```c++
template <class... Args>
array_iterator emplace(Args&&... args);

template <class... Args>
array_iterator emplace(const_array_iterator pos, Args&&... args);
```

Constructs a new json element at the specified position of a json array, shifting all elements currently at or above that position to the right.

#### Parameters

    pos
Iterator that identifies the position in the array to construct the new json value

    args
Arguments to forward to the constructor of the json value

#### Return value

Array iterator pointing to the emplaced value.

#### Exceptions

Throws `std::runtime_error` if not a json array.

### Example

```c++
json a = json::array();
a.emplace_back("Toronto");
a.emplace_back("Vancouver");
a.emplace(a.array_range().begin(),"Montreal");

std::cout << a << std::endl;
```
Output:

```json
["Montreal","Toronto","Vancouver"]
```

