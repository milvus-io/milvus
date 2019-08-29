### jsoncons::json::emplace_back
```c++
template<class... Args>
json& emplace_back(Args&&... args);
```

#### Parameters

    args 
Arguments to forward to the constructor of the json value

#### Return value

A reference to the emplaced json value.

#### Exceptions

Throws `std::runtime_error` if not a json array.

### Example

```c++
json arr = json::array();
arr.emplace_back(10);
arr.emplace_back(20);
arr.emplace_back(30);

std::cout << arr << std::endl;
```
Output:

```json
[10,20,30]
```

