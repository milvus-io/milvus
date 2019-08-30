### jsoncons::json::array_range

```c++
range<array_iterator> array_range();
range<const_array_iterator> array_range() const;
```
Returns a "range" that supports a range-based for loop over the elements of a `json` array      
Throws `std::runtime_error` if not an array.

### Examples

#### Range-based for loop

```c++
json j = json::array{"Toronto", "Vancouver", "Montreal"};

for (const auto& val : j.array_range())
{
    std::cout << val.as<std::string>() << std::endl;
}
```
Output:
```json
Toronto
Vancouver 
Montreal
```

#### Array iterator
```c++
json j = json::array{"Toronto", "Vancouver", "Montreal"};

for (auto it = j.array_range().begin(); it != j.array_range().end(); ++it)
{
    std::cout << it->as<std::string>() << std::endl;
}
```
Output:
```json
Toronto
Vancouver 
Montreal
```


