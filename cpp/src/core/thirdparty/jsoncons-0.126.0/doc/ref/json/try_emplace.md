### jsoncons::json::try_emplace

```c++
template <class T>
pair<object_iterator, bool> try_emplace(const string_view_type& key, 
                                        Args&&... args); // (1)

template <class T>
object_iterator try_emplace(const_object_iterator hint, 
                            const string_view_type& key, 
                            Args&&... args); // (2)
```

#### Parameters

    key
The key used both to look up and to insert if not found

    hint
Iterator to the position before which the new element will be inserted

    args        
Arguments to forward to the constructor of the element

#### Return value

(1) returns a pair consisting of first, an iterator to the inserted value 
or the already existing value, 
and second, a bool indicating whether the insertion took place
(true for insertion, false for no insertion.)

(2) returns an iterator to the inserted value 
or the already existing value. 

#### Exceptions

Throws `std::runtime_error` if not a json object.

### Example

```c++
json a;

a.try_emplace("object1",json());
a.try_emplace("field1","value1");
a["object1"].try_emplace("field2","value2");

std::cout << a << std::endl;
```
Output:

```json
{"field1":"value1","object1":{"field2":"value2"}}
```



