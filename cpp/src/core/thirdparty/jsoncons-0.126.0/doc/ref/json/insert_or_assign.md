### jsoncons::json::insert_or_assign

```c++
template <class T>
pair<object_iterator, bool> insert_or_assign(const string_view_type& key, T&& value); // (1)

template <class T>
object_iterator insert_or_assign(const_object_iterator hint, const string_view_type& key, 
                                 T&& value); // (2)
```

#### Parameters

    key
The member name used to look up and, if not found, to insert

    hint        
An object iterator that provides a hint where to insert the new json value

    value
Value to insert or assign

#### Return value

(1) returns a pair consisting of first, an iterator to the inserted value 
or the already existing value, 
and second, a bool indicating whether the insertion took place
(true for insertion, false for no insertion.)

(2) returns an iterator to the inserted value 
or the already existing value. 

#### Exceptions

Throws `std::runtime_error` if not a json object.

