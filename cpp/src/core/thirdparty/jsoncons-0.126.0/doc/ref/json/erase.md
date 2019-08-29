### `jsoncons::json::erase`

```c++
void erase(const_array_iterator pos); // (1)

void erase(const_array_iterator first, const_array_iterator last); // (2)

void erase(const_object_iterator pos); // (3)

void erase(const_object_iterator first, const_object_iterator last); // (4)

void erase(const string_view_type& name); // (5)
```

(1) Remove an element from an array at the specified position.
Throws `std::runtime_error` if not an array.

(2) Remove the elements from an array in the range '[first,last)'.
Throws `std::runtime_error` if not an array.

(3) Remove a member from an object at the specified position.
Throws `std::runtime_error` if not an object.
    
(4) Remove the members from an object in the range '[first,last)'.
Throws `std::runtime_error` if not an object.

(5) Remove a member with the specified name from an object
Throws `std::runtime_error` if not an object.

