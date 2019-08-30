### jsoncons::json::merge_or_update

```c++
void merge_or_update(const json& source); // (1)
void merge_or_update(json&& source); // (2)
void merge_or_update(object_iterator hint, const json& source); // (3)
void merge_or_update(object_iterator hint, json&& source); // (4)
```

Inserts another json object's key-value pairs into a json object, or assigns them if they already exist.

The `merge_or_update` function performs only a one-level-deep shallow merge, not a deep merge of nested objects.

#### Parameters

<table>
  <tr>
    <td><code>source</code></td>
    <td>`json` object value</td> 
  </tr>
</table>

#### Return value

None

#### Exceptions

Throws `std::runtime_error` if source or *this are not json objects.

### Examples

#### Merge or update `json`

```c++
json j = json::parse(R"(
{
    "a" : 1,
    "b" : 2
}
)");

const json source = json::parse(R"(
{
    "a" : 2,
    "c" : 3
}
)");

j1.merge_or_update(source);

std::cout << j << endl;
```
Output:

```json
{"a":2,"b":2,"c":3}
```

