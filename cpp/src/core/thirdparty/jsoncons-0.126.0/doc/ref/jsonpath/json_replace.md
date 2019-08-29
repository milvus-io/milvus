### jsoncons::jsonpath::json_replace

Searches for all values that match a JSONPath expression and replaces them with the specified value

#### Header
```c++
#include <jsoncons/jsonpath/json_query.hpp>
```

```c++
template<class Json, class T>
void json_replace(Json& root, 
                  const typename Json::string_view_type& path, 
                  T&& new_value)
```
#### Parameters

<table>
  <tr>
    <td>root</td>
    <td>JSON value</td> 
  </tr>
  <tr>
    <td>path</td>
    <td>JSONPath expression string</td> 
  </tr>
  <tr>
    <td>new_value</td>
    <td>The value to use as replacement</td> 
  </tr>
</table>

#### Exceptions

Throws [jsonpath_error](jsonpath_error.md) if JSONPath evaluation fails.

### Examples

#### Change the price of a book

Example file (booklist.json):
```json
{ "store": {
    "book": [ 
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      },
      { "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
      }
    ]
  }
}
```
```c++
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

using namespace jsoncons;
using namespace jsoncons::jsonpath;

int main()
{
    std::ifstream is("input/booklist.json");
    json booklist;
    is >> booklist;

    // Change the price of "Moby Dick"
    json_replace(booklist,"$.store.book[?(@.isbn == '0-553-21311-3')].price",10.0);
    std::cout << pretty_print(booklist) << std::endl;

}
```
Output:
```json
{
    "store": {
        "book": [
            {
                "author": "Nigel Rees",
                "category": "reference",
                "price": 8.95,
                "title": "Sayings of the Century"
            },
            {
                "author": "Evelyn Waugh",
                "category": "fiction",
                "price": 12.99,
                "title": "Sword of Honour"
            },
            {
                "author": "Herman Melville",
                "category": "fiction",
                "isbn": "0-553-21311-3",
                "price": 10.0,
                "title": "Moby Dick"
            }
        ]
    }
}
```

