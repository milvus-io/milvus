## Examples

The examples below illustrate the use of the [json](../ref/json.md) class and [json_query](../ref/jsonpath/json_query.md) function.

### json construction

```c++
#include <jsoncons/json.hpp>

// For convenience
using jsoncons::json;

// Construct a book object
json book1;

book1["category"] = "Fiction";
book1["title"] = "A Wild Sheep Chase: A Novel";
book1["author"] = "Haruki Murakami";
book1["date"] = "2002-04-09";
book1["price"] = 9.01;
book1["isbn"] = "037571894X";  

// Construct another using the insert_or_assign function
json book2;

book2.insert_or_assign("category", "History");
book2.insert_or_assign("title", "Charlie Wilson's War");
book2.insert_or_assign("author", "George Crile");
book2.insert_or_assign("date", "2007-11-06");
book2.insert_or_assign("price", 10.50);
book2.insert_or_assign("isbn", "0802143415");  

// Use insert_or_assign again, but more efficiently
json book3;

// Reserve memory, to avoid reallocations
book3.reserve(6);

// Insert in name alphabetical order
// Give insert_or_assign a hint where to insert the next member
auto hint = book3.insert_or_assign(book3.object_range().begin(),"author", "Haruki Murakami");
hint = book3.insert_or_assign(hint, "category", "Fiction");
hint = book3.insert_or_assign(hint, "date", "2006-01-03");
hint = book3.insert_or_assign(hint, "isbn", "1400079276");  
hint = book3.insert_or_assign(hint, "price", 13.45);
hint = book3.insert_or_assign(hint, "title", "Kafka on the Shore");

// Construct a fourth from a string
json book4 = json::parse(R"(
{
    "category" : "Fiction",
    "title" : "Pulp",
    "author" : "Charles Bukowski",
    "date" : "2004-07-08",
    "price" : 22.48,
    "isbn" : "1852272007"  
}
)");

// Construct a booklist array

json booklist = json::array();

// For efficiency, reserve memory, to avoid reallocations
booklist.reserve(4);

// For efficency, tell jsoncons to move the contents 
// of the four book objects into the array
booklist.add(std::move(book1));    
booklist.add(std::move(book2));    

// Add the third book to the front
auto pos = booklist.add(booklist.array_range().begin(),std::move(book3));

// and the last one immediately after
booklist.add(pos+1,std::move(book4));    

// See what's left of book1, 2, 3 and 4 (expect nulls)
std::cout << book1 << "," << book2 << "," << book3 << "," << book4 << std::endl;


++
//Loop through the booklist elements using a range-based for loop    
for (const auto& book : booklist.array_range())
{
    std::cout << book["title"].as<std::string>()
              << ","
              << book["price"].as<double>() << std::endl;
}

// The second book
json& book = booklist[1];

//Loop through the book's name-value pairs using a range-based for loop    
for (const auto& member : book.object_range())
{
    std::cout << member.key()
              << ","
              << member.value() << std::endl;
}

auto it = book.find("author");
if (it != book.object_range().end())
{
    // member "author" found
}

if (book.contains("author"))
{
    // book has a member "author"
}

book.get("author", "author unknown").as<std::string>();
// Returns author if found, otherwise "author unknown"

try
{
    book["ratings"].as<std::string>();
}
catch (const std::out_of_range& e)
{
    // member "ratings" not found
}

// Add ratings
book["ratings"]["*****"] = 4;
book["ratings"]["*"] = 2;

// Delete one-star ratings
book["ratings"].erase("*");

```
```c++  
    // Serialize the booklist to a file
    std::ofstream os("booklist.json");
    os << pretty_print(booklist);
```

The JSON output `booklist.json`
```json
[
    {
        "author":"Haruki Murakami",
        "category":"Fiction",
        "date":"2006-01-03",
        "isbn":"1400079276",
        "price":13.45,
        "title":"Kafka on the Shore"
    },
    {
        "author":"Charles Bukowski",
        "category":"Fiction",
        "date":"2004-07-08",
        "isbn":"1852272007",
        "price":22.48,
        "ratings":
        {
            "*****":4
        },
        "title":"Pulp"
    },
    {
        "author":"Haruki Murakami",
        "category":"Fiction",
        "date":"2002-04-09",
        "isbn":"037571894X",
        "price":9.01,
        "title":"A Wild Sheep Chase: A Novel"
    },
    {
        "author":"George Crile",
        "category":"History",
        "date":"2007-11-06",
        "isbn":"0802143415",
        "price":10.5,
        "title":"Charlie Wilson's War"
    }
]
```
### json query

```c++
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

// For convenience
using jsoncons::json;
using jsoncons::jsonpath::json_query;

// Deserialize the booklist
std::ifstream is("booklist.json");
json booklist;
is >> booklist;

// Use a JSONPath expression to find 
//  
// (1) The authors of books that cost less than $12
json result = json_query(booklist, "$[*][?(@.price < 12)].author");
std::cout << result << std::endl;

// (2) The number of books
result = json_query(booklist, "$.length");
std::cout << result << std::endl;

// (3) The third book
result = json_query(booklist, "$[2]");
std::cout << std::endl << pretty_print(result) << std::endl;

// (4) The authors of books that were published in 2004
result = json_query(booklist, "$[*][?(@.date =~ /2004.*?/)].author");
std::cout << result << std::endl;

// (5) The titles of all books that have ratings
result = json_query(booklist, "$[*][?(@.ratings)].title");
std::cout << result << std::endl;

// (6) All authors and titles of books
result = json_query(booklist, "$..['author','title']");
std::cout << pretty_print(result) << std::endl;
```
Result:
```json
(1) ["Haruki Murakami","George Crile"]
(2) [4]
(3)
[
    {
        "author":"Haruki Murakami",
        "category":"Fiction",
        "date":"2002-04-09",
        "isbn":"037571894X",
        "price":9.01,
        "title":"A Wild Sheep Chase: A Novel"
    }
]
(4) ["Charles Bukowski"]
(5) ["Pulp"]
(6) 
[
    "Nigel Rees",
    "Sayings of the Century",
    "Evelyn Waugh",
    "Sword of Honour",
    "Herman Melville",
    "Moby Dick",
    "J. R. R. Tolkien",
    "The Lord of the Rings"
]
```
## Once again, this time with wide characters

### wjson construction

```c++
#include <jsoncons/json.hpp>

// For convenience
using jsoncons::wjson;

// Construct a book object
wjson book1;

book1[L"category"] = L"Fiction";
book1[L"title"] = L"A Wild Sheep Chase: A Novel";
book1[L"author"] = L"Haruki Murakami";
book1[L"date"] = L"2002-04-09";
book1[L"price"] = 9.01;
book1[L"isbn"] = L"037571894X";

// Construct another using the insert_or_assign function
wjson book2;

book2.insert_or_assign(L"category", L"History");
book2.insert_or_assign(L"title", L"Charlie Wilson's War");
book2.insert_or_assign(L"author", L"George Crile");
book2.insert_or_assign(L"date", L"2007-11-06");
book2.insert_or_assign(L"price", 10.50);
book2.insert_or_assign(L"isbn", L"0802143415");

// Use insert_or_assign again, but more efficiently
wjson book3;

// Reserve memory, to avoid reallocations
book3.reserve(6);

// Insert in name alphabetical order
// Give insert_or_assign a hint where to insert the next member
auto hint = book3.insert_or_assign(book3.object_range().begin(), L"author", L"Haruki Murakami");
hint = book3.insert_or_assign(hint, L"category", L"Fiction");
hint = book3.insert_or_assign(hint, L"date", L"2006-01-03");
hint = book3.insert_or_assign(hint, L"isbn", L"1400079276");
hint = book3.insert_or_assign(hint, L"price", 13.45);
hint = book3.insert_or_assign(hint, L"title", L"Kafka on the Shore");

// Construct a fourth from a string
wjson book4 = wjson::parse(LR"(
{
    "category" : "Fiction",
    "title" : "Pulp",
    "author" : "Charles Bukowski",
    "date" : "2004-07-08",
    "price" : 22.48,
    "isbn" : "1852272007"  
}
)");

// Construct a booklist array

wjson booklist = wjson::array();

// For efficiency, reserve memory, to avoid reallocations
booklist.reserve(4);

// For efficency, tell jsoncons to move the contents 
// of the four book objects into the array
booklist.add(std::move(book1));
booklist.add(std::move(book2));

// Add the third book to the front
auto pos = booklist.add(booklist.array_range().begin(),std::move(book3));

// and the last one immediately after
booklist.add(pos+1,std::move(book4));    

// See what's left of book1, 2, 3 and 4 (expect nulls)
std::wcout << book1 << L"," << book2 << L"," << book3 << L"," << book4 << std::endl;

++
//Loop through the booklist elements using a range-based for loop    
for (const auto& book : booklist.array_range())
{
    std::wcout << book[L"title"].as<std::wstring>()
               << L","
               << book[L"price"].as<double>() << std::endl;
}

// The second book
wjson& book = booklist[1];

//Loop through the book's name-value pairs using a range-based for loop    
for (const auto& member : book.object_range())
{
    std::wcout << member.key()
               << L","
               << member.value() << std::endl;
}

auto it = book.find(L"author");
if (it != book.object_range().end())
{
    // member "author" found
}

if (book.contains(L"author"))
{
    // book has a member "author"
}

book.get(L"author", L"author unknown").as<std::wstring>();
// Returns author if found, otherwise "author unknown"

try
{
    book[L"ratings"].as<std::wstring>();
}
catch (const std::out_of_range& e)
{
    // member "ratings" not found
}

// Add ratings
book[L"ratings"][L"*****"] = 4;
book[L"ratings"][L"*"] = 2;

// Delete one-star ratings
book[L"ratings"].erase(L"*");

```
```c++
// Serialize the booklist to a file
std::wofstream os("booklist2.json");
os << pretty_print(booklist);
```
### wjson query

```c++
// Deserialize the booklist
std::wifstream is("booklist2.json");
wjson booklist;
is >> booklist;

// Use a JSONPath expression to find 
//  
// (1) The authors of books that cost less than $12
wjson result = json_query(booklist, L"$[*][?(@.price < 12)].author");
std::wcout << result << std::endl;

// (2) The number of books
result = json_query(booklist, L"$.length");
std::wcout << result << std::endl;

// (3) The third book
result = json_query(booklist, L"$[2]");
std::wcout << pretty_print(result) << std::endl;

// (4) The authors of books that were published in 2004
result = json_query(booklist, L"$[*][?(@.date =~ /2004.*?/)].author");
std::wcout << result << std::endl;

// (5) The titles of all books that have ratings
result = json_query(booklist, L"$[*][?(@.ratings)].title");
std::wcout << result << std::endl;

// (6) All authors and titles of books
result = json_query(booklist, L"$..['author','title']");
std::wcout << pretty_print(result) << std::endl;
```
Result:
```json
(1) ["Haruki Murakami","George Crile"]
(2) [4]
(3)
[
    {
        "author":"Haruki Murakami",
        "category":"Fiction",
        "date":"2002-04-09",
        "isbn":"037571894X",
        "price":9.01,
        "title":"A Wild Sheep Chase: A Novel"
    }
]
(4) ["Charles Bukowski"]
(5) ["Pulp"]
(6) 
[
    "Nigel Rees",
    "Sayings of the Century",
    "Evelyn Waugh",
    "Sword of Honour",
    "Herman Melville",
    "Moby Dick",
    "J. R. R. Tolkien",
    "The Lord of the Rings"
]
```

