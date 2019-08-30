// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <string>
#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

using namespace jsoncons;

void json_query_examples() 
{
    std::ifstream is("./input/booklist.json");
    json booklist = json::parse(is);

    // The authors of books that are cheaper than $10
    json result1 = jsonpath::json_query(booklist, "$.store.book[?(@.price < 10)].author");
    std::cout << "(1) " << result1 << "\n";

    // The number of books
    json result2 = jsonpath::json_query(booklist, "$..book.length");
    std::cout << "(2) " << result2 << "\n";

    // The third book
    json result3 = jsonpath::json_query(booklist, "$..book[2]");
    std::cout << "(3)\n" << pretty_print(result3) << "\n";

    // All books whose author's name starts with Evelyn
    json result4 = jsonpath::json_query(booklist, "$.store.book[?(@.author =~ /Evelyn.*?/)]");
    std::cout << "(4)\n" << pretty_print(result4) << "\n";

    // The titles of all books that have isbn number
    json result5 = jsonpath::json_query(booklist, "$..book[?(@.isbn)].title");
    std::cout << "(5) " << result5 << "\n";

    // All authors and titles of books
    json result6 = jsonpath::json_query(booklist, "$['store']['book']..['author','title']");
    std::cout << "(6)\n" << pretty_print(result6) << "\n";

    // Union of two ranges of book titles
    json result7 = jsonpath::json_query(booklist, "$..book[1:2,2:4].title");
    std::cout << "(7)\n" << pretty_print(result7) << "\n";

    // Union of a subset of book titles identified by index
    json result8 = jsonpath::json_query(booklist, "$.store[book[0].title,book[1].title,book[3].title]");
    std::cout << "(8)\n" << pretty_print(result8) << "\n";

    // Union of third book title and all book titles with price > 10
    json result9 = jsonpath::json_query(booklist, "$.store[book[3].title,book[?(@.price > 10)].title]");
    std::cout << "(9)\n" << pretty_print(result9) << "\n";

    // Intersection of book titles with category fiction and price < 15
    json result10 = jsonpath::json_query(booklist, "$.store.book[?(@.category == 'fiction')][?(@.price < 15)].title");
    std::cout << "(10)\n" << pretty_print(result10) << "\n";

    // Normalized path expressions
    json result11 = jsonpath::json_query(booklist, "$.store.book[?(@.author =~ /Evelyn.*?/)]", jsonpath::result_type::path);
    std::cout << "(11)\n" << pretty_print(result11) << "\n";

    // All titles whose author's second name is 'Waugh'
    json result12 = jsonpath::json_query(booklist,"$.store.book[?(tokenize(@.author,'\\\\s+')[1] == 'Waugh')].title");
    std::cout << "(12)\n" << result12 << "\n";

    // All keys in the second book
    json result13 = jsonpath::json_query(booklist,"keys($.store.book[1])[*]");
    std::cout << "(13)\n" << result13 << "\n";
}

void json_replace_example1()
{ 
    std::ifstream is("./input/booklist.json");
    json booklist = json::parse(is);

    jsonpath::json_replace(booklist,"$.store.book[?(@.isbn == '0-553-21311-3')].price",10.0);
    std::cout << pretty_print(booklist) << "\n";
}

void json_replace_example2()
{
    json j;
    try
    {
        j = json::parse(R"(
{"store":
{"book": [
{"category": "reference",
"author": "Margaret Weis",
"title": "Dragonlance Series",
"price": 31.96}, 
{"category": "reference",
"author": "Brent Weeks",
"title": "Night Angel Trilogy",
"price": 14.70
}]}}
)");
    }
    catch (const std::exception& e)
    {
        std::cout << e.what() << "\n";
    }

    std::cout << ("1\n") << pretty_print(j) << "\n";

    jsonpath::json_replace(j,"$..book[?(@.price==31.96)].price", 30.9);

    std::cout << ("2\n") << pretty_print(j) << "\n";
}

void jsonpath_complex_examples()
{
    const json j = json::parse(R"(
    [
      {
        "root": {
          "id" : 10,
          "second": [
            {
                 "names": [
                   2
              ],
              "complex": [
                {
                  "names": [
                    1
                  ],
                  "panels": [
                    {
                      "result": [
                        1
                      ]
                    },
                    {
                      "result": [
                        1,
                        2,
                        3,
                        4
                      ]
                    },
                    {
                      "result": [
                        1
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      },
      {
        "root": {
          "id" : 20,
          "second": [
            {
              "names": [
                2
              ],
              "complex": [
                {
                  "names": [
                    1
                  ],
                  "panels": [
                    {
                      "result": [
                        1
                      ]
                    },
                    {
                      "result": [
                        3,
                        4,
                        5,
                        6
                      ]
                    },
                    {
                      "result": [
                        1
                      ]
                    }
                  ]
                }
              ]
            }
          ]
        }
      }
    ]
    )");

    // Find all arrays of elements where result.length is 4
    json result1 = jsonpath::json_query(j,"$..[?(@.result.length == 4)].result");
    std::cout << "(1) " << result1 << "\n";

    // Find array of elements that has id 10 and result.length is 4
    json result2 = jsonpath::json_query(j,"$..[?(@.id == 10)]..[?(@.result.length == 4)].result");
    std::cout << "(2) " << result2 << "\n";

    // Find all arrays of elements where result.length is 4 and that have value 3 
    json result3 = jsonpath::json_query(j,"$..[?(@.result.length == 4 && (@.result[0] == 3 || @.result[1] == 3 || @.result[2] == 3 || @.result[3] == 3))].result");
    std::cout << "(3) " << result3 << "\n";
}

void jsonpath_union()
{
    const json root = json::parse(R"(
{
  "firstName": "John",
  "lastName" : "doe",
  "age"      : 26,
  "address"  : {
    "streetAddress": "naist street",
    "city"         : "Nara",
    "postalCode"   : "630-0192"
  },
  "phoneNumbers": [
    {
      "type"  : "iPhone",
      "number": "0123-4567-8888"
    },
    {
      "type"  : "home",
      "number": "0123-4567-8910"
    }
  ]
}    )");

    std::string path = "$..[firstName,address.city]";
    json result = jsonpath::json_query(root,path);

    std::cout << result << "\n";
}

void jsonpath_examples()
{
    std::cout << "\nJsonPath examples\n\n";
    std::cout << "Here\n";
    json_replace_example1();
    json_replace_example2();
    jsonpath_complex_examples();
    jsonpath_union();
    json_query_examples();
    std::cout << "\n";
}

