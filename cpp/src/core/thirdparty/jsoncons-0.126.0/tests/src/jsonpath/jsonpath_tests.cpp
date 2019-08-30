// Copyright 2013 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>
#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <unordered_set> // std::unordered_set

using namespace jsoncons;

TEST_CASE("jsonpath name tests")
{
    ojson example = ojson::parse(R"(
    {
        "\\\\" : "1",
        "*" : "2",
        "\\\"" : "3"
    }
    )");

    SECTION("test 1")
    {
        ojson expected = ojson::array();
        expected.push_back(example.at("\\\\"));

        ojson result1 = jsonpath::json_query(example,"$.\\\\");
        ojson result2 = jsonpath::json_query(example, "$. \\\\ ");
        ojson result3 = jsonpath::json_query(example,"$[\\\\]");
        ojson result4 = jsonpath::json_query(example, "$['\\\\\\\\']");
        ojson result5 = jsonpath::json_query(example, "$[\"\\\\\\\\\"]");

        CHECK(result1 == expected);
        CHECK(result2 == expected);
        CHECK(result3 == expected);
        CHECK(result4 == expected);
        CHECK(result5 == expected);

        REQUIRE_THROWS_AS(jsonpath::json_query(example,"$.'\\'"),jsonpath::jsonpath_error);
    }

    SECTION("test 2")
    {
        ojson expected = ojson::array();
        expected.push_back(example.at("\\\\"));
        expected.push_back(example.at("*"));
        expected.push_back(example.at("\\\""));

        ojson result1 = jsonpath::json_query(example,"$.*");
        ojson result2 = jsonpath::json_query(example, "$. * ");
        ojson result3 = jsonpath::json_query(example, "$[*]");
        ojson result4 = jsonpath::json_query(example, "$ [*] ");
        ojson result5 = jsonpath::json_query(example, "$ [ * ] ");

        CHECK(result1 == expected);
        CHECK(result2 == expected);
        CHECK(result3 == expected);
        CHECK(result4 == expected);
        CHECK(result5 == expected);
    }

    SECTION("test 3")
    {
        ojson expected = ojson::array();
        expected.push_back(example.at("*"));

        ojson result1 = jsonpath::json_query(example, "$['*']");
        ojson result2 = jsonpath::json_query(example, "$ [\"*\"] ");

        CHECK(result1 == expected);
        CHECK(result2 == expected);
    }
}

TEST_CASE("jsonpath store tests")
{
    json store = json::parse(R"(
    {
        "store": {
            "book": [
                {
                    "category": "reference",
                    "author": "Nigel Rees",
                    "title": "Sayings of the Century",
                    "price": 8.95
                },
                {
                    "category": "fiction",
                    "author": "Evelyn Waugh",
                    "title": "Sword of Honour",
                    "price": 12.99
                },
                {
                    "category": "fiction",
                    "author": "Herman Melville",
                    "title": "Moby Dick",
                    "isbn": "0-553-21311-3",
                    "price": 8.99
                },
                {
                    "category": "fiction",
                    "author": "J. R. R. Tolkien",
                    "title": "The Lord of the Rings",
                    "isbn": "0-395-19395-8",
                    "price": 22.99
                }
            ],
            "bicycle": {
                "color": "red",
                "price": 19.95
            }
        }
    }
    )");


    SECTION("Absolute path")
    {
        json result1 = jsonpath::json_query(store,"$");
        json result2 = jsonpath::json_query(store,"$.*");
        json result3 = jsonpath::json_query(store,"$..*");

        REQUIRE(result1.size() == 1);
        CHECK(result1[0] == store);
        REQUIRE(result2.size() == 1);
        CHECK(result2[0] == store["store"]);
        REQUIRE(result3.size() == 1);
        CHECK(result3[0] == store["store"]);

        json result4 = jsonpath::json_query(store,"$ ");
        json result5 = jsonpath::json_query(store,"$ \n.\n * ");
        json result6 = jsonpath::json_query(store,"$ .. * ");

        REQUIRE(result4.size() == 1);
        CHECK(result4[0] == store);
        REQUIRE(result5.size() == 1);
        CHECK(result5[0] == store["store"]);
        REQUIRE(result6.size() == 1);
        CHECK(result6[0] == store["store"]);
    }

    SECTION("test_path")
    {
        json book_list = store["store"]["book"];
        json bicycle = store["store"]["bicycle"];

        json result1 = jsonpath::json_query(store,"$.store.book");
        json result2 = jsonpath::json_query(store,"$. store . book ");
        json result3 = jsonpath::json_query(store,"store.book");
        json result4 = jsonpath::json_query(store," store . book ");
        json result5 = jsonpath::json_query(store,"$.'store'.'book'");
        json result6 = jsonpath::json_query(store,"\"store\".\"book\"");

        json expected = json::array();
        expected.push_back(book_list);

        CHECK(result1 == expected);
        CHECK(result2 == expected);
        CHECK(result3 == expected);
        CHECK(result4 == expected);
        CHECK(result5 == expected);
        CHECK(result6 == expected);
    }

        //std::cout << pretty_print(result) << std::endl;
    SECTION("test_jsonpath_store_book2")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$['store']['book']");

        json expected = json::array();
        expected.push_back(book_list);

        CHECK(result == expected);
        //    std::c/out << pretty_print(result) << std::endl;
    }

    SECTION("$.store[book[0].title,book[1].title,book[3].title]")
    {
        json result = jsonpath::json_query(store,"$.store[book[0].title,book[1].title,book[3].title]");
        json expected = json::parse(R"(["Sayings of the Century","Sword of Honour","The Lord of the Rings"])");
        CHECK(result == expected);
    }

    SECTION("$.store[book[3].title,book[?(@.price > 10)].title]")
    {
        json result = jsonpath::json_query(store,"$.store[book[3].title,book[?(@.price > 10)].title]");
        json expected = json::parse(R"(["Sword of Honour","The Lord of the Rings"])");
        CHECK(result == expected);
    }

    SECTION("test_jsonpath_bracket_with_double_quotes")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$[\"store\"][\"book\"]");

        json expected = json::array();
        expected.push_back(book_list);

        CHECK(result == expected);
        //    std::c/out << pretty_print(result) << std::endl;
    }
    SECTION("test_jsonpath_store_book_bicycle")
    {
        /* SECTION("one")
        {

            json result = jsonpath::json_query(store,"$['store']['book']");
            json expected = json::array();
            expected.push_back(book_list);
            CHECK(result == expected);
        }*/

        SECTION("two")
        {
            json book_list = store["store"]["book"];
            json bicycle = store["store"]["bicycle"];

            json result = jsonpath::json_query(store,"$['store']['book','bicycle']");
            json expected1 = json::array();
            expected1.push_back(book_list);
            expected1.push_back(bicycle);
            json expected2 = json::array();
            expected2.push_back(bicycle);
            expected2.push_back(book_list);
            CHECK((result == expected1 || result == expected2));
        }

        //std::cout << pretty_print(result) << std::endl;
    }

    SECTION("test_jsonpath_store_book_bicycle_unquoted")
    {
        json book_list = store["store"]["book"];
        json bicycle = store["store"]["bicycle"];


        json result = jsonpath::json_query(store,"$[store][book,bicycle]");

        json expected1 = json::array();
        expected1.push_back(book_list);
        expected1.push_back(bicycle);
        json expected2 = json::array();
        expected2.push_back(bicycle);
        expected2.push_back(book_list);
        CHECK((result == expected1 || result == expected2));

        //std::cout << pretty_print(result) << std::endl;
    }

    SECTION("test_jsonpath_store_book_union")
    {
        json result = jsonpath::json_query(store,"$['store']..['author','title']");

        json expected = json::parse(R"(
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
        )");
    }

    SECTION("test_jsonpath_store_book_star")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$['store']['book'][*]");
        json expected = book_list;

        //std::cout << pretty_print(result) << std::endl;
        CHECK(result == expected);
    }

    SECTION("test_store_dotdot_price")
    {
        json book_list = store["store"]["book"];
        json bicycle = store["store"]["bicycle"];

        json result = jsonpath::json_query(store,"$.store..price");

        json expected = json::array();
        expected.push_back(bicycle["price"]);
        for (size_t i = 0; i < book_list.size(); ++i)
        {
            expected.push_back(book_list[i]["price"]);
        }

        //std::cout << pretty_print(result) << std::endl;

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_recursive_descent")
    {
        json result1 = jsonpath::json_query(store,"$..book[2]");
        //std::cout << pretty_print(result1) << std::endl;
        CHECK(result1.size() == 1);

        CHECK(result1[0] == store["store"]["book"][2]);

        json result1a = jsonpath::json_query(store,"$..book.2");
        //std::cout << pretty_print(result1a) << std::endl;
        CHECK(result1a.size() == 1);
        CHECK(result1a[0] == store["store"]["book"][2]);

        json result2 = jsonpath::json_query(store,"$..book[-1:]");
        //std::cout << pretty_print(result2) << std::endl;
        CHECK(result2.size() == 1);
        CHECK(result2[0] == store["store"]["book"][3]);

        SECTION("$..book[0,1]")
        {
            json result = jsonpath::json_query(store,"$..book[0,1]");

            json expected1 = json::array{store["store"]["book"][0],store["store"]["book"][1]};
            json expected2 = json::array{store["store"]["book"][1],store["store"]["book"][0]};
            //std::cout << pretty_print(result) << std::endl;
            CHECK(result.size() == 2);
            CHECK((result == expected1 || result == expected2));
        }

        json result4 = jsonpath::json_query(store,"$..book[:2]");
        //std::cout << pretty_print(result4) << std::endl;
        CHECK(result4.size() == 2);
        CHECK(result4[0] == store["store"]["book"][0]);
        CHECK(result4[1] == store["store"]["book"][1]);

        json result5 = jsonpath::json_query(store,"$..book[1:2]");
        //std::cout << pretty_print(result5) << std::endl;
        CHECK(result5.size() == 1);
        CHECK(result5[0] == store["store"]["book"][1]);

        json result6 = jsonpath::json_query(store,"$..book[-2:]");
        //std::cout << pretty_print(result6) << std::endl;
        CHECK(result6.size() == 2);
        CHECK(result6[0] == store["store"]["book"][2]);
        CHECK(result6[1] == store["store"]["book"][3]);

        json result7 = jsonpath::json_query(store,"$..book[2:]");
        //std::cout << pretty_print(result7) << std::endl;
        CHECK(result7.size() == 2);
        CHECK(result7[0] == store["store"]["book"][2]);
        CHECK(result7[1] == store["store"]["book"][3]);
    }

    SECTION("test_jsonpath_filter1")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$..book[?(@.price<10)]");
        //std::cout << pretty_print(result) << std::endl;
        json books = book_list;
        json expected = json::array();
        for (size_t i = 0; i < books.size(); ++i)
        {
            double price = books[i]["price"].as<double>();
            if (price < 10)
            {
                expected.push_back(books[i]);
            }
        }
        CHECK(result == expected);
    }

    SECTION("test_jsonpath_filter2")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$..book[?(10 > @.price)]");

        //std::cout << pretty_print(result) << std::endl;
        json books = book_list;
        json expected = json::array();
        for (size_t i = 0; i < books.size(); ++i)
        {
            double price = books[i]["price"].as<double>();
            if (10 > price)
            {
                expected.push_back(books[i]);
            }
        }
        CHECK(result == expected);
    }
     
    SECTION("test_jsonpath_filter_category_eq_reference")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$..book[?(@.category == 'reference')]");

        //std::cout << pretty_print(result) << std::endl;
        json books = book_list;
        json expected = json::array();
        for (size_t i = 0; i < books.size(); ++i)
        {
            double price = books[i]["price"].as<double>();
            (void)price;

            if (books[i]["category"].as<std::string>() == "reference")
            {
                expected.push_back(books[i]);
            }
        }
        CHECK(result == expected);
    }

    SECTION("test_jsonpath_filter3")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$..book[?((@.price > 8) && (@.price < 12))]");

        json books = book_list;

        json expected = json::array();
        for (size_t i = 0; i < books.size(); ++i)
        {
            double price = books[i]["price"].as<double>();
            if (price > 8 && price < 12)
            {
                expected.push_back(books[i]);
            }
        }
        //std::cout << pretty_print(result) << std::endl;
        //std::cout << pretty_print(expected) << std::endl;

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_book_isbn")
    {
        json book_list = store["store"]["book"];

        json books = book_list;
        for (size_t i = 0; i < books.size(); ++i)
        {
            bool has_isbn = books[i].contains("isbn");
            if (has_isbn)
            {
                json result = jsonpath::json_query(books[i],"$.isbn");
                json expected = json::array();
                expected.push_back(books[i]["isbn"]);
                CHECK(result == expected);
                //std::cout << pretty_print(result) << std::endl;
            }
        }
    }

    SECTION("test_jsonpath_book_empty_isbn")
    {
        json book_list = store["store"]["book"];

        const char* empty_isbn = "{ \"store\": {\"book\": [ { \"category\": \"reference\",\"author\": \"Nigel Rees\",\"title\": \"Sayings of the Century\",\"price\": 8.95},{ \"category\": \"fiction\",\"author\": \"Evelyn Waugh\",\"title\": \"Sword of Honour\",\"price\": 12.99},{ \"category\": \"fiction\",\"author\": \"Herman Melville\",\"title\": \"Moby Dick\",\"isbn\": \"0-553-21311-3\",\"price\": 8.99},{ \"category\": \"fiction\",\"author\": \"J. R. R. Tolkien\",\"title\": \"The Lord of the Rings\",\"isbn\": \"\",\"price\": 22.99}],\"bicycle\": {\"color\": \"red\",\"price\": 19.95}}}";
        json root2 = json::parse(empty_isbn);

        json books = book_list;
        for (size_t i = 0; i < books.size(); ++i)
        {
            bool has_isbn = books[i].contains("isbn");
            if (has_isbn)
            {
                json result = jsonpath::json_query(books[i],"$.isbn");
                json expected = json::array();
                expected.push_back(books[i]["isbn"]);
                CHECK(result == expected);
                //std::cout << pretty_print(result) << std::endl;
            }
        }
    }

    SECTION("test_jsonpath_filter4")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$..book[?(@.isbn)]");

        json books = book_list;

        json expected = json::array();
        for (size_t i = 0; i < books.size(); ++i)
        {
            if (books[i].contains("isbn"))
            {
                expected.push_back(books[i]);
            }
        }
        //std::cout << pretty_print(result) << std::endl;
        //std::cout << pretty_print(expected) << std::endl;

        CHECK(result == expected);
    }
    SECTION("test_jsonpath_array_length")
    {
        json result = jsonpath::json_query(store,"$..book.length");

        //std::cout << pretty_print(result) << std::endl;

        CHECK(1 == result.size());
        CHECK(store["store"]["book"].size() == result[0].as<size_t>());
    }
     
    SECTION("test_jsonpath_book_category")
    {
        json book_list = store["store"]["book"];

        json book = book_list[0];

        json result = jsonpath::json_query(book,"$.category");

        CHECK(1 == result.size());
        CHECK("reference" == result[0].as<std::string>());
    }

    SECTION("test_jsonpath_book_filter_false")
    {
        json result = jsonpath::json_query(store,"$..book[?(false)]");
        //std::cout << pretty_print(result) << std::endl;
        
        json expected = json::array();

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_book_filter_false_and_false")
    {
        json result = jsonpath::json_query(store,"$..book[?(false && false)]");
        //std::cout << pretty_print(result) << std::endl;
        
        json expected = json::array();

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_book_filter_false_or_false")
    {
        json result = jsonpath::json_query(store,"$..book[?(false || false)]");
        //std::cout << pretty_print(result) << std::endl;
        
        json expected = json::array();

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_book_filter_false_or_true")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$..book[?(false || true)]");
        //std::cout << pretty_print(result) << std::endl;
        
        CHECK(result == book_list);
    }

    SECTION("test_jsonpath_store_book_authors")
    {
        json book_list = store["store"]["book"];

        json result = jsonpath::json_query(store,"$.store.book[?(@.price < 10)].author");

        json expected = json::array();
        for (size_t i = 0; i < book_list.size(); ++i)
        {
            json book = book_list[i];
            if (book["price"].as<double>() < 10)
            {
                expected.push_back(book["author"]);
            }
        }

        //json expected = book_list;

        //std::cout << pretty_print(result) << std::endl;

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_store_book_tests")
    {
        json book_list = store["store"]["book"];

        json result1 = jsonpath::json_query(store,"$.store.book[ ?(@.category == @.category) ]");
        CHECK(book_list == result1);

        json result2 = jsonpath::json_query(store,"$.store.book[ ?(@.category == @['category']) ]");
        CHECK(book_list == result2);

        json result3 = jsonpath::json_query(store,"$.store.book[ ?(@ == @) ]");
        CHECK(book_list == result3);

        json result4 = jsonpath::json_query(store,"$.store.book[ ?(@.category != @.category) ]");
        json expected4 = json::array();
        CHECK(result4 == expected4);
    }

    SECTION("test_jsonpath_store_book_tests2")
    {
        json result1 = jsonpath::json_query(store,"$.store.book[ ?((@.author == 'Nigel Rees') || (@.author == 'Evelyn Waugh')) ].author");
        json expected1 = json::array();
        expected1.push_back("Nigel Rees");
        expected1.push_back("Evelyn Waugh");
        CHECK(result1 == expected1);

        json result1b = jsonpath::json_query(store,"$.store.book[ ?((@.author == 'Nigel Rees') || (@.author == 'Evelyn Waugh')) ].title");
        json expected1b = json::array();
        expected1b.push_back("Sayings of the Century");
        expected1b.push_back("Sword of Honour");
        //std::cout << result1b << std::endl;
        CHECK(expected1b == result1b);

        json result2 = jsonpath::json_query(store,"$.store.book[ ?(((@.author == 'Nigel Rees') || (@.author == 'Evelyn Waugh')) && (@.price < 15)) ].author");
        json expected2 = json::array();
        expected2.push_back("Nigel Rees");
        expected2.push_back("Evelyn Waugh");
        CHECK(result2 == expected2);

        json result3 = jsonpath::json_query(store,"$.store.book[ ?(((@.author == 'Nigel Rees') || (@.author == 'Evelyn Waugh')) && (@.category == 'reference')) ].author");
        json expected3 = json::array();
        expected3.push_back("Nigel Rees");
        CHECK(result3 == expected3);

        json result4 = jsonpath::json_query(store,"$.store.book[ ?(((@.author == 'Nigel Rees') || (@.author == 'Evelyn Waugh')) && (@.category != 'fiction')) ].author");
        json expected4 = json::array();
        expected4.push_back("Nigel Rees");
        CHECK(result4 == expected4);

        json result5 = jsonpath::json_query(store,"$.store.book[?('a' == 'a')].author");
        json expected5 = json::array();
        expected5.push_back("Nigel Rees");
        expected5.push_back("Evelyn Waugh");
        expected5.push_back("Herman Melville");
        expected5.push_back("J. R. R. Tolkien");
        CHECK(result5 == expected5);

        json result6 = jsonpath::json_query(store,"$.store.book[?('a' == 'b')].author");
        json expected6 = json::array();
        CHECK(result6 == expected6);
    }

    #if !(defined(__GNUC__) && (__GNUC__ == 4 && __GNUC_MINOR__ < 9))
    // GCC 4.8 has broken regex support: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=53631
    SECTION("test_jsonpath_store_book_regex")
    {
        json result3 = jsonpath::json_query(store,"$.store.book[ ?(@.category =~ /fic.*?/)].author");
        json expected3 = json::array();
        expected3.push_back("Evelyn Waugh");
        expected3.push_back("Herman Melville");
        expected3.push_back("J. R. R. Tolkien");
        CHECK(result3 == expected3);

        json result4 = jsonpath::json_query(store,"$.store.book[ ?(@.author =~ /Evelyn.*?/)].author");
        json expected4 = json::array();
        expected4.push_back("Evelyn Waugh");
        CHECK(result4 == expected4);

        json result5 = jsonpath::json_query(store,"$.store.book[ ?(!(@.author =~ /Evelyn.*?/))].author");
        json expected5 = json::array();
        expected5.push_back("Nigel Rees");
        expected5.push_back("Herman Melville");
        expected5.push_back("J. R. R. Tolkien");
        CHECK(result5 == expected5);
    }
    #endif

    SECTION("test_jsonpath_everything")
    {
        json book_list = store["store"]["book"];
        json bicycle = store["store"]["bicycle"];

        json result = jsonpath::json_query(store,"$.store.*");
        //std::cout << result << std::endl;
     
        json expected = json::array();
        expected.push_back(bicycle);
        expected.push_back(book_list);

        CHECK(result == expected);
    }

    SECTION("test_jsonpath_everything_in_store")
    {
        json result = jsonpath::json_query(store,"$..*");
        //std::cout << result << std::endl;
     
        json expected = json::array();
        expected.push_back(store["store"]);

        CHECK(result == expected);
    }

    SECTION("test_array_slice_operator")
    {
        SECTION("Array slice")
        {

            json result1 = jsonpath::json_query(store,"$..book[1:2].author");
            json expected1 = json::parse(R"(
        [
           "Evelyn Waugh"
        ]
            )");
            CHECK(result1 == expected1);

            json result2 = jsonpath::json_query(store,"$..book[1:3:2].author");
            json expected2 = expected1;
            CHECK(result2 == expected2);

            json result3 = jsonpath::json_query(store,"$..book[1:4:2].author");
            json expected3 = json::parse(R"(
        [
           "Evelyn Waugh",
           "J. R. R. Tolkien"
        ]    
            )");
            CHECK(result3 == expected3);
        }

        SECTION("Union")
        {
            std::unordered_set<std::string> expected = {"Evelyn Waugh","J. R. R. Tolkien","Nigel Rees"};

            json result1 = jsonpath::json_query(store,"$..book[1:4:2,0].author");
            CHECK(result1.size() == expected.size());
            for (const auto& item : result1.array_range())
            {
                CHECK(expected.count(item.as<std::string>()) == 1);
            }

            json result2 = jsonpath::json_query(store,"$..book[1::2,0].author");
            CHECK(result2.size() == expected.size());
            for (const auto& item : result2.array_range())
            {
                CHECK(expected.count(item.as<std::string>()) == 1);
            }
        }
    }

    SECTION("test_max_pre")
    {
        std::string path = "$.store.book[*].price";
        json result = jsonpath::json_query(store,path);

        //std::cout << result << std::endl;
    }

    SECTION("test_max")
    {
        std::string path = "$.store.book[?(@.price < max($.store.book[*].price))].title";

        json expected = json::parse(R"(
    ["Sayings of the Century","Sword of Honour","Moby Dick"]
        )");

        json result = jsonpath::json_query(store,path);
        CHECK(result == expected);

        //std::cout << result << std::endl;
    }

    SECTION("test_min")
    {
        std::string path = "$.store.book[?(@.price > min($.store.book[*].price))].title";

        json expected = json::parse(R"(
    ["Sword of Honour","Moby Dick","The Lord of the Rings"]
        )");

        json result = jsonpath::json_query(store,path);
        CHECK(result == expected);
    }

    SECTION("test_sum_filter_func")
    {
        std::string path = "$.store.book[?(@.price > sum($.store.book[*].price) / 4)].title"; // unexpected exception if '$.store.book.length' instead '4'

        json expected = json::parse(R"(
    ["The Lord of the Rings"]
        )");

        json result = jsonpath::json_query(store,path);
        CHECK(result == expected);
    }

    SECTION("test_prod_func")
    {
        std::string path = "$.store.bicycle[?(479373 < prod($..price) && prod($..price) < 479374)].color";

        json expected = json::parse(R"(
    ["red"]
        )");

        json result = jsonpath::json_query(store,path);
        CHECK(result == expected);
    }

    SECTION("test_ws1")
    {
        json result = jsonpath::json_query(store,"$..book[ ?(( @.price > 8 ) && (@.price < 12)) ].author");

        json expected = json::parse(R"(
    [
       "Nigel Rees",
       "Herman Melville"
    ]
    )");

        CHECK(result == expected);
    }

}

// store tests

TEST_CASE("test_jsonpath_last_of_two_arrays")
{
    json val = json::parse(R"(
{ "store": {
    "book": [ 
          { "author": "Nigel Rees"
          },
          { "author": "Evelyn Waugh"
          },
          { "author": "Herman Melville"
          }
        ]
    },
    "Roman": {
    "book": [ 
          { "author": "Tolstoy L"
          },
          { "author": "Tretyakovskiy R"
          },
          { "author": "Kulik M"
          }
        ]
    }  
}
    )");

    json expected = json::parse(R"(
[
    { "author": "Kulik M"},
    { "author": "Herman Melville"}
]
    )");

    json result = jsonpath::json_query(val, "$..book[(@.length - 1)]");

    CHECK(result == expected);
}

TEST_CASE("test_jsonpath_next_to_last_of_two_arrays")
{
    json val = json::parse(R"(
{ "store": {
    "book": [ 
          { "author": "Nigel Rees"
          },
          { "author": "Evelyn Waugh"
          },
          { "author": "Herman Melville"
          }
        ]
    },
    "Roman": {
    "book": [ 
          { "author": "Tolstoy L"
          },
          { "author": "Tretyakovskiy R"
          },
          { "author": "Kulik M"
          }
        ]
    }  
}
    )");

    json expected = json::parse(R"(
[
    { "author": "Tretyakovskiy R"},
    { "author": "Evelyn Waugh"}
]
    )");

    json result = jsonpath::json_query(val, "$..book[(@.length - 2)]");

    CHECK(result == expected);

    json expected2 = json::parse(R"(
[
    "Tolstoy L",
    "Nigel Rees"
]
    )");
    std::string path2 = "$..[0].author";
    json result2 = jsonpath::json_query(val, path2);
    CHECK(result2 == expected2);

}

TEST_CASE("test_jsonpath_aggregation")
{
    json val = json::parse(R"(
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
}
    )");

    SECTION("$['firstName','lastName']")
    {
        json expected1 = json::parse(R"(
    ["John","doe"]
    )");
            json expected2 = json::parse(R"(
        ["doe","John"]
        )");

        json result2 = jsonpath::json_query(val, "$['firstName','lastName']");
        CHECK((result2 == expected1 || result2 == expected2));

        json result3 = jsonpath::json_query(val, "$[\"firstName\",\"lastName\"]");
        CHECK((result3 == expected1 || result3 == expected2));
    }

    SECTION("$..['firstName','city']")
    {
        json expected1 = json::parse(R"(["John","Nara"])");
        json expected2 = json::parse(R"(["Nara","John"])");
        std::string path = "$..['firstName','city']";

        json result = jsonpath::json_query(val, path);
        CHECK((result == expected1 || result == expected2));
    }
}

TEST_CASE("test_jsonpath_aggregation2")
{
    json val = json::parse(R"(
{ "store": {
    "book": [ 
          { "author": "Nigel Rees"
          },
          { "author": "Evelyn Waugh"
          },
          { "author": "Herman Melville"
          }
        ]
    }  
}
    )");

    json result = jsonpath::json_query(val, "$..book[(@.length - 1),(@.length - 2)]");

    json expected1 = json::parse(R"([{"author": "Herman Melville"},{"author": "Evelyn Waugh"}])");
    json expected2 = json::parse(R"([{"author": "Evelyn Waugh"},{"author": "Herman Melville"}])");
    CHECK((result == expected1 || result == expected2));
}

TEST_CASE("test_jsonpath_aggregation3")
{
    json val = json::parse(R"(
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
}
    )");

    std::unordered_set<std::string> expected = {"iPhone","0123-4567-8888","home","0123-4567-8910"};

    json result = jsonpath::json_query(val, "$..['type','number']");
    CHECK(result.size() == expected.size());
    for (const auto& item : result.array_range())
    {
        CHECK(expected.count(item.as<std::string>()) == 1);
    }
}

TEST_CASE("test_jsonpath_aggregation4")
{
    json val = json::parse(R"(
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
}
    )");

    json test1 = jsonpath::json_query(val, "$.phoneNumbers");
    //std::cout << test1 << std::endl;
    json test2 = jsonpath::json_query(val, "$[phoneNumbers]");
    //std::cout << test2 << std::endl;
    json test3 = jsonpath::json_query(val, "$..['type']");
    //std::cout << test3 << std::endl;

    std::unordered_set<std::string> expected = {"iPhone","0123-4567-8888","home","0123-4567-8910"};

    json result2 = jsonpath::json_query(val, "$.phoneNumbers..['type','number']");
    CHECK(result2.size() == expected.size());
    for (const auto& item : result2.array_range())
    {
        CHECK(expected.count(item.as<std::string>()) == 1);
    }
}

TEST_CASE("test_jsonpath_string_indexation")
{
    json val;
    val["about"] = "I\xe2\x82\xacJ";

    json expected1 = json::array(1,"I");
    json result1 = jsonpath::json_query(val, "$..about[0]");
    CHECK(result1 == expected1);

    json expected2 = json::array(1,"\xe2\x82\xac");
    json result2 = jsonpath::json_query(val, "$..about[1]");
    CHECK(result2 == expected2);

    json expected3 = json::array(1,"J");
    json result3 = jsonpath::json_query(val, "$..about[2]");
    CHECK(result3 == expected3);

    json expected4 = json::array(1,3);
    json result4 = jsonpath::json_query(val, "$..about.length");
    CHECK(result4 == expected4);
}

TEST_CASE("test_union_array_elements")
{
    json val = json::parse(R"(
{ "store": {
    "book": [ 
          { "author": "Nigel Rees"
          },
          { "author": "Evelyn Waugh"
          },
          { "author": "Herman Melville"
          }
        ]
    },
  "Roman": {
    "book": [ 
          { "author": "Tolstoy L"
          },
          { "author": "Tretyakovskiy R"
          },
          { "author": "Kulik M"
          }
        ]
    }  
}
    )");

    json expected1 = json::parse(R"(
[
    { "author": "Kulik M"},
    { "author": "Herman Melville"}
]
    )");
    json result1 = jsonpath::json_query(val, "$..book[-1]");
    CHECK(result1 == expected1);

    std::unordered_set<std::string> expected = {"Kulik M","Tolstoy L","Herman Melville","Nigel Rees"};

    json expected2 = json::parse(R"(
[
    {
        "author": "Kulik M"
    },
    {
        "author": "Tolstoy L"
    },
    {
        "author": "Herman Melville"
    },
    {
        "author": "Nigel Rees"
    }
]
    )");
    json result2 = jsonpath::json_query(val, "$..book[-1,-3]");

    CHECK(result2.size() == expected.size());
    for (const auto& item : result2.array_range())
    {
        CHECK(expected.count(item["author"].as<std::string>()) == 1);
    }

    //CHECK(result2 == expected2);
    //json expected3 = expected2;
    json result3 = jsonpath::json_query(val, "$..book[-1,(@.length - 3)]");
    CHECK(result3.size() == expected.size());
    for (const auto& item : result3.array_range())
    {
        CHECK(expected.count(item["author"].as<std::string>()) == 1);
    }
    //CHECK(result3 == expected3);
    //json expected4 = expected2;
    json result4 = jsonpath::json_query(val, "$..book[(@.length - 1),-3]");
    CHECK(result4.size() == expected.size());
    for (const auto& item : result4.array_range())
    {
        CHECK(expected.count(item["author"].as<std::string>()) == 1);
    }
    //CHECK(result4 == expected4);
}

TEST_CASE("test_replace")
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
"price": 31.96}, {"category": "reference",
"author": "Brent Weeks",
"title": "Night Angel Trilogy",
"price": 14.70
}]}}
)");
    }
    catch (const ser_error& e)
    {
        std::cout << e.what() << std::endl;
    }

    //std::cout << "!!!test_replace" << std::endl;
    //std::cout << ("1\n") << pretty_print(j) << std::endl;

    CHECK(31.96 == Approx(j["store"]["book"][0]["price"].as<double>()).epsilon(0.001));

    jsonpath::json_replace(j,"$..book[?(@.price==31.96)].price", 30.9);

    CHECK(30.9 == Approx(j["store"]["book"][0]["price"].as<double>()).epsilon(0.001));

    //std::cout << ("2\n") << pretty_print(j) << std::endl;
}


TEST_CASE("test_select_two")
{
    json j = json::parse(R"(
[
  {
    "a": 5,
    "b": 500,
    "c": 5000
  },
  {
    "a": 6,
    "b": 600,
    "c": 6000
  },
  {
    "a": 7,
    "b": 700,
    "c": 7000
  }
]
)");

    json result = jsonpath::json_query(j,"$..*[?((@.a == 5 && @.b == 500) || (@.a == 6 && @.b == 600))]");

    json expected = json::parse(R"(
[
  {
    "a": 5,
    "b": 500,
    "c": 5000
  },
  {
    "a": 6,
    "b": 600,
    "c": 6000
  }
]
)");

    CHECK(result == expected);
}

TEST_CASE("test_select_length_4")
{
    json j = json::parse(R"(
[
      {
        "result": [
          1,
          2,
          3,
          4
        ]
      }
]

)");

    json result = jsonpath::json_query(j,"$..[?(@.result.length == 4)]");

    json expected = json::parse(R"(
[{"result":[1,2,3,4]}]
    )");

    CHECK(result == expected);
}

TEST_CASE("complex json")
{
    const json complex_json = json::parse(R"(
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
      }
    ]
    )");

    SECTION("test_select_length_4_2")
    {
        json result = jsonpath::json_query(complex_json,"$..[?(@.result.length == 4)]");
        json expected = json::parse(R"(
    [{"result":[1,2,3,4]},{"result":[1,2,3,4]}]
        )");

        CHECK(result == expected);
    }
    SECTION("test_select_length_4_2_plus")
    {

        json result = jsonpath::json_query(complex_json,"$..[?(@.id == 10)]..[?(@.result.length == 4)]");
        //std::cout << result << std::endl;

        json expected = json::parse(R"(
    [{"result":[1,2,3,4]}]
        )");

        CHECK(result == expected);
    }

    SECTION("test_select_length_4_2_plus_plus")
    {

        json result = jsonpath::json_query(complex_json,"$..[?(@.result.length == 4)][?(@.result[0] == 3 || @.result[1] == 3 || @.result[2] == 3 || @.result[3] == 3)]");
        //std::cout << result << std::endl;

        json expected = json::parse(R"(
    [{"result":[1,2,3,4]},{"result":[1,2,3,4]}]
        )");

        CHECK(result == expected);
    }
}


TEST_CASE("test_nested")
{
    json j = json::parse(R"(
{
    "id" : 10,
    "b": {"id" : 10} 
}        
)");

    json result = jsonpath::json_query(j,"$..[?(@.id == 10)]");

    json expected = json::parse(R"(
[
   {
      "id" : 10,
      "b" : {
         "id" : 10
      }
   },
   {
      "id" : 10
   }
]
)");

    CHECK(result == expected);
}

TEST_CASE("test_array_nested")
{
    json j = json::parse(R"(
{
    "a" : [
        { 
            "id" : 10,
            "b": {"id" : 10} 
        }
    ]
}        
)");

    json result = jsonpath::json_query(j,"$..[?(@.id == 10)]");

    json expected = json::parse(R"(
[
   {
      "id" : 10,
      "b" : {
         "id" : 10
      }
   },
   {
      "id" : 10
   }
]
)");

    CHECK(result == expected);
}

TEST_CASE("test_array_array_nested")
{
    json j = json::parse(R"(
{
    "a" : [[
        { 
            "id" : 10,
            "b": {"id" : 10} 
        }
    ]]
}        
)");

    json result = jsonpath::json_query(j,"$..[?(@.id == 10)]");

    json expected = json::parse(R"(
[
   {
      "id" : 10,
      "b" : {
         "id" : 10
      }
   },
   {
      "id" : 10
   }
]
)");

    CHECK(result == expected);
}

TEST_CASE("jsonpath test 1")
{
    json j = json::parse(R"(
[
    {
        "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
    },
    {
        "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
    },
    {
        "category": "fiction",
        "author": "Herman Melville",
        "title": "Moby Dick",
        "isbn": "0-553-21311-3",
        "price": 8.99
    },
    {
        "category": "fiction",
        "author": "J. R. R. Tolkien",
        "title": "The Lord of the Rings",
        "isbn": "0-395-19395-8",
        "price": 22.99
    }
]
)");

    SECTION("$.0.category")
    {
        json result = jsonpath::json_query(j,"$.0.category");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("reference"));
    }
    SECTION("$.0.'category'")
    {
        json result = jsonpath::json_query(j,"$.0.'category'");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("reference"));
    }
    SECTION("$[0].category")
    {
        json result = jsonpath::json_query(j,"$[0].category");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("reference"));
    }
    SECTION("0.category")
    {
        json result = jsonpath::json_query(j,"0.category");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("reference"));
    }

    SECTION("0['category']")
    {
        json result = jsonpath::json_query(j,"0['category']");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("reference"));
    }
    SECTION("0[\"category\"]")
    {
        json result = jsonpath::json_query(j,"0[\"category\"]");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("reference"));
    }
    SECTION("count($.*)")
    {
        json result = jsonpath::json_query(j,"count($.*)");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<int>() == 4);
    }

    SECTION("$.*")
    {
        json result = jsonpath::json_query(j,"$.*");
        REQUIRE(result.size() == 4);
        CHECK(result == j);
    }

    SECTION("$[-3].category")
    {
        json result = jsonpath::json_query(j,"$[-3].category");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<std::string>() == std::string("fiction"));
    }

    SECTION("$[-2:].category")
    {
        json expected = json::parse(R"([ "Moby Dick", "The Lord of the Rings"])");
        json result = jsonpath::json_query(j,"$[-2:].title");
        REQUIRE(result.size() == 2);
        CHECK(result == expected);
    }

    SECTION("$[-1,-3,-4].category")
    {
        std::unordered_set<std::string> expected{"The Lord of the Rings", "Sword of Honour", "Sayings of the Century"};
        json result = jsonpath::json_query(j,"$[-1,-3,-4].title");

        CHECK(result.size() == expected.size());
        for (const auto& item : result.array_range())
        {
            CHECK(expected.count(item.as<std::string>()) == 1);
        }
    }

    SECTION("count($.*)")
    {
        json result = jsonpath::json_query(j,"count($.*)");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<int>() == 4);
    }

    SECTION("count($[*])")
    {
        json result = jsonpath::json_query(j,"count($[*])");
        REQUIRE(result.size() == 1);
        CHECK(result[0].as<int>() == 4);
    }
    SECTION("keys($[1])")
    {
        json expected = json::array{"author","category","price","title"};

        json result = jsonpath::json_query(j,"keys($[1])[*]");
        CHECK(result == expected);
    }
#if !(defined(__GNUC__) && (__GNUC__ == 4 && __GNUC_MINOR__ < 9))
// GCC 4.8 has broken regex support: https://gcc.gnu.org/bugzilla/show_bug.cgi?id=53631
    SECTION("$[?(tokenize(@.author,'\\\\s+')[1] == 'Waugh')].title")
    {
        json expected = json::array{"Sword of Honour"};

        json result = jsonpath::json_query(j,"$[?(tokenize(@.author,'\\\\s+')[1] == 'Waugh')].title");

        CHECK(result == expected);
    }

    SECTION("tokenize($[0].author,'\\\\s+')")
    {
        json expected = json::parse("[[\"Nigel\",\"Rees\"]]");

        json result = jsonpath::json_query(j,"tokenize($[0].author,'\\\\s+')");

        CHECK(result == expected);
    }
#endif
}
TEST_CASE("jsonpath array union test")
{
    json root = json::parse(R"(
[[1,2,3,4,1,2,3,4],[0,1,2,3,4,5,6,7,8,9],[0,1,2,3,4,5,6,7,8,9]]
)");
    SECTION("Test 1")
    {
        json expected = json::parse(R"(
[[0,1,2,3,4,5,6,7,8,9],[1,2,3,4,1,2,3,4]]
)");
        json result = jsonpath::json_query(root,"$[0,1,2]");
        CHECK(result == expected);
    }

    SECTION("Test 2")
    {
        json expected = json::parse(R"(
[1,2,3,4]
)");
        json result = jsonpath::json_query(root, "$[0][0:4,2:8]");
        CHECK(result == expected);
    }

    SECTION("Test 3")
    {
        json expected = json::parse(R"(
[1,4]
)");
        json result = jsonpath::json_query(root, "$[0][0,0,0,3]");
        CHECK(result == expected);
    }

    SECTION("Test 4")
    {
        json expected = json::parse(R"(
[1,2]
)");
        //json result1 = jsonpath::json_query(root,"$[0.0,1.1,2.2]");
        json result2 = jsonpath::json_query(root,"$[0[0],1[1],2[2]]");

        //CHECK(result1 == expected);
        CHECK(result2 == expected);
    }
}

TEST_CASE("jsonpath object union test 1")
{
    const json root = json::parse(R"(
[{
  "firstName": "John",
  "lastName" : "Smith",
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
},
{
  "firstName": "John",
  "lastName" : "Doe",
  "age"      : 30,
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
}]    
    
)");

    SECTION("$..[firstName,address.city]")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = "$..[firstName,address.city]";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION("$..[firstName,*.city]")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = "$..[firstName,*.city]";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION("$..[firstName,address[city]]")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = "$..[firstName,address[city]]";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION("$..[firstName,address['city']]")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = "$..[firstName,address['city']]";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION("$..[firstName,address.'city']")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = "$..[firstName,address.'city']";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION(R"($..[firstName,address."city"])")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = R"($..[firstName,address."city"])";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION(R"($..[firstName,address["city"]])")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = R"($..[firstName,address["city"]])";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION(R"($..['firstName','address'["city"]])")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = R"($..['firstName','address'["city"]])";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION(R"($..["firstName","address"["city"]])")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = R"($..["firstName","address"["city"]])";
        json result = jsonpath::json_query(root,path);
        CHECK(result == expected);
    }
    SECTION(R"($..["firstName","address"["city"]])")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = R"($..[?(@.firstName == 'John')])";
        json result = jsonpath::json_query(root,path);
        //std::cout << pretty_print(result) << "\n\n";
        //CHECK(result == expected);
    }
    SECTION(R"($..[?(@.firstName == 'John')][?(@.lastName == 'Doe')])")
    {
        json expected = json::parse(R"(["John","Nara"])");
        std::string path = R"($..[?(@.firstName == 'John')][?(@.lastName == 'Doe')])";
        json result = jsonpath::json_query(root,path);
        REQUIRE(result.size() == 1);
        CHECK(result[0] == root[1]);
    }
}

TEST_CASE("jsonpath object union test")
{
    const json root = json::parse(R"(
[{
  "firstName": "John",
  "lastName" : "Smith"
},
{
  "firstName": "John",
  "lastName" : "Doe"
}
]    
    
)");

    SECTION(R"($..["firstName","address"["city"]])")
    {
        json expected = json::parse(R"([{"firstName":"John","lastName":"Smith"},{"firstName":"John","lastName":"Doe"}])");
        std::string path = R"($..[?(@.firstName == 'John')])";
        json result = jsonpath::json_query(root,path);
        //std::cout << result << "\n\n";
        CHECK(result == expected);
    }

    SECTION(R"($..["firstName","address"["city"]])")
    {
        json expected = json::parse(R"([{"firstName":"John","lastName":"Doe"}])");
        std::string path = R"($[?(@.firstName == 'John')][?(@.lastName == 'Doe')])";
        json result = jsonpath::json_query(root,path);
        //std::cout << result << "\n\n";
        CHECK(result == expected);
    }
}

