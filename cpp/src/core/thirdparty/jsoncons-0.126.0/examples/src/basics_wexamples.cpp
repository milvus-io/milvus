// jsoncons_test.cpp : Defines the entry point for the console application.
//

#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

using namespace jsoncons;

void basics_wjson_example1()
{
    // Construct a book object
    wjson book1;

    book1[L"category"] = L"Fiction";
    book1[L"title"] = L"A Wild Sheep Chase: A Novel";
    book1[L"author"] = L"Haruki Murakami";
    book1[L"date"] = L"2002-04-09";
    book1[L"price"] = 9.01;
    book1[L"isbn"] = L"037571894X";

    // Construct another using the member function insert_or_assign
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
    booklist.push_back(std::move(book1));
    booklist.push_back(std::move(book2));

    // Add the third one to the front
    auto where = booklist.insert(booklist.array_range().begin(),std::move(book3));

    // Add the last one immediately after
    booklist.insert(where+1,std::move(book4));    

    // See what's left of book1, 2, 3 and 4 (expect nulls)
    std::wcout << book1 << L"," << book2 << L"," << book3 << L"," << book4 << std::endl;

    //Loop through the booklist elements using a range-based for loop    
    for (const auto& book : booklist.array_range())
    {
        std::wcout << book[L"title"].as<std::wstring>()
                   << L","
                   << book[L"price"].as<double>() << std::endl;
    }

    // The second book
    wjson& book = booklist[1];

    //Loop through the book members using a range-based for loop    
    for(const auto& member : book.object_range())
    {
        std::wcout << member.key()
                   << L","
                   << member.value() << std::endl;
    }

    auto it = book.find(L"author");
    if (it != book.object_range().end())
    {
        // book has member "author"
    }

    if (book.contains(L"author"))
    {
        // book has member "author"
    }

    std::wstring s = book.get_with_default(L"author", L"author unknown");
    // Returns author if found, otherwise "author unknown"

    try
    {
        book[L"ratings"].as<std::wstring>();
    }
    catch (const std::out_of_range&)
    {
        // member "ratings" not found
    }

    // Add ratings
    book[L"ratings"][L"*****"] = 4;
    book[L"ratings"][L"*"] = 2;

    // Delete one-star ratings
    book[L"ratings"].erase(L"*");

    // Serialize the booklist to a file
    std::wofstream os("./output/booklist2.json");
    os << pretty_print(booklist);
}

void basics_wjson_example2()
{
    // Deserialize the booklist
    std::wifstream is("./output/booklist2.json");
    wjson booklist;
    is >> booklist;

    // Use a JSONPath expression to find 
    //  
    // (1) The authors of books that cost less than $12
    wjson result = jsonpath::json_query(booklist, L"$[*][?(@.price < 12)].author");
    std::wcout << L"(1) " << result << std::endl;

    // (2) The number of books
    result = jsonpath::json_query(booklist, L"$.length");
    std::wcout << L"(2) " << result << std::endl;

    // (3) The third book
    result = jsonpath::json_query(booklist, L"$[2]");
    std::wcout << L"(3) " << std::endl << pretty_print(result) << std::endl;

    // (4) The authors of books that were published in 2004
    result = jsonpath::json_query(booklist, L"$[*][?(@.date =~ /2004.*?/)].author");
    std::wcout << L"(4) " << result << std::endl;

    // (5) The titles of all books that have ratings
    result = jsonpath::json_query(booklist, L"$[*][?(@.ratings)].title");
    std::wcout << L"(5) " << result << std::endl;
}

void basics_wexamples()
{
    std::cout << "\nBasics\n\n";
    basics_wjson_example1();
    basics_wjson_example2();
    std::cout << std::endl;
}

