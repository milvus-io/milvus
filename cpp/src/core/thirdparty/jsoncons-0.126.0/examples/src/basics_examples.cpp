// jsoncons_test.cpp : Defines the entry point for the console application.
//

#include <fstream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpath/json_query.hpp>

using namespace jsoncons;
using namespace jsoncons::jsonpath;

void basics_json_example1()
{
    // Construct a book object
    json book1;

    book1["category"] = "Fiction";
    book1["title"] = "A Wild Sheep Chase: A Novel";
    book1["author"] = "Haruki Murakami";
    book1["date"] = "2002-04-09";
    book1["price"] = 9.01;
    book1["isbn"] = "037571894X";  

    // Construct another using the member function insert_or_assign
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
    booklist.push_back(std::move(book1));    
    booklist.push_back(std::move(book2));    

    // Add the third one to the front
    auto where = booklist.insert(booklist.array_range().begin(),std::move(book3));
    
    // Add the last one immediately after
    booklist.insert(where+1,std::move(book4));    

    // See what's left of book1, 2, 3 and 4 (expect nulls)
    std::cout << book1 << "," << book2 << "," << book3 << "," << book4 << std::endl;

    //Loop through the booklist elements using a range-based for loop    
    for(auto book : booklist.array_range())
    {
        std::cout << book["title"].as<std::string>()
                  << ","
                  << book["price"].as<double>() << std::endl;
    }

    // The second book
    json& book = booklist[1];

    //Loop through the book members using a range-based for loop    
    for(auto member : book.object_range())
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
        // book has member "author"
    }

    std::string s = book.get_with_default("author", "author unknown");
    // Returns author if found, otherwise "author unknown"

    try
    {
        book["ratings"].as<std::string>();
    }
    catch (const std::out_of_range&)
    {
        // member "ratings" not found
    }

    // Add ratings
    book["ratings"]["*****"] = 4;
    book["ratings"]["*"] = 1;

    // Delete one-star ratings
    book["ratings"].erase("*");

    // Serialize the booklist to a file
    std::ofstream os("./output/booklist.json");
    os << pretty_print(booklist);
}

void basics_json_example2()
{
    // Deserialize the booklist
    std::ifstream is("./output/booklist.json");
    json booklist;
    is >> booklist;

    // Use a JSONPath expression to find 
      
    // (1) The authors of books that cost less than $12
    json result = json_query(booklist, "$[*][?(@.price < 12)].author");
    std::cout << "(1) " << result << std::endl;

    // (2) The number of books
    result = json_query(booklist, "$.length");
    std::cout << "(2) " << result << std::endl;

    // (3) The third book
    result = json_query(booklist, "$[2]");
    std::cout << "(3) " << std::endl << pretty_print(result) << std::endl;

    // (4) The authors of books that were published in 2004
    result = json_query(booklist, "$[*][?(@.date =~ /2004.*?/)].author");
    std::cout << "(4) " << result << std::endl;

    // (5) The titles of all books that have ratings
    result = json_query(booklist, "$[*][?(@.ratings)].title");
    std::cout << "(5) " << result << std::endl;
}

void basics_examples()
{
    std::cout << "\nBasics\n\n";
    basics_json_example1();
    basics_json_example2();
    std::cout << std::endl;
}

