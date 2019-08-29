// Copyright 2017 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <cstdint>

using namespace jsoncons;

TEST_CASE("encode and decode json")
{
    json j(std::make_pair(false,std::string("foo")));

    SECTION("string test")
    {
        std::string s;
        encode_json(j, s);
        json result = decode_json<json>(s);
        CHECK(result == j);
    }

    SECTION("stream test")
    {
        std::stringstream ss;
        encode_json(j, ss);
        json result = decode_json<json>(ss);
        CHECK(result == j);
    }
}

TEST_CASE("encode and decode wjson")
{
    wjson j(std::make_pair(false,std::wstring(L"foo")));

    SECTION("string test")
    {
        std::wstring s;
        encode_json(j, s);
        wjson result = decode_json<wjson>(s);
        CHECK(result == j);
    }

    SECTION("stream test")
    {
        std::wstringstream ss;
        encode_json(j, ss);
        wjson result = decode_json<wjson>(ss);
        CHECK(result == j);
    }
}

TEST_CASE("convert_pair_test")
{
    auto val = std::make_pair(false,std::string("foo"));
    std::string s;

    jsoncons::encode_json(val, s);

    auto result = jsoncons::decode_json<std::pair<bool,std::string>>(s);

    CHECK(val == result);
}

TEST_CASE("convert_vector_test")
{
    std::vector<double> v = {1,2,3,4,5,6};

    std::string s;
    jsoncons::encode_json(v,s);

    auto result = jsoncons::decode_json<std::vector<double>>(s);

    REQUIRE(v.size() == result.size());
    for (size_t i = 0; i < result.size(); ++i)
    {
        CHECK(v[i] == result[i]);
    }
}

TEST_CASE("convert_map_test")
{
    std::map<std::string,double> m = {{"a",1},{"b",2}};

    std::string s;
    jsoncons::encode_json(m,s);
    auto result = jsoncons::decode_json<std::map<std::string,double>>(s);

    REQUIRE(result.size() == m.size());
    CHECK(m["a"] == result["a"]);
    CHECK(m["b"] == result["b"]);
}

TEST_CASE("convert_array_test")
{
    std::array<double,4> v{1,2,3,4};

    std::string s;
    jsoncons::encode_json(v,s);
    std::array<double, 4> result = jsoncons::decode_json<std::array<double,4>>(s);
    REQUIRE(result.size() == v.size());
    for (size_t i = 0; i < result.size(); ++i)
    {
        CHECK(v[i] == result[i]);
    }
}

#if !(defined(__GNUC__) && __GNUC__ <= 5)
TEST_CASE("convert_tuple_test")
{
    typedef std::map<std::string,std::tuple<std::string,std::string,double>> employee_collection;

    employee_collection employees = 
    { 
        {"John Smith",{"Hourly","Software Engineer",10000}},
        {"Jane Doe",{"Commission","Sales",20000}}
    };

    std::string s;
    jsoncons::encode_json(employees, s, jsoncons::indenting::indent);
    std::cout << "(1)\n" << s << std::endl;
    auto employees2 = jsoncons::decode_json<employee_collection>(s);
    REQUIRE(employees2.size() == employees.size());

    std::cout << "\n(2)\n";
    for (const auto& pair : employees2)
    {
        std::cout << pair.first << ": " << std::get<1>(pair.second) << std::endl;
    }
}
#endif

namespace ns {

struct book
{
    std::string author;
    std::string title;
    double price;

    friend std::ostream& operator<<(std::ostream& os, const book& b)
    {
        std::cout << "author: " << b.author << ", title: " << b.title << ", price: " << b.price << "\n";
        return os;
    }
};

} // namespace ns

JSONCONS_MEMBER_TRAITS_DECL(ns::book,author,title,price)
    
TEST_CASE("book_conversion_test")
{
    ns::book book_list{"Haruki Murakami", "Kafka on the Shore", 25.17};

    std::string s;
    encode_json(book_list, s, indenting::indent);

    std::cout << "s: " << s << "\n";

}

namespace ns {

    struct reputon
    {
        std::string rater;
        std::string assertion;
        std::string rated;
        double rating;

        friend bool operator==(const reputon& lhs, const reputon& rhs)
        {
            return lhs.rater == rhs.rater && lhs.assertion == rhs.assertion && 
                   lhs.rated == rhs.rated && lhs.rating == rhs.rating;
        }

        friend bool operator!=(const reputon& lhs, const reputon& rhs)
        {
            return !(lhs == rhs);
        };
    };

    class reputation_object
    {
        std::string application;
        std::vector<reputon> reputons;

        // Make json_type_traits specializations friends to give accesses to private members
        JSONCONS_TYPE_TRAITS_FRIEND;
    public:
        reputation_object()
        {
        }
        reputation_object(const std::string& application, const std::vector<reputon>& reputons)
            : application(application), reputons(reputons)
        {}

        friend bool operator==(const reputation_object& lhs, const reputation_object& rhs)
        {
            return (lhs.application == rhs.application) && (lhs.reputons == rhs.reputons);
        }

        friend bool operator!=(const reputation_object& lhs, const reputation_object& rhs)
        {
            return !(lhs == rhs);
        };
    };

} // namespace ns

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_MEMBER_TRAITS_DECL(ns::reputon, rater, assertion, rated, rating)
JSONCONS_MEMBER_TRAITS_DECL(ns::reputation_object, application, reputons)

TEST_CASE("reputation_object")
{
    ns::reputation_object val("hiking", { ns::reputon{"HikingAsylum.example.com","strong-hiker","Marilyn C",0.90} });

    SECTION("1")
    {
        std::string s;
        encode_json(val, s);
        auto val2 = decode_json<ns::reputation_object>(s);
        CHECK(val2 == val);
    }

    SECTION("2")
    {
        std::string s;
        encode_json(val, s, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(s);
        CHECK(val2 == val);
    }

    SECTION("3")
    {
        std::string s;
        json_options options;
        encode_json(val, s, options, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(s, options);
        CHECK(val2 == val);
    }

    SECTION("4")
    {
        std::string s;
        encode_json(ojson(), val, s);
        auto val2 = decode_json<ns::reputation_object>(ojson(), s);
        CHECK(val2 == val);
    }

    SECTION("5")
    {
        std::string s;
        encode_json(ojson(), val, s, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(ojson(), s);
        CHECK(val2 == val);
    }

    SECTION("6")
    {
        std::string s;
        json_options options;
        encode_json(ojson(), val, s, options, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(ojson(), s, options);
        CHECK(val2 == val);
    }

    SECTION("os 1")
    {
        std::stringstream os;
        encode_json(val, os);
        auto val2 = decode_json<ns::reputation_object>(os);
        CHECK(val2 == val);
    }

    SECTION("os 2")
    {
        std::stringstream os;
        encode_json(val, os, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(os);
        CHECK(val2 == val);
    }

    SECTION("os 3")
    {
        std::stringstream os;
        json_options options;
        encode_json(val, os, options, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(os, options);
        CHECK(val2 == val);
    }

    SECTION("os 4")
    {
        std::stringstream os;
        encode_json(ojson(), val, os);
        auto val2 = decode_json<ns::reputation_object>(ojson(), os);
        CHECK(val2 == val);
    }

    SECTION("os 5")
    {
        std::stringstream os;
        encode_json(ojson(), val, os, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(ojson(), os);
        CHECK(val2 == val);
    }

    SECTION("os 6")
    {
        std::stringstream os;
        json_options options;
        encode_json(ojson(), val, os, options, indenting::indent);
        auto val2 = decode_json<ns::reputation_object>(ojson(), os, options);
        CHECK(val2 == val);
    }

}



