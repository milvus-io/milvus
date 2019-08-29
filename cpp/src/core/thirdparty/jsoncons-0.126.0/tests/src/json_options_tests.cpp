// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <new>

using namespace jsoncons;

TEST_CASE("test_default_nan_replacement")
{
    json obj;
    obj["field1"] = std::sqrt(-1.0);
    obj["field2"] = 1.79e308 * 1000;
    obj["field3"] = -1.79e308 * 1000;

    std::ostringstream os;
    os << print(obj);
    std::string expected = R"({"field1":null,"field2":null,"field3":null})";

    CHECK(os.str() == expected);
}

TEST_CASE("test_write_nan_replacement")
{
    json j;
    j["field1"] = std::sqrt(-1.0);
    j["field2"] = 1.79e308 * 1000;
    j["field3"] = -1.79e308 * 1000;

    json_options options;
    options.inf_to_num("1e9999");

    std::ostringstream os;
    os << print(j, options);
    std::string expected = R"({"field1":null,"field2":1e9999,"field3":-1e9999})";

    CHECK(os.str() == expected);
}

TEST_CASE("test_read_write_read_nan_replacement")
{
    json j;
    j["field1"] = std::sqrt(-1.0);
    j["field2"] = 1.79e308 * 1000;
    j["field3"] = -1.79e308 * 1000;

    json_options options;
    options.nan_to_str("NaN")
           .inf_to_str("Inf");

    std::ostringstream os;
    os << pretty_print(j, options);

    json j2 = json::parse(os.str(),options);

    json expected;
    expected["field1"] = std::nan("");
    expected["field2"] = std::numeric_limits<double>::infinity();
    expected["field3"] = -std::numeric_limits<double>::infinity();

    CHECK(expected.to_string(options) == j.to_string(options));
    CHECK(expected.to_string() == j.to_string());
}

TEST_CASE("object_array empty array")
{
    std::string s = R"(
{
    "foo": []
}
    )";

    SECTION("same_line")
    {
        json j = json::parse(s);

        json_options options;
        options.object_array_line_splits(line_split_kind::same_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        std::string expected = R"({
    "foo": []
})";
        CHECK(os.str() == expected);
    }

    SECTION("new_line")
    {
        json j = json::parse(s);

        json_options options;
        options.object_array_line_splits(line_split_kind::new_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        std::string expected = R"({
    "foo": []
})";
        CHECK(os.str() == expected);
    }

    SECTION("multi_line")
    {
        json j = json::parse(s);

        json_options options;
        options.object_array_line_splits(line_split_kind::multi_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        std::string expected = R"({
    "foo": []
})";
        CHECK(os.str() == expected);
    }

}

TEST_CASE("object_array with/without line_length_limit")
{
    std::string s = R"(
{
    "foo": ["bar", "baz", [1, 2, 3]],
    "qux": [1, 2, 3, null, 123, 45.3, 342334, 234]
}
    )";

    SECTION("same_line")
    {
    std::string expected = R"({
    "foo": ["bar","baz",
        [1,2,3]
    ],
    "qux": [1,2,3,null,123,45.3,342334,234]
})";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(120)
               .spaces_around_comma(spaces_option::no_spaces)
               .object_array_line_splits(line_split_kind::same_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        CHECK(os.str() == expected);
    }

    SECTION("new_line")
    {
    std::string expected = R"({
    "foo": [
        "bar","baz",
        [1,2,3]
    ],
    "qux": [
        1,2,3,null,123,45.3,342334,234
    ]
})";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(120)
               .spaces_around_comma(spaces_option::no_spaces)
               .object_array_line_splits(line_split_kind::new_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }

    SECTION("multi_line")
    {
    std::string expected = R"({
    "foo": [
        "bar",
        "baz",
        [1,2,3]
    ],
    "qux": [
        1,
        2,
        3,
        null,
        123,
        45.3,
        342334,
        234
    ]
})";

        json j = json::parse(s);

        json_options options;
        options.spaces_around_comma(spaces_option::no_spaces)
               .object_array_line_splits(line_split_kind::multi_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }

    SECTION("same_line with line length limit")
    {
    std::string expected = R"({
    "foo": ["bar","baz",
        [1,2,3]
    ],
    "qux": [1,2,3,null,
        123,45.3,342334,
        234
    ]
})";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(20)
               .spaces_around_comma(spaces_option::no_spaces)
               .object_array_line_splits(line_split_kind::same_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }

    SECTION("new_line with line length limit") // Revisit 234
    {
    std::string expected = R"({
    "foo": [
        "bar","baz",
        [1,2,3]
    ],
    "qux": [
        1,2,3,null,123,
        45.3,342334,234
    ]
})";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(20)
               .spaces_around_comma(spaces_option::no_spaces)
               .object_array_line_splits(line_split_kind::new_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }
}

TEST_CASE("array_object with/without line_length_limit")
{
    std::string s = R"(
[
   {
       "author": "Graham Greene",
       "title": "The Comedians"
   },
   {
       "author": "Koji Suzuki",
       "title": "ring"
   },
   {
       "author": "Haruki Murakami",
       "title": "A Wild Sheep Chase"
   }
]
    )";
    SECTION("same_line")
    {
    std::string expected = R"([{"author": "Graham Greene","title": "The Comedians"},{"author": "Koji Suzuki","title": "ring"},{"author": "Haruki Murakami",
                                                                                                 "title": "A Wild Sheep Chase"}])";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(120)
               .spaces_around_comma(spaces_option::no_spaces)
               .array_object_line_splits(line_split_kind::same_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }

    SECTION("new_line")
    {
    std::string expected = R"([
    {"author": "Graham Greene","title": "The Comedians"},
    {"author": "Koji Suzuki","title": "ring"},
    {"author": "Haruki Murakami","title": "A Wild Sheep Chase"}
])";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(120)
               .spaces_around_comma(spaces_option::no_spaces)
               .array_object_line_splits(line_split_kind::new_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }

    SECTION("multi_line (default)")
    {
    std::string expected = R"([
    {
        "author": "Graham Greene",
        "title": "The Comedians"
    },
    {
        "author": "Koji Suzuki",
        "title": "ring"
    },
    {
        "author": "Haruki Murakami",
        "title": "A Wild Sheep Chase"
    }
])";

        json j = json::parse(s);

        json_options options;
        options.spaces_around_comma(spaces_option::no_spaces);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }
    SECTION("same_line with line length limit")
    {
    std::string expected = R"([{"author": "Graham Greene",
  "title": "The Comedians"},
    {"author": "Koji Suzuki",
     "title": "ring"},
    {"author": "Haruki Murakami",
     "title": "A Wild Sheep Chase"}])";

        json j = json::parse(s);

        json_options options;
        options.line_length_limit(20)
               .spaces_around_comma(spaces_option::no_spaces)
               .array_object_line_splits(line_split_kind::same_line);

        std::ostringstream os;
        os << pretty_print(j, options);

        //std::cout << pretty_print(j, options) << "\n";
        CHECK(os.str() == expected);
    }
    SECTION("new_line with line length limit")
    {
    std::string expected = R"([
    {"author": "Graham Greene",
     "title": "The Comedians"},
    {"author": "Koji Suzuki",
     "title": "ring"},
    {"author": "Haruki Murakami",
     "title": "A Wild Sheep Chase"}
])";
        json j = json::parse(s);

        json_options options;
        options.line_length_limit(20)
               .spaces_around_comma(spaces_option::no_spaces)
               .array_object_line_splits(line_split_kind::new_line);

        std::ostringstream os;
        os << pretty_print(j, options);
        CHECK(os.str() == expected);

        //std::cout << pretty_print(j, options) << "\n";
    }
}

TEST_CASE("json_options tests")
{
    SECTION("pad_inside_array_brackets")
    {
        std::string s = R"({
    "foo": [ 1, 2 ]
})";

        json j = json::parse(s);

        json_options options;
        options.pad_inside_array_brackets(true);

        std::ostringstream os;
        j.dump(os, options, indenting::indent);
        CHECK(os.str() == s);
    }
    SECTION("pad_inside_object_braces")
    {
        std::string s = R"([{ "foo": 1 }])";

        json j = json::parse(s);

        json_options options;
        options.pad_inside_object_braces(true)
               .array_object_line_splits(line_split_kind::same_line);

        std::ostringstream os;
        j.dump(os, options, indenting::indent);
        CHECK(os.str() == s);
    }
}
