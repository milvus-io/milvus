// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json_encoder.hpp>
#include <jsoncons/json_filter.hpp>
#include <jsoncons/json_reader.hpp>
#include <jsoncons/json.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <new>
#include <fstream>

using namespace jsoncons;

struct warning
{
    std::string name;
    size_t line_number;
    size_t column_number;
};

class name_fix_up_filter : public json_filter
{
    std::string member_name_;
public:
    std::vector<warning> warnings;

    name_fix_up_filter(json_content_handler& handler)
        : json_filter(handler)
    {
    }

private:
    bool do_name(const string_view_type& name,
                 const ser_context& context) override
    {
        member_name_ = std::string(name);
        if (member_name_ != "name")
        {
            this->to_handler().name(name,context);
        }
        return true;
    }

    bool do_string_value(const string_view_type& s,
                         semantic_tag tag,
                         const ser_context& context) override
    {
        if (member_name_ == "name")
        {
            size_t end_first = s.find_first_of(" \t");
            size_t start_last = s.find_first_not_of(" \t", end_first);
            this->to_handler().name("first-name", context);
            string_view_type first = s.substr(0, end_first);
            this->to_handler().string_value(first, tag, context);
            if (start_last != string_view_type::npos)
            {
                this->to_handler().name("last-name", context);
                string_view_type last = s.substr(start_last);
                this->to_handler().string_value(last, tag, context);
            }
            else
            {
                warnings.push_back(warning{std::string(s),
                                   context.line(),
                                   context.column()});
            }
        }
        else
        {
            this->to_handler().string_value(s, tag, context);
        }
        return true;
    }
};

TEST_CASE("test_filter")
{
    std::string in_file = "./input/address-book.json";
    std::string out_file = "./output/address-book-new.json";
    std::ifstream is(in_file, std::ofstream::binary);
    std::ofstream os(out_file);

    json_encoder encoder(os);
    name_fix_up_filter filter(encoder);
    json_reader reader(is, filter);
    reader.read_next();

    CHECK(1 == filter.warnings.size());
    CHECK("John" ==filter.warnings[0].name);
    CHECK(9 == filter.warnings[0].line_number);
    CHECK(26 == filter.warnings[0].column_number);
}

TEST_CASE("test_filter2")
{
    std::string in_file = "./input/address-book.json";
    std::string out_file = "./output/address-book-new.json";
    std::ifstream is(in_file, std::ofstream::binary);
    std::ofstream os(out_file);

    json_encoder encoder(os);

    name_fix_up_filter filter2(encoder);

    rename_object_member_filter filter1("email","email2",filter2);

    json_reader reader(is, filter1);
    reader.read_next();

    CHECK(1 == filter2.warnings.size());
    CHECK("John" ==filter2.warnings[0].name);
    CHECK(9 == filter2.warnings[0].line_number);
    CHECK(26 == filter2.warnings[0].column_number);
}

TEST_CASE("test_rename_name")
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
    CHECK(j["store"]["book"][0]["price"].as<double>() == Approx(31.96).epsilon(0.001));

    std::stringstream ss;
    json_encoder encoder(ss);
    rename_object_member_filter filter("price","price2",encoder);
    j.dump(filter);

    json j2 = json::parse(ss);
    CHECK(j2["store"]["book"][0]["price2"].as<double>() == Approx(31.96).epsilon(0.001));
}

TEST_CASE("test_chained_filters")
{
    ojson j = ojson::parse(R"({"first":1,"second":2,"fourth":3,"fifth":4})");

    json_decoder<ojson> decoder;

    rename_object_member_filter filter2("fifth", "fourth", decoder);
    rename_object_member_filter filter1("fourth", "third", filter2);

    j.dump(filter1);
    ojson j2 = decoder.get_result();
    CHECK(j2.size() == 4);
    CHECK(j2["first"] == 1);
    CHECK(j2["second"] == 2);
    CHECK(j2["third"] == 3);
    CHECK(j2["fourth"] == 4);
}

