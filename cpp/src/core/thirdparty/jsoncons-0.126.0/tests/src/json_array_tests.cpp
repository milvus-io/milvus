// Copyright 2013 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <list>

using namespace jsoncons;

TEST_CASE("test_initializer_list_of_integers")
{
    json arr = json::array{0,1,2,3};
    CHECK(arr.is_array());
    CHECK(arr.size() == 4);
    for (size_t i = 0; i < arr.size(); ++i)
    {
        CHECK(i == arr[i].as<size_t>());
    }
}

TEST_CASE("test_assignment_to_initializer_list")
{
    json arr = json::array({0,1,2,3});

    arr = json::array{0,1,2,3};
    CHECK(arr.is_array());
    CHECK(arr.size() == 4);
    for (size_t i = 0; i < arr.size(); ++i)
    {
        CHECK(i == arr[i].as<size_t>());
    }
}

TEST_CASE("test_assignment_to_initializer_list2")
{
    json val;
    val["data"]["id"] = json::array{0,1,2,3,4,5,6,7};
    val["data"]["item"] = json::array{json::array{2},
                                      json::array{4,5,2,3},
                                      json::array{4},
                                      json::array{4,5,2,3},
                                      json::array{2},
                                      json::array{4,5,3},
                                      json::array{2},
                                      json::array{4,3}};

    CHECK(val["data"]["item"][0][0] == json(2));
    CHECK(val["data"]["item"][1][0] == json(4));
    CHECK(val["data"]["item"][2][0] == json(4));
    CHECK(val["data"]["item"][3][0] == json(4));
    CHECK(val["data"]["item"][4][0] == json(2));
    CHECK(val["data"]["item"][5][0] == json(4));
    CHECK(val["data"]["item"][6][0] == json(2));
    CHECK(val["data"]["item"][7][0] == json(4));
    CHECK(val["data"]["item"][7][1] == json(3));
}

TEST_CASE("test_assignment_to_initializer_list3")
{
    json val;
    val["data"]["id"] = json::array{0,1,2,3,4,5,6,7};
    val["data"]["item"] = json::array{json::object{{"first",1},{"second",2}}};

    json expected_id = json::parse(R"(
[0,1,2,3,4,5,6,7]
    )");

    json expected_item = json::parse(R"(
    [{"first":1,"second":2}]
    )");

    CHECK(expected_id == val["data"]["id"]);
    CHECK(expected_item == val["data"]["item"]);
}

TEST_CASE("test_assign_initializer_list_of_object")
{
    json arr = json::array();

    json transaction;
    transaction["Debit"] = 10000;

    arr = json::array{transaction};
    CHECK(arr.is_array());
    CHECK(arr.size() == 1);
    CHECK(arr[0] == transaction);
}

TEST_CASE("test_initializer_list_of_objects")
{
    json book1;
    book1["author"] = "Smith";
    book1["title"] = "Old Bones";

    json book2;
    book2["author"] = "Jones";
    book2["title"] = "New Things";

    json arr = json::array{book1, book2};
    CHECK(arr.is_array());
    CHECK(arr.size() == 2);

    CHECK(book1 == arr[0]);
    CHECK(book2 == arr[1]);
}

TEST_CASE("test_array_constructor")
{
    json arr = json::array();
    arr.resize(10,10.0);
    CHECK(arr.is_array());
    CHECK(arr.size() == 10);
    CHECK(arr[0].as<double>() == Approx(10.0).epsilon(0.0000001));
}

TEST_CASE("test_make_array")
{
    json arr = json::array();
    CHECK(arr.size() == 0);
    arr.resize(10,10.0);
    CHECK(arr.is_array());
    CHECK(arr.size() == 10);
    CHECK(arr[0].as<double>() == Approx(10.0).epsilon(0.0000001));

}

TEST_CASE("test_add_element_to_array")
{
    json arr = json::array();
    CHECK(arr.is_array());
    CHECK(arr.is<json::array>());
    arr.push_back("Toronto");
    arr.push_back("Vancouver");
    arr.insert(arr.array_range().begin(),"Montreal");

    CHECK(arr.size() == 3);

    CHECK(arr[0].as<std::string>() == std::string("Montreal"));
    CHECK(arr[1].as<std::string>() == std::string("Toronto"));
    CHECK(arr[2].as<std::string>() == std::string("Vancouver"));
}

TEST_CASE("test_emplace_element_to_array")
{
    json a = json::array();
    CHECK(a.is_array());
    CHECK(a.is<json::array>());
    a.emplace_back("Toronto");
    a.emplace_back("Vancouver");
    a.emplace(a.array_range().begin(),"Montreal");

    CHECK(a.size() == 3);

    CHECK(a[0].as<std::string>() == std::string("Montreal"));
    CHECK(a[1].as<std::string>() == std::string("Toronto"));
    CHECK(a[2].as<std::string>() == std::string("Vancouver"));
}

TEST_CASE("test_array_add_pos")
{
    json arr = json::array();
    CHECK(arr.is_array());
    CHECK(arr.is<json::array>());
    arr.push_back("Toronto");
    arr.push_back("Vancouver");
    arr.insert(arr.array_range().begin(),"Montreal");

    CHECK(arr.size() == 3);

    CHECK(arr[0].as<std::string>() == std::string("Montreal"));
    CHECK(arr[1].as<std::string>() == std::string("Toronto"));
    CHECK(arr[2].as<std::string>() == std::string("Vancouver"));
}

TEST_CASE("test_array_erase_range")
{
    json arr = json::array();
    CHECK(arr.is_array());
    CHECK(arr.is<json::array>());
    arr.push_back("Toronto");
    arr.push_back("Vancouver");
    arr.insert(arr.array_range().begin(),"Montreal");

    CHECK(arr.size() == 3);

    arr.erase(arr.array_range().begin()+1,arr.array_range().end());

    CHECK(arr.size() == 1);
    CHECK(arr[0].as<std::string>() == std::string("Montreal"));
}

TEST_CASE("test_reserve_array_capacity")
{
    json cities = json::array();
    CHECK(cities.is_array());
    CHECK(cities.is<json::array>());
    cities.reserve(10);  // storage is allocated
    CHECK(cities.capacity() == 10);
    CHECK(cities.size() == 0);

    cities.push_back("Toronto");
    CHECK(cities.is_array());
    CHECK(cities.is<json::array>());
    CHECK(cities.capacity() == 10);
    CHECK(cities.size() == 1);
    cities.push_back("Vancouver");
    cities.insert(cities.array_range().begin(),"Montreal");
    CHECK(cities.capacity() == 10);
    CHECK(cities.size() == 3);
}

TEST_CASE("test make_array()")
{
    json j = json::make_array();
    CHECK(j.is_array());
    CHECK(j.size() == 0);
    j.emplace_back("Toronto");
    j.emplace_back("Vancouver");
    j.emplace(j.array_range().begin(),"Montreal");
    CHECK(j[0].as<std::string>() == std::string("Montreal"));
    CHECK(j[1].as<std::string>() == std::string("Toronto"));
    CHECK(j[2].as<std::string>() == std::string("Vancouver"));
}

TEST_CASE("test_one_dim_array")
{
    basic_json<char,sorted_policy,std::allocator<char>> a = basic_json<char,sorted_policy,std::allocator<char>>::make_array<1>(10,0);
    CHECK(a.size() == 10);
    CHECK(a[0].as<int64_t>() == 0);
    a[1] = 1;
    a[2] = 2;
    CHECK(a[1].as<int64_t>() == 1);
    CHECK(a[2].as<int64_t>() == 2);
    CHECK(a[9].as<int64_t>() == 0);

    CHECK(a[1].as<long long>() == 1);
    CHECK(a[2].as<long long>() == 2);
    CHECK(a[9].as<long long>() == 0);
}

TEST_CASE("test_two_dim_array")
{
    json a = json::make_array<2>(3,4,0);
    CHECK(a.size() == 3);
    a[0][0] = "Tenor";
    a[0][1] = "ATM vol";
    a[0][2] = "25-d-MS";
    a[0][3] = "25-d-RR";
    a[1][0] = "1Y";
    a[1][1] = 0.20;
    a[1][2] = 0.009;
    a[1][3] = -0.006;
    a[2][0] = "2Y";
    a[2][1] = 0.18;
    a[2][2] = 0.009;
    a[2][3] = -0.005;

    CHECK(a[0][0].as<std::string>() ==std::string("Tenor"));
    CHECK(a[2][3].as<double>() == Approx(-0.005).epsilon(0.00000001));

    CHECK(a[0][0].as<std::string>() ==std::string("Tenor"));
    CHECK(a[2][3].as<double>() == Approx(-0.005).epsilon(0.00000001));
}

TEST_CASE("test_three_dim_array")
{
    json a = json::make_array<3>(4,3,2,0);
    CHECK(a.size() == 4);
    a[0][2][0] = 2;
    a[0][2][1] = 3;

    CHECK(a[0][2][0].as<int64_t>() == 2);
    CHECK(a[0][2][1].as<int64_t>() == 3);
    CHECK(a[3][2][1].as<int64_t>() == 0);

    CHECK(a[0][2][0].as<long long>() == 2);
    CHECK(a[0][2][1].as<long long>() == 3);
    CHECK(a[3][2][1].as<long long>() == 0);
}

TEST_CASE("test_array_assign_vector")
{
    std::vector<std::string> vec;
    vec.push_back("Toronto");
    vec.push_back("Vancouver");
    vec.push_back("Montreal");

    json val;
    val = vec;

    CHECK(val.size() == 3);
    CHECK(val[0].as<std::string>() ==std::string("Toronto"));
    CHECK(val[1].as<std::string>() ==std::string("Vancouver"));
    CHECK(val[2].as<std::string>() ==std::string("Montreal"));

}

TEST_CASE("test_array_assign_vector_of_bool")
{
    std::vector<bool> vec;
    vec.push_back(true);
    vec.push_back(false);
    vec.push_back(true);

    json val;
    val = vec;

    CHECK(val.size() == 3);
    CHECK(val[0].as<bool>() == true);
    CHECK(val[1].as<bool>() == false);
    CHECK(val[2].as<bool>() == true);

}

TEST_CASE("test_array_add_null")
{
    json a = json::array();
    a.push_back(jsoncons::null_type());
    a.push_back(json::null());
    CHECK(a[0].is_null());
    CHECK(a[1].is_null());
}

TEST_CASE("test_array_from_container")
{
    std::vector<int> vec;
    vec.push_back(10);
    vec.push_back(20);
    vec.push_back(30);

    json val1 = vec;
    REQUIRE(vec.size() == 3);
    CHECK(vec[0] == 10);
    CHECK(vec[1] == 20);
    CHECK(vec[2] == 30);

    std::list<double> list;
    list.push_back(10.5);
    list.push_back(20.5);
    list.push_back(30.5);

    json val2 = list;
    REQUIRE(val2.size() == 3);
    CHECK(val2[0].as<double>() == Approx(10.5).epsilon(0.000001));
    CHECK(val2[1].as<double>() == Approx(20.5).epsilon(0.000001));
    CHECK(val2[2].as<double>() == Approx(30.5).epsilon(0.000001));
}

TEST_CASE("test_array_as_vector_of_double")
{
    std::string s("[0,1.1,2,3.1]");
    json val = json::parse(s);

    std::vector<double> v = val.as<std::vector<double>>(); 
    CHECK(v.size() == 4);
    CHECK(v[0] == Approx(0.0).epsilon(0.0000000001));
    CHECK(v[1] == Approx(1.1).epsilon(0.0000000001));
    CHECK(v[2] == Approx(2.0).epsilon(0.0000000001));
    CHECK(v[3] == Approx(3.1).epsilon(0.0000000001));
}

TEST_CASE("test_array_as_vector_of_bool")
{
    std::string s("[true,false,true]");
    json val = json::parse(s);

    std::vector<bool> v = val.as<std::vector<bool>>(); 
    CHECK(v.size() == 3);
    CHECK(v[0] == true);
    CHECK(v[1] == false);
    CHECK(v[2] == true);
}

TEST_CASE("test_array_as_vector_of_string")
{
    std::string s("[\"Hello\",\"World\"]");
    json val = json::parse(s);

    std::vector<std::string> v = val.as<std::vector<std::string>>(); 
    CHECK(v.size() == 2);
    CHECK(v[0] == "Hello");
    CHECK(v[1] == "World");
}

TEST_CASE("test_array_as_vector_of_char")
{
    std::string s("[20,30]");
    json val = json::parse(s);

    std::vector<char> v = val.as<std::vector<char>>(); 
    CHECK(v.size() == 2);
    CHECK(v[0] == 20);
    CHECK(v[1] == 30);
}

TEST_CASE("test_array_as_vector_of_int")
{
    std::string s("[0,1,2,3]");
    json val = json::parse(s);

    std::vector<int> v = val.as<std::vector<int>>(); 
    CHECK(v.size() == 4);
    CHECK(v[0]==0);
    CHECK(v[1]==1);
    CHECK(v[2]==2);
    CHECK(v[3]==3);

    std::vector<unsigned int> v1 = val.as<std::vector<unsigned int>>(); 
    CHECK(v1.size() == 4);
    CHECK(v1[0]==0);
    CHECK(v1[1]==1);
    CHECK(v1[2]==2);
    CHECK(v1[3]==3);

    std::vector<long> v2 = val.as<std::vector<long>>(); 
    CHECK(v2.size() == 4);
    CHECK(v2[0]==0);
    CHECK(v2[1]==1);
    CHECK(v2[2]==2);
    CHECK(v2[3]==3);

    std::vector<unsigned long> v3 = val.as<std::vector<unsigned long>>(); 
    CHECK(v3.size() == 4);
    CHECK(v3[0]==0);
    CHECK(v3[1]==1);
    CHECK(v3[2]==2);
    CHECK(v3[3]==3);

    std::vector<long long> v4 = val.as<std::vector<long long>>(); 
    CHECK(v4.size() == 4);
    CHECK(v4[0]==0);
    CHECK(v4[1]==1);
    CHECK(v4[2]==2);
    CHECK(v4[3]==3);

    std::vector<unsigned long long> v5 = val.as<std::vector<unsigned long long>>(); 
    CHECK(v5.size() == 4);
    CHECK(v5[0]==0);
    CHECK(v5[1]==1);
    CHECK(v5[2]==2);
    CHECK(v5[3]==3);
}

TEST_CASE("test_array_as_vector_of_int_on_proxy")
{
    std::string s("[0,1,2,3]");
    json val = json::parse(s);
    json root;
    root["val"] = val;
    std::vector<int> v = root["val"].as<std::vector<int>>();
    CHECK(v.size() == 4);
    CHECK(v[0]==0);
    CHECK(v[1]==1);
    CHECK(v[2]==2);
    CHECK(v[3]==3);
}

