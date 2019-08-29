// Copyright 2017 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>

using namespace jsoncons;

void jsonpatch_add_add()
{
    // Apply a JSON Patch

    json doc = R"(
        { "foo": "bar"}
    )"_json;

    json doc2 = doc;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )"_json;

    std::error_code ec;
    jsonpatch::apply_patch(doc, patch, ec);

    std::cout << "(1)\n" << pretty_print(doc) << std::endl;

    // Create a JSON Patch

    auto patch2 = jsonpatch::from_diff(doc2,doc);

    std::cout << "(2)\n" << pretty_print(patch2) << std::endl;

    jsonpatch::apply_patch(doc2,patch2,ec);

    std::cout << "(3)\n" << pretty_print(doc2) << std::endl;
}

void jsonpatch_add_add_add_failed1()
{
    json target = R"(
        { "foo": "bar"}
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )"_json;

    try
    {
        jsonpatch::apply_patch(target, patch);
    }
    catch (const jsonpatch::jsonpatch_error& e)
    {
        std::cout << "(1) " << e.what() << std::endl;
        std::cout << "(2) " << target << std::endl;
    }
}

void jsonpatch_add_add_add_failed2()
{
    json target = R"(
        { "foo": "bar"}
    )"_json;

    json patch = R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )"_json;

    std::error_code ec;
    jsonpatch::apply_patch(target, patch, ec);

    std::cout << "(1) " << std::error_code(ec).message() << std::endl;
    std::cout << "(2) " << target << std::endl;
}

void create_a_json_patch()
{
    json source = R"(
        {"/": 9, "foo": "bar"}
    )"_json;

    json target = R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )"_json;

    auto patch = jsonpatch::from_diff(source, target);

    std::error_code ec;
    jsonpatch::apply_patch(source, patch, ec);

    std::cout << "(1)\n" << pretty_print(patch) << std::endl;
    std::cout << "(2)\n" << pretty_print(source) << std::endl;
}

void jsonpatch_examples()
{
    std::cout << "\njsonpatch examples\n\n";
    create_a_json_patch();
    jsonpatch_add_add();
    jsonpatch_add_add_add_failed2();
    jsonpatch_add_add_add_failed1();
    std::cout << std::endl;
}

