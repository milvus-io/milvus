// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT License.

#define BOOST_TEST_MAIN
#define BOOST_TEST_MODULE Main
#include "inc/Test.h"

#include <string>
#include <boost/test/tree/visitor.hpp>

using namespace boost::unit_test;

class SPTAGVisitor : public test_tree_visitor
{
public:
    void visit(test_case const& test)
    {
        std::string prefix(2, '\t');
        std::cout << prefix << "Case: " << test.p_name << std::endl;
    }

    bool test_suite_start(test_suite const& suite)
    {
        std::string prefix(1, '\t');
        std::cout << prefix << "Suite: " << suite.p_name << std::endl;
        return true;
    }
};

struct GlobalFixture
{
    GlobalFixture()
    {
        SPTAGVisitor visitor;
        traverse_test_tree(framework::master_test_suite(), visitor, false);
    }

};

BOOST_TEST_GLOBAL_FIXTURE(GlobalFixture);

