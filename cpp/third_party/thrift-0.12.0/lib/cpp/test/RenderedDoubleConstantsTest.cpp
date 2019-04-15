/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
#define EPSILON 0.0000001
#include <typeindex>
#include <typeinfo>
#include <vector>

#include "gen-cpp/DoubleConstantsTest_constants.h"
using namespace thrift::test;

#define BOOST_TEST_MODULE RenderedDoubleConstantsTest
#include <boost/test/unit_test.hpp>

BOOST_AUTO_TEST_SUITE(RenderedDoubleConstantsTest)

BOOST_AUTO_TEST_CASE(test_rendered_double_constants) {
    const double EXPECTED_DOUBLE_ASSIGNED_TO_INT_CONSTANT = 1.0;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT = -100.0;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT = 9223372036854775807.0;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT = -9223372036854775807.0;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS = 3.14159265359;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE = 1000000.1;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE = -1000000.1;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE = 1.7e+308;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE = 9223372036854775816.43;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE = -1.7e+308;
    const double EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE = -9223372036854775816.43;
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_INT_CONSTANT_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_INT_CONSTANT, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_LARGE_DOUBLE_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_SMALL_DOUBLE_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE, EPSILON);
    BOOST_CHECK_CLOSE(
        g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE_TEST,
        EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE, EPSILON);
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_INT_CONSTANT_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_INT_CONSTANT).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_LARGE_DOUBLE_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_SMALL_DOUBLE_TEST).hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE).hash_code());
    BOOST_CHECK(
        typeid(g_DoubleConstantsTest_constants.DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE_TEST)
            .hash_code() ==
        typeid(EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE).hash_code());
}

BOOST_AUTO_TEST_CASE(test_rendered_double_list) {
    const std::vector<double> EXPECTED_DOUBLE_LIST{1.0,-100.0,100.0,9223372036854775807.0,-9223372036854775807.0,
        3.14159265359,1000000.1,-1000000.1,1.7e+308,-1.7e+308,9223372036854775816.43,-9223372036854775816.43};
    BOOST_CHECK_EQUAL(g_DoubleConstantsTest_constants.DOUBLE_LIST_TEST.size(), EXPECTED_DOUBLE_LIST.size());
    for (unsigned int i = 0; i < EXPECTED_DOUBLE_LIST.size(); ++i) {
        BOOST_CHECK_CLOSE(g_DoubleConstantsTest_constants.DOUBLE_LIST_TEST[i], EXPECTED_DOUBLE_LIST[i], EPSILON);
    }
}

BOOST_AUTO_TEST_SUITE_END()
