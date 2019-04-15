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
#define BOOST_TEST_MODULE AnnotationTest
#include <boost/test/unit_test.hpp>
#include "gen-cpp/AnnotationTest_types.h"
#include <ostream>
#include <sstream>

// Normally thrift generates ostream operators, however
// with the annotation "cpp.customostream" one can tell the
// compiler they are going to provide their own, and not
// emit operator << or printTo().

std::ostream& operator<<(std::ostream& os, const ostr_custom& osc)
{
  os << "{ bar = " << osc.bar << "; }";
  return os;
}

BOOST_AUTO_TEST_SUITE(BOOST_TEST_MODULE)

BOOST_AUTO_TEST_CASE(test_cpp_compiler_generated_ostream_operator)
{
  ostr_default def;
  def.__set_bar(10);

  std::stringstream ssd;
  ssd << def;
  BOOST_CHECK_EQUAL(ssd.str(), "ostr_default(bar=10)");
}

BOOST_AUTO_TEST_CASE(test_cpp_customostream_uses_consuming_application_definition)
{
  ostr_custom cus;
  cus.__set_bar(10);

  std::stringstream csd;
  csd << cus;
  BOOST_CHECK_EQUAL(csd.str(), "{ bar = 10; }");
}

/**
 * Disabled; see THRIFT-1567 - not sure what it is supposed to do
BOOST_AUTO_TEST_CASE(test_cpp_type) {
  // Check that the "cpp.type" annotation changes "struct foo" to "DenseFoo"
  // This won't compile if the annotation is mishandled
  DenseFoo foo;
  foo.__set_bar(5);
}
 */

BOOST_AUTO_TEST_SUITE_END()
