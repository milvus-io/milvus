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
 /* jshint -W100 */

/*
 * JavaScript test suite for double constants inside
 * DebugProtoTest.thrift. These tests will run against Normal (-gen js)
 * Apache Thrift interfaces.
 *
 * Synchronous blocking calls should be identical in both
 * Normal and jQuery interfaces. All synchronous tests belong
 * here.
 *
 * Asynchronous success callbacks passed as the last parameter
 * of an RPC call should be identical in both Normal and jQuery
 * interfaces. Async success tests belong here.
 *
 * Asynchronous exception processing is different in Normal
 * and jQuery interfaces. Such tests belong in the test-nojq.js
 * or test-jq.js files respectively. jQuery specific XHR object
 * tests also belong in test-jq.js. Do not create any jQuery
 * dependencies in this file or in test-nojq.js
 *
 * To compile client code for this test use:
 *      $ thrift -gen js ThriftTest.thrift
 *      $ thrift -gen js DebugProtoTest.thrift
 *
 * See also:
 * ++ test-nojq.js for "-gen js" only tests
 */

// double assertion threshold
const EPSILON = 0.0000001;

// Work around for old API used by QUnitAdapter of jsTestDriver
if (typeof QUnit.log == 'function') {
  // When using real QUnit (fron PhantomJS) log failures to console
  QUnit.log(function(details) {
    if (!details.result) {
      console.log('======== FAIL ========');
      console.log('TestName: ' + details.name);
      if (details.message) console.log(details.message);
      console.log('Expected: ' + details.expected);
      console.log('Actual  : ' + details.actual);
      console.log('======================');
    }
  });
}

QUnit.module('Double rendering');

  QUnit.test('Double (rendering)', function(assert) {
    console.log('Double rendering test -- starts');
    const EXPECTED_DOUBLE_ASSIGNED_TO_INT_CONSTANT = 1;
    const EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT = -100;
    const EXPECTED_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT = 9223372036854775807;
    const EXPECTED_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT = -9223372036854775807;
    const EXPECTED_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS = 3.14159265359;
    const EXPECTED_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE = 1000000.1;
    const EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE = -1000000.1;
    const EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE = 1.7e+308;
    const EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE = 9223372036854775816.43;
    const EXPECTED_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE = -1.7e+308;
    const EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE = -9223372036854775816.43;
    assert.ok(
        Math.abs(EXPECTED_DOUBLE_ASSIGNED_TO_INT_CONSTANT - DOUBLE_ASSIGNED_TO_INT_CONSTANT_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT -
            DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT -
            DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT -
            DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS -
            DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE -
            DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE -
            DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_DOUBLE -
            DOUBLE_ASSIGNED_TO_LARGE_DOUBLE_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE -
            DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_SMALL_DOUBLE -
            DOUBLE_ASSIGNED_TO_SMALL_DOUBLE_TEST) <= EPSILON);
    assert.ok(
        Math.abs(
            EXPECTED_DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE -
            DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE_TEST) <= EPSILON);
    assert.equal(typeof DOUBLE_ASSIGNED_TO_INT_CONSTANT_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_NEGATIVE_INT_CONSTANT_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_LARGEST_INT_CONSTANT_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_SMALLEST_INT_CONSTANT_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_DOUBLE_WITH_MANY_DECIMALS_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_FRACTIONAL_DOUBLE_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_NEGATIVE_FRACTIONAL_DOUBLE_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_LARGE_DOUBLE_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_LARGE_FRACTIONAL_DOUBLE_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_SMALL_DOUBLE_TEST, 'number');
    assert.equal(typeof DOUBLE_ASSIGNED_TO_NEGATIVE_BUT_LARGE_FRACTIONAL_DOUBLE_TEST, 'number');
    const EXPECTED_DOUBLE_LIST =
        [1,-100,100,9223372036854775807,-9223372036854775807,3.14159265359,1000000.1,-1000000.1,1.7e+308,-1.7e+308,
            9223372036854775816.43,-9223372036854775816.43];
    assert.equal(DOUBLE_LIST_TEST.length, EXPECTED_DOUBLE_LIST.length);
    for (let i = 0; i < EXPECTED_DOUBLE_LIST.length; ++i) {
           assert.ok(Math.abs(EXPECTED_DOUBLE_LIST[i] - DOUBLE_LIST_TEST[i]) <= EPSILON);
    }
    console.log('Double rendering test -- ends');
  });

