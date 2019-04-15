/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * 'AS IS' BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

// This is the Node.js test driver for the standard Apache Thrift
// test service. The driver invokes every function defined in the
// Thrift Test service with a representative range of parameters.
//
// The ThriftTestDriver function requires a client object
// connected to a server hosting the Thrift Test service and
// supports an optional callback function which is called with
// a status message when the test is complete.

const test = require("tape");

const helpers = require("./helpers");
const ttypes = require(`./${helpers.genPath}/ThriftTest_types`);
const TException = require("thrift").Thrift.TException;
const Int64 = require("node-int64");
const testCases = require("./test-cases");

exports.ThriftTestDriver = function(client, callback) {
  test(
    "NodeJS Style Callback Client Tests",
    { skip: helpers.ecmaMode === "es6" },
    function(assert) {
      const checkRecursively = makeRecursiveCheck(assert);

      function makeAsserter(assertionFn) {
        return function(c) {
          const fnName = c[0];
          const expected = c[1];
          client[fnName](expected, function(err, actual) {
            assert.error(err, fnName + ": no callback error");
            assertionFn(actual, expected, fnName);
          });
        };
      }

      testCases.simple.forEach(
        makeAsserter(function(a, e, m) {
          if (a instanceof Int64) {
            const e64 = e instanceof Int64 ? e : new Int64(e);
            assert.deepEqual(a.buffer, e64.buffer, m);
          } else {
            assert.equal(a, e, m);
          }
        })
      );
      testCases.deep.forEach(makeAsserter(assert.deepEqual));
      testCases.deepUnordered.forEach(
        makeAsserter(makeUnorderedDeepEqual(assert))
      );

      const arr = [];
      for (let i = 0; i < 256; ++i) {
        arr[i] = 255 - i;
      }
      let buf = new Buffer(arr);
      client.testBinary(buf, function(err, response) {
        assert.error(err, "testBinary: no callback error");
        assert.equal(response.length, 256, "testBinary");
        assert.deepEqual(response, buf, "testBinary(Buffer)");
      });
      buf = new Buffer(arr);
      client.testBinary(buf.toString("binary"), function(err, response) {
        assert.error(err, "testBinary: no callback error");
        assert.equal(response.length, 256, "testBinary");
        assert.deepEqual(response, buf, "testBinary(string)");
      });

      client.testMapMap(42, function(err, response) {
        const expected = {
          "4": { "1": 1, "2": 2, "3": 3, "4": 4 },
          "-4": { "-4": -4, "-3": -3, "-2": -2, "-1": -1 }
        };
        assert.error(err, "testMapMap: no callback error");
        assert.deepEqual(expected, response, "testMapMap");
      });

      client.testStruct(testCases.out, function(err, response) {
        assert.error(err, "testStruct: no callback error");
        checkRecursively(testCases.out, response, "testStruct");
      });

      client.testNest(testCases.out2, function(err, response) {
        assert.error(err, "testNest: no callback error");
        checkRecursively(testCases.out2, response, "testNest");
      });

      client.testInsanity(testCases.crazy, function(err, response) {
        assert.error(err, "testInsanity: no callback error");
        checkRecursively(testCases.insanity, response, "testInsanity");
      });

      client.testInsanity(testCases.crazy2, function(err, response) {
        assert.error(err, "testInsanity2: no callback error");
        checkRecursively(testCases.insanity, response, "testInsanity2");
      });

      client.testException("TException", function(err, response) {
        assert.ok(
          err instanceof TException,
          "testException: correct error type"
        );
        assert.ok(!response, "testException: no response");
      });

      client.testException("Xception", function(err, response) {
        assert.ok(
          err instanceof ttypes.Xception,
          "testException: correct error type"
        );
        assert.ok(!response, "testException: no response");
        assert.equal(err.errorCode, 1001, "testException: correct error code");
        assert.equal(
          "Xception",
          err.message,
          "testException: correct error message"
        );
      });

      client.testException("no Exception", function(err, response) {
        assert.error(err, "testException: no callback error");
        assert.ok(!response, "testException: no response");
      });

      client.testOneway(0, function(err, response) {
        assert.error(err, "testOneway: no callback error");
        assert.strictEqual(response, undefined, "testOneway: void response");
      });

      checkOffByOne(function(done) {
        client.testI32(-1, function(err, response) {
          assert.error(err, "checkOffByOne: no callback error");
          assert.equal(-1, response);
          assert.end();
          done();
        });
      }, callback);
    }
  );

  // ES6 does not support callback style
  if (helpers.ecmaMode === "es6") {
    checkOffByOne(done => done(), callback);
  }
};

exports.ThriftTestDriverPromise = function(client, callback) {
  test("Promise Client Tests", function(assert) {
    const checkRecursively = makeRecursiveCheck(assert);

    function makeAsserter(assertionFn) {
      return function(c) {
        const fnName = c[0];
        const expected = c[1];
        client[fnName](expected)
          .then(function(actual) {
            assertionFn(actual, expected, fnName);
          })
          .catch(() => assert.fail("fnName"));
      };
    }

    testCases.simple.forEach(
      makeAsserter(function(a, e, m) {
        if (a instanceof Int64) {
          const e64 = e instanceof Int64 ? e : new Int64(e);
          assert.deepEqual(a.buffer, e64.buffer, m);
        } else {
          assert.equal(a, e, m);
        }
      })
    );
    testCases.deep.forEach(makeAsserter(assert.deepEqual));
    testCases.deepUnordered.forEach(
      makeAsserter(makeUnorderedDeepEqual(assert))
    );

    client
      .testStruct(testCases.out)
      .then(function(response) {
        checkRecursively(testCases.out, response, "testStruct");
      })
      .catch(() => assert.fail("testStruct"));

    client
      .testNest(testCases.out2)
      .then(function(response) {
        checkRecursively(testCases.out2, response, "testNest");
      })
      .catch(() => assert.fail("testNest"));

    client
      .testInsanity(testCases.crazy)
      .then(function(response) {
        checkRecursively(testCases.insanity, response, "testInsanity");
      })
      .catch(() => assert.fail("testInsanity"));

    client
      .testInsanity(testCases.crazy2)
      .then(function(response) {
        checkRecursively(testCases.insanity, response, "testInsanity2");
      })
      .catch(() => assert.fail("testInsanity2"));

    client
      .testException("TException")
      .then(function() {
        assert.fail("testException: TException");
      })
      .catch(function(err) {
        assert.ok(err instanceof TException);
      });

    client
      .testException("Xception")
      .then(function() {
        assert.fail("testException: Xception");
      })
      .catch(function(err) {
        assert.ok(err instanceof ttypes.Xception);
        assert.equal(err.errorCode, 1001);
        assert.equal("Xception", err.message);
      });

    client
      .testException("no Exception")
      .then(function(response) {
        assert.equal(undefined, response); //void
      })
      .catch(() => assert.fail("testException"));

    client
      .testOneway(0)
      .then(function(response) {
        assert.strictEqual(response, undefined, "testOneway: void response");
      })
      .catch(() => assert.fail("testOneway: should not reject"));

    checkOffByOne(function(done) {
      client
        .testI32(-1)
        .then(function(response) {
          assert.equal(-1, response);
          assert.end();
          done();
        })
        .catch(() => assert.fail("checkOffByOne"));
    }, callback);
  });
};

// Helper Functions
// =========================================================

function makeRecursiveCheck(assert) {
  return function(map1, map2, msg) {
    const equal = checkRecursively(map1, map2);

    assert.ok(equal, msg);

    // deepEqual doesn't work with fields using node-int64
    function checkRecursively(map1, map2) {
      if (typeof map1 !== "function" && typeof map2 !== "function") {
        if (!map1 || typeof map1 !== "object") {
          //Handle int64 types (which use node-int64 in Node.js JavaScript)
          if (
            typeof map1 === "number" &&
            typeof map2 === "object" &&
            map2.buffer &&
            map2.buffer instanceof Buffer &&
            map2.buffer.length === 8
          ) {
            const n = new Int64(map2.buffer);
            return map1 === n.toNumber();
          } else {
            return map1 == map2;
          }
        } else {
          return Object.keys(map1).every(function(key) {
            return checkRecursively(map1[key], map2[key]);
          });
        }
      }
    }
  };
}

function checkOffByOne(done, callback) {
  const retry_limit = 30;
  const retry_interval = 100;
  let test_complete = false;
  let retrys = 0;

  /**
   * redo a simple test after the oneway to make sure we aren't "off by one" --
   * if the server treated oneway void like normal void, this next test will
   * fail since it will get the void confirmation rather than the correct
   * result. In this circumstance, the client will throw the exception:
   *
   * Because this is the last test against the server, when it completes
   * the entire suite is complete by definition (the tests run serially).
   */
  done(function() {
    test_complete = true;
  });

  //We wait up to retry_limit * retry_interval for the test suite to complete
  function TestForCompletion() {
    if (test_complete && callback) {
      callback("Server successfully tested!");
    } else {
      if (++retrys < retry_limit) {
        setTimeout(TestForCompletion, retry_interval);
      } else if (callback) {
        callback(
          "Server test failed to complete after " +
            (retry_limit * retry_interval) / 1000 +
            " seconds"
        );
      }
    }
  }

  setTimeout(TestForCompletion, retry_interval);
}

function makeUnorderedDeepEqual(assert) {
  return function(actual, expected, name) {
    assert.equal(actual.length, expected.length, name);
    for (const k in actual) {
      let found = false;
      for (const k2 in expected) {
        if (actual[k] === expected[k2]) {
          found = true;
        }
      }
      if (!found) {
        assert.fail("Unexpected value " + actual[k] + " with key " + k);
      }
    }
  };
}
