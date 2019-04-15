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
 * JavaScript test suite for ThriftTest.thrift. These tests
 * will run only with jQuery (-gen js:jquery) Apache Thrift
 * interfaces. To create client code:
 *      $ thrift -gen js:jquery ThriftTest.thrift
 *
 * See also:
 * ++ test.js for generic tests
 * ++ test-nojq.js for "-gen js" only tests
 */


//////////////////////////////////
//jQuery asynchronous tests
jQuery.ajaxSetup({ timeout: 0 });

QUnit.module('jQ Async Manual');

  QUnit.test('testI32', function(assert) {
    assert.expect(2);
    const done = assert.async(2);

    const transport = new Thrift.Transport();
    const protocol = new Thrift.Protocol(transport);
    const client = new ThriftTest.ThriftTestClient(protocol);

    const jqxhr = jQuery.ajax({
      url: '/service',
      data: client.send_testI32(Math.pow(-2, 31)),
      type: 'POST',
      cache: false,
      dataType: 'text',
      success: function(res) {
        transport.setRecvBuffer(res);
        assert.equal(client.recv_testI32(), Math.pow(-2, 31));
        done();
      },
      error: function() { assert.ok(false); },
      complete: function() {
        assert.ok(true);
        done();
      }
    });
  });

  QUnit.test('testI64', function(assert) {
    assert.expect(2);
    const done = assert.async(2);

    const transport = new Thrift.Transport();
    const protocol = new Thrift.Protocol(transport);
    const client = new ThriftTest.ThriftTestClient(protocol);

    jQuery.ajax({
      url: '/service',
      //This is usually 2^61 but JS cannot represent anything over 2^52 accurately
      data: client.send_testI64(Math.pow(-2, 52)),
      type: 'POST',
      cache: false,
      dataType: 'text',
      success: function(res) {
        transport.setRecvBuffer(res);
        //This is usually 2^61 but JS cannot represent anything over 2^52 accurately
        assert.equal(client.recv_testI64(), Math.pow(-2, 52));
        done();
      },
      error: function() { assert.ok(false); },
      complete: function() {
        assert.ok(true);
        done();
      }
    });
  });


QUnit.module('jQ Async');
  QUnit.test('I32', function(assert) {
    assert.expect(3);

    const done = assert.async(3);
    client.testI32(Math.pow(2, 30), function(result) {
      assert.equal(result, Math.pow(2, 30));
      done();
    });

    const jqxhr = client.testI32(Math.pow(-2, 31), function(result) {
      assert.equal(result, Math.pow(-2, 31));
      done();
    });

    jqxhr.success(function(result) {
      assert.equal(result, Math.pow(-2, 31));
      done();
    });
  });

  QUnit.test('I64', function(assert) {
    assert.expect(4);

    const done = assert.async(4);
    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    client.testI64(Math.pow(2, 52), function(result) {
      assert.equal(result, Math.pow(2, 52));
      done();
    });

    //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
    client.testI64(Math.pow(-2, 52), function(result) {
      assert.equal(result, Math.pow(-2, 52));
      done();
    })
    .error(function(xhr, status, e) { assert.ok(false, e.message); })
    .success(function(result) {
      //This is usually 2^60 but JS cannot represent anything over 2^52 accurately
      assert.equal(result, Math.pow(-2, 52));
      done();
    })
    .complete(function() {
      assert.ok(true);
      done();
    });
  });

  QUnit.test('Xception', function(assert) {
    assert.expect(2);

    const done = assert.async(2);

    const dfd = client.testException('Xception', function(result) {
      assert.ok(false);
      done();
    })
    .error(function(xhr, status, e) {
      assert.equal(e.errorCode, 1001);
      assert.equal(e.message, 'Xception');
      done();
      $(document).ajaxError( function() { done(); } );
    });
  });
