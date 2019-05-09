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

var assert = require('assert');
var thrift = require('thrift');
var helpers = require('./helpers');
var ThriftTest = require('./gen-nodejs/ThriftTest');
var ThriftTestDriver = require('./test_driver').ThriftTestDriver;

// createXHRConnection createWSConnection
var connection = thrift.createXHRConnection("localhost", 9090, {
    transport: helpers.transports['buffered'],
    protocol: helpers.protocols['json'],
    path: '/test'
});

connection.on('error', function(err) {
    assert(false, err);
});

// Uncomment the following line to start a websockets connection
// connection.open();

// createWSClient createXHRClient
var client = thrift.createXHRClient(ThriftTest, connection);

ThriftTestDriver(client, function (status) {
    console.log('Browser:', status);
});
