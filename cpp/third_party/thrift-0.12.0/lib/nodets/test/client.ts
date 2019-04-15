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

import assert = require("assert");
import thrift = require("thrift");
import Thrift = thrift.Thrift;
import ThriftTest = require("./gen-nodejs/ThriftTest");
import test_driver = require("./test_driver");
import ThriftTestDriver = test_driver.ThriftTestDriver;
import ThriftTestDriverPromise = test_driver.ThriftTestDriverPromise;

// var program = require("commander");
import * as program from "commander";

program
  .option("--port <port>", "Set thrift server port number to connect", 9090)
  .option("--promise", "test with promise style functions")
  .option("--protocol", "Set thrift protocol (binary) [protocol]")
  .parse(process.argv);

var port: number = program.port;
var promise = program.promise;

var options = {
  transport: Thrift.TBufferedTransport,
  protocol: Thrift.TBinaryProtocol
};

var testDriver = promise ? ThriftTestDriverPromise : ThriftTestDriver;

var connection = thrift.createConnection("localhost", port, options);

connection.on("error", function(err: string) {
    assert(false, err);
});

var client = thrift.createClient(ThriftTest.Client, connection);
runTests();

function runTests() {
  testDriver(client, function (status: string) {
    console.log(status);
    process.exit(0);
  });
}

exports.expressoTest = function() {};
