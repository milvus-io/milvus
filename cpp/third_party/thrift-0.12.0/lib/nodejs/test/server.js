#!/usr/bin/env node

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

const fs = require("fs");
const path = require("path");
const thrift = require("../lib/thrift");
const program = require("commander");
const helpers = require("./helpers");

program
  .option(
    "-p, --protocol <protocol>",
    "Set thrift protocol (binary|compact|json)",
    "binary"
  )
  .option(
    "-t, --transport <transport>",
    "Set thrift transport (buffered|framed|http)",
    "buffered"
  )
  .option("--ssl", "use ssl transport")
  .option("--port <port>", "Set thrift server port", 9090)
  .option("--domain-socket <path>", "Set thift server unix domain socket")
  .option(
    "-t, --type <type>",
    "Select server type (http|multiplex|tcp|websocket)",
    "tcp"
  )
  .option("--callback", "test with callback style functions")
  .option("--es6", "Use es6 code")
  .option("--es5", "Use es5 code")
  .parse(process.argv);

const ThriftTest = require(`./${helpers.genPath}/ThriftTest`);
const SecondService = require(`./${helpers.genPath}/SecondService`);
const { ThriftTestHandler } = require("./test_handler");

const port = program.port;
const domainSocket = program.domainSocket;
const ssl = program.ssl;

let type = program.type;
if (program.transport === "http") {
  program.transport = "buffered";
  type = "http";
}

let options = {
  transport: helpers.transports[program.transport],
  protocol: helpers.protocols[program.protocol]
};

if (type === "http" || type === "websocket") {
  options.handler = ThriftTestHandler;
  options.processor = ThriftTest;

  options = {
    services: { "/test": options },
    cors: {
      "*": true
    }
  };
}

let processor;
if (type === "multiplex") {
  const SecondServiceHandler = {
    secondtestString: function(thing, result) {
      console.log('testString("' + thing + '")');
      result(null, 'testString("' + thing + '")');
    }
  };

  processor = new thrift.MultiplexedProcessor();

  processor.registerProcessor(
    "ThriftTest",
    new ThriftTest.Processor(ThriftTestHandler)
  );

  processor.registerProcessor(
    "SecondService",
    new SecondService.Processor(SecondServiceHandler)
  );
}

if (ssl) {
  if (
    type === "tcp" ||
    type === "multiplex" ||
    type === "http" ||
    type === "websocket"
  ) {
    options.tls = {
      key: fs.readFileSync(path.resolve(__dirname, "server.key")),
      cert: fs.readFileSync(path.resolve(__dirname, "server.crt"))
    };
  }
}

let server;
if (type === "tcp") {
  server = thrift.createServer(ThriftTest, ThriftTestHandler, options);
} else if (type === "multiplex") {
  server = thrift.createMultiplexServer(processor, options);
} else if (type === "http" || type === "websocket") {
  server = thrift.createWebServer(options);
}

if (domainSocket) {
  server.listen(domainSocket);
} else if (
  type === "tcp" ||
  type === "multiplex" ||
  type === "http" ||
  type === "websocket"
) {
  server.listen(port);
}
