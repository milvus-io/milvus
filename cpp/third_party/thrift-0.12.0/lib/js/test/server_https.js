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

//This HTTP server is designed to server the test.html browser
//  based JavaScript test page (which must be in the current directory).
//  This server also supplies the Thrift based test service, which depends
//  on the standard ThriftTest.thrift IDL service (which must be compiled
//  for Node and browser based JavaScript in ./gen-nodejs and ./gen-js
//  respectively). The current directory must also include the browser
//  support libraries for test.html (jquery.js, qunit.js and qunit.css
//  in ./build/js/lib).

const fs = require('fs');
const thrift = require('../../nodejs/lib/thrift');
const es6Mode = process.argv.includes('--es6');
const genFolder = es6Mode ? 'gen-nodejs-es6' : 'gen-nodejs';
const ThriftTestSvc = require(`./${genFolder}/ThriftTest.js`);
const ThriftTestHandler = require('./test_handler').ThriftTestHandler;

//Setup the I/O stack options for the ThriftTest service
const ThriftTestSvcOpt = {
  transport: thrift.TBufferedTransport,
  protocol: thrift.TJSONProtocol,
  processor: ThriftTestSvc,
  handler: ThriftTestHandler
};

const ThriftWebServerOptions = {
  files: __dirname,
  tls: {
     key: fs.readFileSync('../../../test/keys/server.key'),
     cert: fs.readFileSync('../../../test/keys/server.crt')
  },
  services: {
    '/service': ThriftTestSvcOpt
  }
};

const server = thrift.createWebServer(ThriftWebServerOptions);
const port = es6Mode ? 8090 : 8091;
server.listen(port);
console.log(`Serving files from: ${__dirname}`);
console.log(`Http/Thrift Server (ES6 mode ${es6Mode}) running on port: ${port}`);
