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
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

namespace c_glib OneWayTest
namespace java onewaytest
namespace cpp onewaytest
namespace rb Onewaytest
namespace perl OneWayTest
namespace csharp Onewaytest
namespace js OneWayTest
namespace st OneWayTest
namespace py OneWayTest
namespace py.twisted OneWayTest
namespace go onewaytest
namespace php OneWayTest
namespace delphi Onewaytest
namespace cocoa OneWayTest
namespace lua OneWayTest
namespace xsd test (uri = 'http://thrift.apache.org/ns/OneWayTest')
namespace netcore ThriftAsync.OneWayTest

// a minimal Thrift service, for use in OneWayHTTPTtest.cpp
service OneWayService {
  void roundTripRPC(),
  oneway void oneWayRPC()
}
