/**
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

using System;
using System.Text;
using Thrift.Transport;
using System.Collections.Generic;

namespace Thrift.Protocol
{

    /// <summary>
    /// TMultiplexedProtocol is a protocol-independent concrete decorator that allows a Thrift
    /// client to communicate with a multiplexing Thrift server, by prepending the service name
    /// to the function name during function calls.
    /// <para/>
    /// NOTE: THIS IS NOT TO BE USED BY SERVERS.
    /// On the server, use TMultiplexedProcessor to handle requests from a multiplexing client.
    /// <para/>
    /// This example uses a single socket transport to invoke two services:
    /// <code>
    ///     TSocket transport = new TSocket("localhost", 9090);
    ///     transport.open();
    ///
    ///     TBinaryProtocol protocol = new TBinaryProtocol(transport);
    ///
    ///     TMultiplexedProtocol mp = new TMultiplexedProtocol(protocol, "Calculator");
    ///     Calculator.Client service = new Calculator.Client(mp);
    ///
    ///     TMultiplexedProtocol mp2 = new TMultiplexedProtocol(protocol, "WeatherReport");
    ///     WeatherReport.Client service2 = new WeatherReport.Client(mp2);
    ///
    ///     System.out.println(service.add(2,2));
    ///     System.out.println(service2.getTemperature());
    /// </code>
    /// </summary>
    public class TMultiplexedProtocol : TProtocolDecorator
    {

        /// <summary>
        /// Used to delimit the service name from the function name.
        /// </summary>
        public static string SEPARATOR = ":";

        private string ServiceName;

        /// <summary>
        /// Wrap the specified protocol, allowing it to be used to communicate with a
        /// multiplexing server.  The <paramref name="serviceName"/> is required as it is
        /// prepended to the message header so that the multiplexing server can broker
        /// the function call to the proper service.
        /// </summary>
        /// <param name="protocol">Your communication protocol of choice, e.g. <see cref="TBinaryProtocol"/>.</param>
        /// <param name="serviceName">The service name of the service communicating via this protocol.</param>
        public TMultiplexedProtocol(TProtocol protocol, string serviceName)
            : base(protocol)
        {
            ServiceName = serviceName;
        }

        /// <summary>
        /// Prepends the service name to the function name, separated by TMultiplexedProtocol.SEPARATOR.
        /// </summary>
        /// <param name="tMessage">The original message.</param>
        public override void WriteMessageBegin(TMessage tMessage)
        {
            switch (tMessage.Type)
            {
                case TMessageType.Call:
                case TMessageType.Oneway:
                    base.WriteMessageBegin(new TMessage(
                        ServiceName + SEPARATOR + tMessage.Name,
                        tMessage.Type,
                        tMessage.SeqID));
                    break;

                default:
                    base.WriteMessageBegin(tMessage);
                    break;
            }
        }
    }
}
