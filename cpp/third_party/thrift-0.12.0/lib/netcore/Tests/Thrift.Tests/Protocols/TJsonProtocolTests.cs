// Licensed to the Apache Software Foundation(ASF) under one
// or more contributor license agreements.See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using NSubstitute;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Transports;
using Thrift.Transports.Client;

namespace Thrift.Tests.Protocols
{
    // ReSharper disable once InconsistentNaming
    [TestClass]
    public class TJSONProtocolTests
    {
        [TestMethod]
        public void TJSONProtocol_Can_Create_Instance_Test()
        {
            var httpClientTransport = Substitute.For<THttpClientTransport>(new Uri("http://localhost"), null);

            var result = new TJSONProtocolWrapper(httpClientTransport);

            Assert.IsNotNull(result);
            Assert.IsNotNull(result.WrappedContext);
            Assert.IsNotNull(result.WrappedReader);
            Assert.IsNotNull(result.Transport);
            Assert.IsTrue(result.WrappedRecursionDepth == 0);
            Assert.IsTrue(result.WrappedRecursionLimit == TProtocol.DefaultRecursionDepth);

            Assert.IsTrue(result.Transport.Equals(httpClientTransport));
            Assert.IsTrue(result.WrappedContext.GetType().Name.Equals("JSONBaseContext", StringComparison.OrdinalIgnoreCase));
            Assert.IsTrue(result.WrappedReader.GetType().Name.Equals("LookaheadReader", StringComparison.OrdinalIgnoreCase));
        }

        private class TJSONProtocolWrapper : TJsonProtocol
        {
            public TJSONProtocolWrapper(TClientTransport trans) : base(trans)
            {
            }

            public object WrappedContext => Context;
            public object WrappedReader => Reader;
            public int WrappedRecursionDepth => RecursionDepth;
            public int WrappedRecursionLimit => RecursionLimit;
        }
    }
}
