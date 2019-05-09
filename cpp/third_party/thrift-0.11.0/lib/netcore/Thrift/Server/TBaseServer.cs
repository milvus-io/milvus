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
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Thrift.Protocols;
using Thrift.Transports;

namespace Thrift.Server
{
    // ReSharper disable once InconsistentNaming
    public abstract class TBaseServer
    {
        protected readonly ILogger Logger;
        protected ITProtocolFactory InputProtocolFactory;
        protected TTransportFactory InputTransportFactory;
        protected ITProcessorFactory ItProcessorFactory;
        protected ITProtocolFactory OutputProtocolFactory;
        protected TTransportFactory OutputTransportFactory;

        protected TServerEventHandler ServerEventHandler;
        protected TServerTransport ServerTransport;

        protected TBaseServer(ITProcessorFactory itProcessorFactory, TServerTransport serverTransport,
            TTransportFactory inputTransportFactory, TTransportFactory outputTransportFactory,
            ITProtocolFactory inputProtocolFactory, ITProtocolFactory outputProtocolFactory,
            ILogger logger)
        {
            if (itProcessorFactory == null) throw new ArgumentNullException(nameof(itProcessorFactory));
            if (inputTransportFactory == null) throw new ArgumentNullException(nameof(inputTransportFactory));
            if (outputTransportFactory == null) throw new ArgumentNullException(nameof(outputTransportFactory));
            if (inputProtocolFactory == null) throw new ArgumentNullException(nameof(inputProtocolFactory));
            if (outputProtocolFactory == null) throw new ArgumentNullException(nameof(outputProtocolFactory));
            if (logger == null) throw new ArgumentNullException(nameof(logger));

            ItProcessorFactory = itProcessorFactory;
            ServerTransport = serverTransport;
            InputTransportFactory = inputTransportFactory;
            OutputTransportFactory = outputTransportFactory;
            InputProtocolFactory = inputProtocolFactory;
            OutputProtocolFactory = outputProtocolFactory;
            Logger = logger;
        }

        public void SetEventHandler(TServerEventHandler seh)
        {
            ServerEventHandler = seh;
        }

        public TServerEventHandler GetEventHandler()
        {
            return ServerEventHandler;
        }

        public abstract void Stop();

        public virtual void Start()
        {
            // do nothing
        }

        public virtual async Task ServeAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }
    }
}