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
    //TODO: unhandled exceptions, etc.

    // ReSharper disable once InconsistentNaming
    public class AsyncBaseServer : TBaseServer
    {
        private readonly int _clientWaitingDelay;
        private volatile Task _serverTask;

        public AsyncBaseServer(ITAsyncProcessor processor, TServerTransport serverTransport,
            ITProtocolFactory inputProtocolFactory, ITProtocolFactory outputProtocolFactory,
            ILoggerFactory loggerFactory, int clientWaitingDelay = 10)
            : this(new SingletonTProcessorFactory(processor), serverTransport,
                new TTransportFactory(), new TTransportFactory(),
                inputProtocolFactory, outputProtocolFactory,
                loggerFactory.CreateLogger(nameof(AsyncBaseServer)), clientWaitingDelay)
        {
        }

        public AsyncBaseServer(ITProcessorFactory itProcessorFactory, TServerTransport serverTransport,
            TTransportFactory inputTransportFactory, TTransportFactory outputTransportFactory,
            ITProtocolFactory inputProtocolFactory, ITProtocolFactory outputProtocolFactory,
            ILogger logger, int clientWaitingDelay = 10)
            : base(itProcessorFactory, serverTransport, inputTransportFactory, outputTransportFactory,
                inputProtocolFactory, outputProtocolFactory, logger)
        {
            _clientWaitingDelay = clientWaitingDelay;
        }

        public override async Task ServeAsync(CancellationToken cancellationToken)
        {
            try
            {
                // cancelation token
                _serverTask = Task.Factory.StartNew(() => StartListening(cancellationToken), TaskCreationOptions.LongRunning);
                await _serverTask;
            }
            catch (Exception ex)
            {
                Logger.LogError(ex.ToString());
            }
        }

        private async Task StartListening(CancellationToken cancellationToken)
        {
            ServerTransport.Listen();

            Logger.LogTrace("Started listening at server");

            if (ServerEventHandler != null)
            {
                await ServerEventHandler.PreServeAsync(cancellationToken);
            }

            while (!cancellationToken.IsCancellationRequested)
            {
                if (ServerTransport.IsClientPending())
                {
                    Logger.LogTrace("Waiting for client connection");

                    try
                    {
                        var client = await ServerTransport.AcceptAsync(cancellationToken);
                        await Task.Factory.StartNew(() => Execute(client, cancellationToken), cancellationToken);
                    }
                    catch (TTransportException ttx)
                    {
                        Logger.LogTrace($"Transport exception: {ttx}");

                        if (ttx.Type != TTransportException.ExceptionType.Interrupted)
                        {
                            Logger.LogError(ttx.ToString());
                        }
                    }
                }
                else
                {
                    try
                    {
                        await Task.Delay(TimeSpan.FromMilliseconds(_clientWaitingDelay), cancellationToken);
                    }
                    catch(TaskCanceledException) { }
                }
            }

            ServerTransport.Close();

            Logger.LogTrace("Completed listening at server");
        }

        public override void Stop()
        {
        }

        private async Task Execute(TClientTransport client, CancellationToken cancellationToken)
        {
            Logger.LogTrace("Started client request processing");

            var processor = ItProcessorFactory.GetAsyncProcessor(client, this);

            TClientTransport inputTransport = null;
            TClientTransport outputTransport = null;
            TProtocol inputProtocol = null;
            TProtocol outputProtocol = null;
            object connectionContext = null;

            try
            {
                inputTransport = InputTransportFactory.GetTransport(client);
                outputTransport = OutputTransportFactory.GetTransport(client);

                inputProtocol = InputProtocolFactory.GetProtocol(inputTransport);
                outputProtocol = OutputProtocolFactory.GetProtocol(outputTransport);

                if (ServerEventHandler != null)
                {
                    connectionContext = await ServerEventHandler.CreateContextAsync(inputProtocol, outputProtocol, cancellationToken);
                }

                while (!cancellationToken.IsCancellationRequested)
                {
                    if (!await inputTransport.PeekAsync(cancellationToken))
                    {
                        break;
                    }

                    if (ServerEventHandler != null)
                    {
                        await ServerEventHandler.ProcessContextAsync(connectionContext, inputTransport, cancellationToken);
                    }

                    if (!await processor.ProcessAsync(inputProtocol, outputProtocol, cancellationToken))
                    {
                        break;
                    }
                }
            }
            catch (TTransportException ttx)
            {
                Logger.LogTrace($"Transport exception: {ttx}");
            }
            catch (Exception x)
            {
                Logger.LogError($"Error: {x}");
            }

            if (ServerEventHandler != null)
            {
                await ServerEventHandler.DeleteContextAsync(connectionContext, inputProtocol, outputProtocol, cancellationToken);
            }

            inputTransport?.Close();
            outputTransport?.Close();

            Logger.LogTrace("Completed client request processing");
        }
    }
}
