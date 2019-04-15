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
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;
using Thrift.Protocols.Entities;

namespace Thrift
{
    // ReSharper disable once InconsistentNaming
    public class TMultiplexedProcessor : ITAsyncProcessor
    {
        //TODO: Localization

        private readonly Dictionary<string, ITAsyncProcessor> _serviceProcessorMap =
            new Dictionary<string, ITAsyncProcessor>();

        public async Task<bool> ProcessAsync(TProtocol iprot, TProtocol oprot)
        {
            return await ProcessAsync(iprot, oprot, CancellationToken.None);
        }

        public async Task<bool> ProcessAsync(TProtocol iprot, TProtocol oprot, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<bool>(cancellationToken);
            }

            try
            {
                var message = await iprot.ReadMessageBeginAsync(cancellationToken);

                if ((message.Type != TMessageType.Call) && (message.Type != TMessageType.Oneway))
                {
                    await FailAsync(oprot, message, TApplicationException.ExceptionType.InvalidMessageType,
                        "Message exType CALL or ONEWAY expected", cancellationToken);
                    return false;
                }

                // Extract the service name
                var index = message.Name.IndexOf(TMultiplexedProtocol.Separator, StringComparison.Ordinal);
                if (index < 0)
                {
                    await FailAsync(oprot, message, TApplicationException.ExceptionType.InvalidProtocol,
                        $"Service name not found in message name: {message.Name}. Did you forget to use a TMultiplexProtocol in your client?",
                        cancellationToken);
                    return false;
                }

                // Create a new TMessage, something that can be consumed by any TProtocol
                var serviceName = message.Name.Substring(0, index);
                ITAsyncProcessor actualProcessor;
                if (!_serviceProcessorMap.TryGetValue(serviceName, out actualProcessor))
                {
                    await FailAsync(oprot, message, TApplicationException.ExceptionType.InternalError,
                        $"Service name not found: {serviceName}. Did you forget to call RegisterProcessor()?",
                        cancellationToken);
                    return false;
                }

                // Create a new TMessage, removing the service name
                var newMessage = new TMessage(
                    message.Name.Substring(serviceName.Length + TMultiplexedProtocol.Separator.Length),
                    message.Type,
                    message.SeqID);

                // Dispatch processing to the stored processor
                return
                    await
                        actualProcessor.ProcessAsync(new StoredMessageProtocol(iprot, newMessage), oprot,
                            cancellationToken);
            }
            catch (IOException)
            {
                return false; // similar to all other processors
            }
        }

        public void RegisterProcessor(string serviceName, ITAsyncProcessor processor)
        {
            if (_serviceProcessorMap.ContainsKey(serviceName))
            {
                throw new InvalidOperationException(
                    $"Processor map already contains processor with name: '{serviceName}'");
            }

            _serviceProcessorMap.Add(serviceName, processor);
        }

        private async Task FailAsync(TProtocol oprot, TMessage message, TApplicationException.ExceptionType extype,
            string etxt, CancellationToken cancellationToken)
        {
            var appex = new TApplicationException(extype, etxt);

            var newMessage = new TMessage(message.Name, TMessageType.Exception, message.SeqID);

            await oprot.WriteMessageBeginAsync(newMessage, cancellationToken);
            await appex.WriteAsync(oprot, cancellationToken);
            await oprot.WriteMessageEndAsync(cancellationToken);
            await oprot.Transport.FlushAsync(cancellationToken);
        }

        private class StoredMessageProtocol : TProtocolDecorator
        {
            readonly TMessage _msgBegin;

            public StoredMessageProtocol(TProtocol protocol, TMessage messageBegin)
                : base(protocol)
            {
                _msgBegin = messageBegin;
            }

            public override async Task<TMessage> ReadMessageBeginAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return await Task.FromCanceled<TMessage>(cancellationToken);
                }

                return _msgBegin;
            }
        }
    }
}