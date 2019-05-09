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
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols;

namespace Thrift
{
    /// <summary>
    ///     TBaseClient.
    ///     Base client for generated clients.
    ///     Do not change this class without checking generated code (namings, etc.)
    /// </summary>
    public abstract class TBaseClient
    {
        private readonly TProtocol _inputProtocol;
        private readonly TProtocol _outputProtocol;
        private bool _isDisposed;
        private int _seqId;
        public readonly Guid ClientId = Guid.NewGuid();

        protected TBaseClient(TProtocol inputProtocol, TProtocol outputProtocol)
        {
            if (inputProtocol == null)
            {
                throw new ArgumentNullException(nameof(inputProtocol));
            }

            if (outputProtocol == null)
            {
                throw new ArgumentNullException(nameof(outputProtocol));
            }

            _inputProtocol = inputProtocol;
            _outputProtocol = outputProtocol;
        }

        public TProtocol InputProtocol => _inputProtocol;

        public TProtocol OutputProtocol => _outputProtocol;

        public int SeqId
        {
            get { return ++_seqId; }
        }

        public virtual async Task OpenTransportAsync()
        {
            await OpenTransportAsync(CancellationToken.None);
        }

        public virtual async Task OpenTransportAsync(CancellationToken cancellationToken)
        {
            if (!_inputProtocol.Transport.IsOpen)
            {
                await _inputProtocol.Transport.OpenAsync(cancellationToken);
            }

            if (!_inputProtocol.Transport.IsOpen)
            {
                await _outputProtocol.Transport.OpenAsync(cancellationToken);
            }
        }

        public void Dispose()
        {
            Dispose(true);
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _inputProtocol?.Dispose();
                    _outputProtocol?.Dispose();
                }
            }

            _isDisposed = true;
        }
    }
}
