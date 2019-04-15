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
using System.IO.Pipes;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transports.Server
{
    // ReSharper disable once InconsistentNaming
    public class TNamedPipeServerTransport : TServerTransport
    {
        /// <summary>
        ///     This is the address of the Pipe on the localhost.
        /// </summary>
        private readonly string _pipeAddress;

        private bool _asyncMode = true;
        private volatile bool _isPending = true;

        private NamedPipeServerStream _stream = null;

        public TNamedPipeServerTransport(string pipeAddress)
        {
            _pipeAddress = pipeAddress;
        }

        public override void Listen()
        {
            // nothing to do here
        }

        public override void Close()
        {
            if (_stream != null)
            {
                try
                {
                    //TODO: check for disconection 
                    _stream.Disconnect();
                    _stream.Dispose();
                }
                finally
                {
                    _stream = null;
                    _isPending = false;
                }
            }
        }

        public override bool IsClientPending()
        {
            return _isPending;
        }

        private void EnsurePipeInstance()
        {
            if (_stream == null)
            {
                var direction = PipeDirection.InOut;
                var maxconn = 254;
                var mode = PipeTransmissionMode.Byte;
                var options = _asyncMode ? PipeOptions.Asynchronous : PipeOptions.None;
                var inbuf = 4096;
                var outbuf = 4096;
                // TODO: security

                try
                {
                    _stream = new NamedPipeServerStream(_pipeAddress, direction, maxconn, mode, options, inbuf, outbuf);
                }
                catch (NotImplementedException) // Mono still does not support async, fallback to sync
                {
                    if (_asyncMode)
                    {
                        options &= (~PipeOptions.Asynchronous);
                        _stream = new NamedPipeServerStream(_pipeAddress, direction, maxconn, mode, options, inbuf,
                            outbuf);
                        _asyncMode = false;
                    }
                    else
                    {
                        throw;
                    }
                }
            }
        }

        protected override async Task<TClientTransport> AcceptImplementationAsync(CancellationToken cancellationToken)
        {
            try
            {
                EnsurePipeInstance();

                await _stream.WaitForConnectionAsync(cancellationToken);

                var trans = new ServerTransport(_stream);
                _stream = null; // pass ownership to ServerTransport

                //_isPending = false;

                return trans;
            }
            catch (TTransportException)
            {
                Close();
                throw;
            }
            catch (Exception e)
            {
                Close();
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, e.Message);
            }
        }

        private class ServerTransport : TClientTransport
        {
            private readonly NamedPipeServerStream _stream;

            public ServerTransport(NamedPipeServerStream stream)
            {
                _stream = stream;
            }

            public override bool IsOpen => _stream != null && _stream.IsConnected;

            public override async Task OpenAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    await Task.FromCanceled(cancellationToken);
                }
            }

            public override void Close()
            {
                _stream?.Dispose();
            }

            public override async Task<int> ReadAsync(byte[] buffer, int offset, int length,
                CancellationToken cancellationToken)
            {
                if (_stream == null)
                {
                    throw new TTransportException(TTransportException.ExceptionType.NotOpen);
                }

                return await _stream.ReadAsync(buffer, offset, length, cancellationToken);
            }

            public override async Task WriteAsync(byte[] buffer, int offset, int length,
                CancellationToken cancellationToken)
            {
                if (_stream == null)
                {
                    throw new TTransportException(TTransportException.ExceptionType.NotOpen);
                }

                await _stream.WriteAsync(buffer, offset, length, cancellationToken);
            }

            public override async Task FlushAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    await Task.FromCanceled(cancellationToken);
                }
            }

            protected override void Dispose(bool disposing)
            {
                _stream?.Dispose();
            }
        }
    }
}