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
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Transports.Client;

namespace Thrift.Transports.Server
{
    // ReSharper disable once InconsistentNaming
    public class TServerSocketTransport : TServerTransport
    {
        private readonly int _clientTimeout;
        private readonly int _port;
        private readonly bool _useBufferedSockets;
        private TcpListener _server;

        public TServerSocketTransport(TcpListener listener)
            : this(listener, 0)
        {
        }

        public TServerSocketTransport(TcpListener listener, int clientTimeout)
        {
            _server = listener;
            _clientTimeout = clientTimeout;
        }

        public TServerSocketTransport(int port)
            : this(port, 0)
        {
        }

        public TServerSocketTransport(int port, int clientTimeout)
            : this(port, clientTimeout, false)
        {
        }

        public TServerSocketTransport(int port, int clientTimeout, bool useBufferedSockets)
        {
            _port = port;
            _clientTimeout = clientTimeout;
            _useBufferedSockets = useBufferedSockets;
            try
            {
                // Make server socket
                _server = new TcpListener(IPAddress.Any, _port);
                _server.Server.NoDelay = true;
            }
            catch (Exception)
            {
                _server = null;
                throw new TTransportException("Could not create ServerSocket on port " + port + ".");
            }
        }

        public override void Listen()
        {
            // Make sure not to block on accept
            if (_server != null)
            {
                try
                {
                    _server.Start();
                }
                catch (SocketException sx)
                {
                    throw new TTransportException("Could not accept on listening socket: " + sx.Message);
                }
            }
        }

        public override bool IsClientPending()
        {
            return _server.Pending();
        }

        protected override async Task<TClientTransport> AcceptImplementationAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<TClientTransport>(cancellationToken);
            }

            if (_server == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "No underlying server socket.");
            }

            try
            {
                TSocketClientTransport tSocketTransport = null;
                var tcpClient = await _server.AcceptTcpClientAsync();

                try
                {
                    tSocketTransport = new TSocketClientTransport(tcpClient)
                    {
                        Timeout = _clientTimeout
                    };

                    if (_useBufferedSockets)
                    {
                        return new TBufferedClientTransport(tSocketTransport);
                    }

                    return tSocketTransport;
                }
                catch (Exception)
                {
                    if (tSocketTransport != null)
                    {
                        tSocketTransport.Dispose();
                    }
                    else //  Otherwise, clean it up ourselves.
                    {
                        ((IDisposable) tcpClient).Dispose();
                    }

                    throw;
                }
            }
            catch (Exception ex)
            {
                throw new TTransportException(ex.ToString());
            }
        }

        public override void Close()
        {
            if (_server != null)
            {
                try
                {
                    _server.Stop();
                }
                catch (Exception ex)
                {
                    throw new TTransportException("WARNING: Could not close server socket: " + ex);
                }
                _server = null;
            }
        }
    }
}