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

namespace Thrift.Transports.Client
{
    // ReSharper disable once InconsistentNaming
    public class TSocketClientTransport : TStreamClientTransport
    {
        private bool _isDisposed;

        public TSocketClientTransport(TcpClient client)
        {
            TcpClient = client ?? throw new ArgumentNullException(nameof(client));

            if (IsOpen)
            {
                InputStream = client.GetStream();
                OutputStream = client.GetStream();
            }
        }

        public TSocketClientTransport(IPAddress host, int port)
            : this(host, port, 0)
        {
        }

        public TSocketClientTransport(IPAddress host, int port, int timeout)
        {
            Host = host;
            Port = port;

            TcpClient = new TcpClient();
            TcpClient.ReceiveTimeout = TcpClient.SendTimeout = timeout;
            TcpClient.Client.NoDelay = true;
        }

        public TcpClient TcpClient { get; private set; }
        public IPAddress Host { get; }
        public int Port { get; }

        public int Timeout
        {
            set
            {
                if (TcpClient != null)
                {
                    TcpClient.ReceiveTimeout = TcpClient.SendTimeout = value;
                }
            }
        }

        public override bool IsOpen
        {
            get
            {
                if (TcpClient == null)
                {
                    return false;
                }

                return TcpClient.Connected;
            }
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            if (IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.AlreadyOpen, "Socket already connected");
            }

            if (Port <= 0)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "Cannot open without port");
            }

            if (TcpClient == null)
            {
                throw new InvalidOperationException("Invalid or not initialized tcp client");
            }

            await TcpClient.ConnectAsync(Host, Port);

            InputStream = TcpClient.GetStream();
            OutputStream = TcpClient.GetStream();
        }

        public override void Close()
        {
            base.Close();

            if (TcpClient != null)
            {
                TcpClient.Dispose();
                TcpClient = null;
            }
        }

        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    TcpClient?.Dispose();

                    base.Dispose(disposing);
                }
            }
            _isDisposed = true;
        }
    }
}