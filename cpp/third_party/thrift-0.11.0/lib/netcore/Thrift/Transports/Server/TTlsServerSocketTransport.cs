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
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Transports.Client;

namespace Thrift.Transports.Server
{
    // ReSharper disable once InconsistentNaming
    public class TTlsServerSocketTransport : TServerTransport
    {
        private readonly RemoteCertificateValidationCallback _clientCertValidator;
        private readonly int _clientTimeout = 0;
        private readonly LocalCertificateSelectionCallback _localCertificateSelectionCallback;
        private readonly int _port;
        private readonly X509Certificate2 _serverCertificate;
        private readonly SslProtocols _sslProtocols;
        private readonly bool _useBufferedSockets;
        private TcpListener _server;

        public TTlsServerSocketTransport(int port, X509Certificate2 certificate)
            : this(port, false, certificate)
        {
        }

        public TTlsServerSocketTransport(
            int port,
            bool useBufferedSockets,
            X509Certificate2 certificate,
            RemoteCertificateValidationCallback clientCertValidator = null,
            LocalCertificateSelectionCallback localCertificateSelectionCallback = null,
            SslProtocols sslProtocols = SslProtocols.Tls12)
        {
            if (!certificate.HasPrivateKey)
            {
                throw new TTransportException(TTransportException.ExceptionType.Unknown,
                    "Your server-certificate needs to have a private key");
            }

            _port = port;
            _serverCertificate = certificate;
            _useBufferedSockets = useBufferedSockets;
            _clientCertValidator = clientCertValidator;
            _localCertificateSelectionCallback = localCertificateSelectionCallback;
            _sslProtocols = sslProtocols;

            try
            {
                // Create server socket
                _server = new TcpListener(IPAddress.Any, _port);
                _server.Server.NoDelay = true;
            }
            catch (Exception)
            {
                _server = null;
                throw new TTransportException($"Could not create ServerSocket on port {port}.");
            }
        }

        public override void Listen()
        {
            // Make sure accept is not blocking
            if (_server != null)
            {
                try
                {
                    _server.Start();
                }
                catch (SocketException sx)
                {
                    throw new TTransportException($"Could not accept on listening socket: {sx.Message}");
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
                var client = await _server.AcceptTcpClientAsync();
                client.SendTimeout = client.ReceiveTimeout = _clientTimeout;

                //wrap the client in an SSL Socket passing in the SSL cert
                var tTlsSocket = new TTlsSocketClientTransport(client, _serverCertificate, true, _clientCertValidator,
                    _localCertificateSelectionCallback, _sslProtocols);

                await tTlsSocket.SetupTlsAsync();

                if (_useBufferedSockets)
                {
                    var trans = new TBufferedClientTransport(tTlsSocket);
                    return trans;
                }

                return tTlsSocket;
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
                    throw new TTransportException($"WARNING: Could not close server socket: {ex}");
                }

                _server = null;
            }
        }
    }
}