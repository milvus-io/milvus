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

namespace Thrift.Transports.Client
{
    //TODO: check for correct work

    // ReSharper disable once InconsistentNaming
    public class TTlsSocketClientTransport : TStreamClientTransport
    {
        private readonly X509Certificate2 _certificate;
        private readonly RemoteCertificateValidationCallback _certValidator;
        private readonly IPAddress _host;
        private readonly bool _isServer;
        private readonly LocalCertificateSelectionCallback _localCertificateSelectionCallback;
        private readonly int _port;
        private readonly SslProtocols _sslProtocols;
        private TcpClient _client;
        private SslStream _secureStream;
        private int _timeout;

        public TTlsSocketClientTransport(TcpClient client, X509Certificate2 certificate, bool isServer = false,
            RemoteCertificateValidationCallback certValidator = null,
            LocalCertificateSelectionCallback localCertificateSelectionCallback = null,
            SslProtocols sslProtocols = SslProtocols.Tls12)
        {
            _client = client;
            _certificate = certificate;
            _certValidator = certValidator;
            _localCertificateSelectionCallback = localCertificateSelectionCallback;
            _sslProtocols = sslProtocols;
            _isServer = isServer;

            if (isServer && certificate == null)
            {
                throw new ArgumentException("TTlsSocketClientTransport needs certificate to be used for server",
                    "certificate");
            }

            if (IsOpen)
            {
                InputStream = client.GetStream();
                OutputStream = client.GetStream();
            }
        }

        public TTlsSocketClientTransport(IPAddress host, int port, string certificatePath,
            RemoteCertificateValidationCallback certValidator = null,
            LocalCertificateSelectionCallback localCertificateSelectionCallback = null,
            SslProtocols sslProtocols = SslProtocols.Tls12)
            : this(host, port, 0,
                new X509Certificate2(certificatePath),
                certValidator,
                localCertificateSelectionCallback,
                sslProtocols)
        {
        }

        public TTlsSocketClientTransport(IPAddress host, int port,
            X509Certificate2 certificate = null,
            RemoteCertificateValidationCallback certValidator = null,
            LocalCertificateSelectionCallback localCertificateSelectionCallback = null,
            SslProtocols sslProtocols = SslProtocols.Tls12)
            : this(host, port, 0,
                certificate,
                certValidator,
                localCertificateSelectionCallback,
                sslProtocols)
        {
        }

        public TTlsSocketClientTransport(IPAddress host, int port, int timeout,
            X509Certificate2 certificate,
            RemoteCertificateValidationCallback certValidator = null,
            LocalCertificateSelectionCallback localCertificateSelectionCallback = null,
            SslProtocols sslProtocols = SslProtocols.Tls12)
        {
            _host = host;
            _port = port;
            _timeout = timeout;
            _certificate = certificate;
            _certValidator = certValidator;
            _localCertificateSelectionCallback = localCertificateSelectionCallback;
            _sslProtocols = sslProtocols;

            InitSocket();
        }

        public int Timeout
        {
            set { _client.ReceiveTimeout = _client.SendTimeout = _timeout = value; }
        }

        public TcpClient TcpClient => _client;

        public IPAddress Host => _host;

        public int Port => _port;

        public override bool IsOpen
        {
            get
            {
                if (_client == null)
                {
                    return false;
                }

                return _client.Connected;
            }
        }

        private void InitSocket()
        {
            _client = new TcpClient();
            _client.ReceiveTimeout = _client.SendTimeout = _timeout;
            _client.Client.NoDelay = true;
        }

        private bool DefaultCertificateValidator(object sender, X509Certificate certificate, X509Chain chain,
            SslPolicyErrors sslValidationErrors)
        {
            return sslValidationErrors == SslPolicyErrors.None;
        }

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.AlreadyOpen, "Socket already connected");
            }

            if (_host == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "Cannot open null host");
            }

            if (_port <= 0)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "Cannot open without port");
            }

            if (_client == null)
            {
                InitSocket();
            }

            if (_client != null)
            {
                await _client.ConnectAsync(_host, _port);
                await SetupTlsAsync();
            }
        }

        public async Task SetupTlsAsync()
        {
            var validator = _certValidator ?? DefaultCertificateValidator;

            if (_localCertificateSelectionCallback != null)
            {
                _secureStream = new SslStream(_client.GetStream(), false, validator, _localCertificateSelectionCallback);
            }
            else
            {
                _secureStream = new SslStream(_client.GetStream(), false, validator);
            }

            try
            {
                if (_isServer)
                {
                    // Server authentication
                    await
                        _secureStream.AuthenticateAsServerAsync(_certificate, _certValidator != null, _sslProtocols,
                            true);
                }
                else
                {
                    // Client authentication
                    var certs = _certificate != null
                        ? new X509CertificateCollection {_certificate}
                        : new X509CertificateCollection();

                    await _secureStream.AuthenticateAsClientAsync(_host.ToString(), certs, _sslProtocols, true);
                }
            }
            catch (Exception)
            {
                Close();
                throw;
            }

            InputStream = _secureStream;
            OutputStream = _secureStream;
        }

        public override void Close()
        {
            base.Close();
            if (_client != null)
            {
                _client.Dispose();
                _client = null;
            }

            if (_secureStream != null)
            {
                _secureStream.Dispose();
                _secureStream = null;
            }
        }
    }
}