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
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transports.Client
{
    // ReSharper disable once InconsistentNaming
    public class THttpClientTransport : TClientTransport
    {
        private readonly X509Certificate[] _certificates;
        private readonly Uri _uri;

        // Timeouts in milliseconds
        private int _connectTimeout = 30000;
        private HttpClient _httpClient;
        private Stream _inputStream;

        private bool _isDisposed;
        private MemoryStream _outputStream = new MemoryStream();

        public THttpClientTransport(Uri u, IDictionary<string, string> customHeaders)
            : this(u, Enumerable.Empty<X509Certificate>(), customHeaders)
        {
        }

        public THttpClientTransport(Uri u, IEnumerable<X509Certificate> certificates,
            IDictionary<string, string> customHeaders)
        {
            _uri = u;
            _certificates = (certificates ?? Enumerable.Empty<X509Certificate>()).ToArray();
            CustomHeaders = customHeaders;

            // due to current bug with performance of Dispose in netcore https://github.com/dotnet/corefx/issues/8809
            // this can be switched to default way (create client->use->dispose per flush) later
            _httpClient = CreateClient();
        }

        public IDictionary<string, string> CustomHeaders { get; }

        public int ConnectTimeout
        {
            set { _connectTimeout = value; }
        }

        public override bool IsOpen => true;

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override void Close()
        {
            if (_inputStream != null)
            {
                _inputStream.Dispose();
                _inputStream = null;
            }

            if (_outputStream != null)
            {
                _outputStream.Dispose();
                _outputStream = null;
            }

            if (_httpClient != null)
            {
                _httpClient.Dispose();
                _httpClient = null;
            }
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int length,
            CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<int>(cancellationToken);
            }

            if (_inputStream == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "No request has been sent");
            }

            try
            {
                var ret = await _inputStream.ReadAsync(buffer, offset, length, cancellationToken);

                if (ret == -1)
                {
                    throw new TTransportException(TTransportException.ExceptionType.EndOfFile, "No more data available");
                }

                return ret;
            }
            catch (IOException iox)
            {
                throw new TTransportException(TTransportException.ExceptionType.Unknown, iox.ToString());
            }
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            await _outputStream.WriteAsync(buffer, offset, length, cancellationToken);
        }

        private HttpClient CreateClient()
        {
            var handler = new HttpClientHandler();
            handler.ClientCertificates.AddRange(_certificates);

            var httpClient = new HttpClient(handler);

            if (_connectTimeout > 0)
            {
                httpClient.Timeout = TimeSpan.FromSeconds(_connectTimeout);
            }

            httpClient.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/x-thrift"));
            httpClient.DefaultRequestHeaders.UserAgent.Add(new ProductInfoHeaderValue("THttpClientTransport", "0.11.0"));

            if (CustomHeaders != null)
            {
                foreach (var item in CustomHeaders)
                {
                    httpClient.DefaultRequestHeaders.Add(item.Key, item.Value);
                }
            }

            return httpClient;
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            try
            {
                try
                {
                    if (_outputStream.CanSeek)
                    {
                        _outputStream.Seek(0, SeekOrigin.Begin);
                    }

                    using (var outStream = new StreamContent(_outputStream))
                    {
                        var msg = await _httpClient.PostAsync(_uri, outStream, cancellationToken);

                        msg.EnsureSuccessStatusCode();

                        if (_inputStream != null)
                        {
                            _inputStream.Dispose();
                            _inputStream = null;
                        }

                        _inputStream = await msg.Content.ReadAsStreamAsync();
                        if (_inputStream.CanSeek)
                        {
                            _inputStream.Seek(0, SeekOrigin.Begin);
                        }
                    }
                }
                catch (IOException iox)
                {
                    throw new TTransportException(TTransportException.ExceptionType.Unknown, iox.ToString());
                }
                catch (WebException wx)
                {
                    throw new TTransportException(TTransportException.ExceptionType.Unknown,
                        "Couldn't connect to server: " + wx);
                }
            }
            finally
            {
                _outputStream = new MemoryStream();
            }
        }

        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _inputStream?.Dispose();
                    _outputStream?.Dispose();
                    _httpClient?.Dispose();
                }
            }
            _isDisposed = true;
        }
    }
}