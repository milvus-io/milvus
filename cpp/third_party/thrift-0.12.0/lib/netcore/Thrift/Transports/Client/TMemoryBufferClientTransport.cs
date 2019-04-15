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

using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transports.Client
{
    // ReSharper disable once InconsistentNaming
    public class TMemoryBufferClientTransport : TClientTransport
    {
        private readonly MemoryStream _byteStream;
        private bool _isDisposed;

        public TMemoryBufferClientTransport()
        {
            _byteStream = new MemoryStream();
        }

        public TMemoryBufferClientTransport(byte[] buf)
        {
            _byteStream = new MemoryStream(buf);
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
            /** do nothing **/
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int length,
            CancellationToken cancellationToken)
        {
            return await _byteStream.ReadAsync(buffer, offset, length, cancellationToken);
        }

        public override async Task WriteAsync(byte[] buffer, CancellationToken cancellationToken)
        {
            await _byteStream.WriteAsync(buffer, 0, buffer.Length, cancellationToken);
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            await _byteStream.WriteAsync(buffer, offset, length, cancellationToken);
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public byte[] GetBuffer()
        {
            return _byteStream.ToArray();
        }

        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _byteStream?.Dispose();
                }
            }
            _isDisposed = true;
        }
    }
}