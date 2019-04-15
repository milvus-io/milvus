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
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Thrift.Transports.Client
{
    //TODO: check for correct implementation 

    // ReSharper disable once InconsistentNaming
    public class TFramedClientTransport : TClientTransport
    {
        private const int HeaderSize = 4;
        private readonly byte[] _headerBuf = new byte[HeaderSize];
        private readonly MemoryStream _readBuffer = new MemoryStream(1024);
        private readonly TClientTransport _transport;
        private readonly MemoryStream _writeBuffer = new MemoryStream(1024);

        private bool _isDisposed;

        public TFramedClientTransport(TClientTransport transport)
        {
            _transport = transport ?? throw new ArgumentNullException(nameof(transport));

            InitWriteBuffer();
        }

        public override bool IsOpen => !_isDisposed && _transport.IsOpen;

        public override async Task OpenAsync(CancellationToken cancellationToken)
        {
            CheckNotDisposed();

            await _transport.OpenAsync(cancellationToken);
        }

        public override void Close()
        {
            CheckNotDisposed();

            _transport.Close();
        }

        public override async Task<int> ReadAsync(byte[] buffer, int offset, int length,
            CancellationToken cancellationToken)
        {
            CheckNotDisposed();

            ValidateBufferArgs(buffer, offset, length);

            if (!IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            var got = await _readBuffer.ReadAsync(buffer, offset, length, cancellationToken);
            if (got > 0)
            {
                return got;
            }

            // Read another frame of data
            await ReadFrameAsync(cancellationToken);

            return await _readBuffer.ReadAsync(buffer, offset, length, cancellationToken);
        }

        private async Task ReadFrameAsync(CancellationToken cancellationToken)
        {
            await _transport.ReadAllAsync(_headerBuf, 0, HeaderSize, cancellationToken);

            var size = DecodeFrameSize(_headerBuf);

            _readBuffer.SetLength(size);
            _readBuffer.Seek(0, SeekOrigin.Begin);

            ArraySegment<byte> bufSegment;
            _readBuffer.TryGetBuffer(out bufSegment);

            var buff = bufSegment.Array;

            await _transport.ReadAllAsync(buff, 0, size, cancellationToken);
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            CheckNotDisposed();

            ValidateBufferArgs(buffer, offset, length);

            if (!IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            if (_writeBuffer.Length + length > int.MaxValue)
            {
                await FlushAsync(cancellationToken);
            }

            await _writeBuffer.WriteAsync(buffer, offset, length, cancellationToken);
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            CheckNotDisposed();

            if (!IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            //ArraySegment<byte> bufSegment;
            //_writeBuffer.TryGetBuffer(out bufSegment);
            //var buf = bufSegment.Array;
            var buf = _writeBuffer.ToArray();

            //var len = (int)_writeBuffer.Length;
            var dataLen = (int) _writeBuffer.Length - HeaderSize;
            if (dataLen < 0)
            {
                throw new InvalidOperationException(); // logic error actually
            }

            // Inject message header into the reserved buffer space
            EncodeFrameSize(dataLen, buf);

            // Send the entire message at once
            await _transport.WriteAsync(buf, cancellationToken);

            InitWriteBuffer();

            await _transport.FlushAsync(cancellationToken);
        }

        private void InitWriteBuffer()
        {
            // Reserve space for message header to be put right before sending it out
            _writeBuffer.SetLength(HeaderSize);
            _writeBuffer.Seek(0, SeekOrigin.End);
        }

        private static void EncodeFrameSize(int frameSize, byte[] buf)
        {
            buf[0] = (byte) (0xff & (frameSize >> 24));
            buf[1] = (byte) (0xff & (frameSize >> 16));
            buf[2] = (byte) (0xff & (frameSize >> 8));
            buf[3] = (byte) (0xff & (frameSize));
        }

        private static int DecodeFrameSize(byte[] buf)
        {
            return
                ((buf[0] & 0xff) << 24) |
                ((buf[1] & 0xff) << 16) |
                ((buf[2] & 0xff) << 8) |
                (buf[3] & 0xff);
        }


        private void CheckNotDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException("TFramedClientTransport");
            }
        }

        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _readBuffer?.Dispose();
                    _writeBuffer?.Dispose();
                    _transport?.Dispose();
                }
            }
            _isDisposed = true;
        }
    }
}