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
    // ReSharper disable once InconsistentNaming
    public class TBufferedClientTransport : TClientTransport
    {
        private readonly int _bufSize;
        private readonly MemoryStream _inputBuffer = new MemoryStream(0);
        private readonly MemoryStream _outputBuffer = new MemoryStream(0);
        private readonly TClientTransport _transport;
        private bool _isDisposed;

        //TODO: should support only specified input transport?
        public TBufferedClientTransport(TClientTransport transport, int bufSize = 1024)
        {
            if (bufSize <= 0)
            {
                throw new ArgumentOutOfRangeException(nameof(bufSize), "Buffer size must be a positive number.");
            }

            _transport = transport ?? throw new ArgumentNullException(nameof(transport));
            _bufSize = bufSize;
        }

        public TClientTransport UnderlyingTransport
        {
            get
            {
                CheckNotDisposed();

                return _transport;
            }
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
            //TODO: investigate how it should work correctly
            CheckNotDisposed();

            ValidateBufferArgs(buffer, offset, length);

            if (!IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            if (_inputBuffer.Capacity < _bufSize)
            {
                _inputBuffer.Capacity = _bufSize;
            }

            var got = await _inputBuffer.ReadAsync(buffer, offset, length, cancellationToken);
            if (got > 0)
            {
                return got;
            }

            _inputBuffer.Seek(0, SeekOrigin.Begin);
            _inputBuffer.SetLength(_inputBuffer.Capacity);

            ArraySegment<byte> bufSegment;
            _inputBuffer.TryGetBuffer(out bufSegment);

            // investigate
            var filled = await _transport.ReadAsync(bufSegment.Array, 0, (int) _inputBuffer.Length, cancellationToken);
            _inputBuffer.SetLength(filled);

            if (filled == 0)
            {
                return 0;
            }

            return await ReadAsync(buffer, offset, length, cancellationToken);
        }

        public override async Task WriteAsync(byte[] buffer, int offset, int length, CancellationToken cancellationToken)
        {
            CheckNotDisposed();

            ValidateBufferArgs(buffer, offset, length);

            if (!IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            // Relative offset from "off" argument
            var writtenCount = 0;
            if (_outputBuffer.Length > 0)
            {
                var capa = (int) (_outputBuffer.Capacity - _outputBuffer.Length);
                var writeSize = capa <= length ? capa : length;
                await _outputBuffer.WriteAsync(buffer, offset, writeSize, cancellationToken);

                writtenCount += writeSize;
                if (writeSize == capa)
                {
                    //ArraySegment<byte> bufSegment;
                    //_outputBuffer.TryGetBuffer(out bufSegment);
                    var data = _outputBuffer.ToArray();
                    //await _transport.WriteAsync(bufSegment.Array, cancellationToken);
                    await _transport.WriteAsync(data, cancellationToken);
                    _outputBuffer.SetLength(0);
                }
            }

            while (length - writtenCount >= _bufSize)
            {
                await _transport.WriteAsync(buffer, offset + writtenCount, _bufSize, cancellationToken);
                writtenCount += _bufSize;
            }

            var remain = length - writtenCount;
            if (remain > 0)
            {
                if (_outputBuffer.Capacity < _bufSize)
                {
                    _outputBuffer.Capacity = _bufSize;
                }
                await _outputBuffer.WriteAsync(buffer, offset + writtenCount, remain, cancellationToken);
            }
        }

        public override async Task FlushAsync(CancellationToken cancellationToken)
        {
            CheckNotDisposed();

            if (!IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            if (_outputBuffer.Length > 0)
            {
                //ArraySegment<byte> bufSegment;
                var data = _outputBuffer.ToArray(); // TryGetBuffer(out bufSegment);

                await _transport.WriteAsync(data /*bufSegment.Array*/, cancellationToken);
                _outputBuffer.SetLength(0);
            }

            await _transport.FlushAsync(cancellationToken);
        }

        private void CheckNotDisposed()
        {
            if (_isDisposed)
            {
                throw new ObjectDisposedException(nameof(_transport));
            }
        }

        // IDisposable
        protected override void Dispose(bool disposing)
        {
            if (!_isDisposed)
            {
                if (disposing)
                {
                    _inputBuffer?.Dispose();
                    _outputBuffer?.Dispose();
                    _transport?.Dispose();
                }
            }
            _isDisposed = true;
        }
    }
}