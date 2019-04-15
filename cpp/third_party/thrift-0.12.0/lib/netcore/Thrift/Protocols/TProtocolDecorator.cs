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
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols.Entities;

namespace Thrift.Protocols
{
    // ReSharper disable once InconsistentNaming
    /// <summary>
    ///     TProtocolDecorator forwards all requests to an enclosed TProtocol instance,
    ///     providing a way to author concise concrete decorator subclasses.While it has
    ///     no abstract methods, it is marked abstract as a reminder that by itself,
    ///     it does not modify the behaviour of the enclosed TProtocol.
    /// </summary>
    public abstract class TProtocolDecorator : TProtocol
    {
        private readonly TProtocol _wrappedProtocol;

        protected TProtocolDecorator(TProtocol protocol)
            : base(protocol.Transport)
        {
            _wrappedProtocol = protocol ?? throw new ArgumentNullException(nameof(protocol));
        }

        public override async Task WriteMessageBeginAsync(TMessage message, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteMessageBeginAsync(message, cancellationToken);
        }

        public override async Task WriteMessageEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteMessageEndAsync(cancellationToken);
        }

        public override async Task WriteStructBeginAsync(TStruct @struct, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteStructBeginAsync(@struct, cancellationToken);
        }

        public override async Task WriteStructEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteStructEndAsync(cancellationToken);
        }

        public override async Task WriteFieldBeginAsync(TField field, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteFieldBeginAsync(field, cancellationToken);
        }

        public override async Task WriteFieldEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteFieldEndAsync(cancellationToken);
        }

        public override async Task WriteFieldStopAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteFieldStopAsync(cancellationToken);
        }

        public override async Task WriteMapBeginAsync(TMap map, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteMapBeginAsync(map, cancellationToken);
        }

        public override async Task WriteMapEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteMapEndAsync(cancellationToken);
        }

        public override async Task WriteListBeginAsync(TList list, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteListBeginAsync(list, cancellationToken);
        }

        public override async Task WriteListEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteListEndAsync(cancellationToken);
        }

        public override async Task WriteSetBeginAsync(TSet set, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteSetBeginAsync(set, cancellationToken);
        }

        public override async Task WriteSetEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteSetEndAsync(cancellationToken);
        }

        public override async Task WriteBoolAsync(bool b, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteBoolAsync(b, cancellationToken);
        }

        public override async Task WriteByteAsync(sbyte b, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteByteAsync(b, cancellationToken);
        }

        public override async Task WriteI16Async(short i16, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteI16Async(i16, cancellationToken);
        }

        public override async Task WriteI32Async(int i32, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteI32Async(i32, cancellationToken);
        }

        public override async Task WriteI64Async(long i64, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteI64Async(i64, cancellationToken);
        }

        public override async Task WriteDoubleAsync(double d, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteDoubleAsync(d, cancellationToken);
        }

        public override async Task WriteStringAsync(string s, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteStringAsync(s, cancellationToken);
        }

        public override async Task WriteBinaryAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            await _wrappedProtocol.WriteBinaryAsync(bytes, cancellationToken);
        }

        public override async Task<TMessage> ReadMessageBeginAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadMessageBeginAsync(cancellationToken);
        }

        public override async Task ReadMessageEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.ReadMessageEndAsync(cancellationToken);
        }

        public override async Task<TStruct> ReadStructBeginAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadStructBeginAsync(cancellationToken);
        }

        public override async Task ReadStructEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.ReadStructEndAsync(cancellationToken);
        }

        public override async Task<TField> ReadFieldBeginAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadFieldBeginAsync(cancellationToken);
        }

        public override async Task ReadFieldEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.ReadFieldEndAsync(cancellationToken);
        }

        public override async Task<TMap> ReadMapBeginAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadMapBeginAsync(cancellationToken);
        }

        public override async Task ReadMapEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.ReadMapEndAsync(cancellationToken);
        }

        public override async Task<TList> ReadListBeginAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadListBeginAsync(cancellationToken);
        }

        public override async Task ReadListEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.ReadListEndAsync(cancellationToken);
        }

        public override async Task<TSet> ReadSetBeginAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadSetBeginAsync(cancellationToken);
        }

        public override async Task ReadSetEndAsync(CancellationToken cancellationToken)
        {
            await _wrappedProtocol.ReadSetEndAsync(cancellationToken);
        }

        public override async Task<bool> ReadBoolAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadBoolAsync(cancellationToken);
        }

        public override async Task<sbyte> ReadByteAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadByteAsync(cancellationToken);
        }

        public override async Task<short> ReadI16Async(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadI16Async(cancellationToken);
        }

        public override async Task<int> ReadI32Async(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadI32Async(cancellationToken);
        }

        public override async Task<long> ReadI64Async(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadI64Async(cancellationToken);
        }

        public override async Task<double> ReadDoubleAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadDoubleAsync(cancellationToken);
        }

        public override async Task<string> ReadStringAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadStringAsync(cancellationToken);
        }

        public override async Task<byte[]> ReadBinaryAsync(CancellationToken cancellationToken)
        {
            return await _wrappedProtocol.ReadBinaryAsync(cancellationToken);
        }
    }
}