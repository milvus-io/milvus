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

using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols.Entities;

namespace Thrift.Protocols.Utilities
{
    // ReSharper disable once InconsistentNaming
    public static class TProtocolUtil
    {
        public static async Task SkipAsync(TProtocol protocol, TType type, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            protocol.IncrementRecursionDepth();
            try
            {
                switch (type)
                {
                    case TType.Bool:
                        await protocol.ReadBoolAsync(cancellationToken);
                        break;
                    case TType.Byte:
                        await protocol.ReadByteAsync(cancellationToken);
                        break;
                    case TType.I16:
                        await protocol.ReadI16Async(cancellationToken);
                        break;
                    case TType.I32:
                        await protocol.ReadI32Async(cancellationToken);
                        break;
                    case TType.I64:
                        await protocol.ReadI64Async(cancellationToken);
                        break;
                    case TType.Double:
                        await protocol.ReadDoubleAsync(cancellationToken);
                        break;
                    case TType.String:
                        // Don't try to decode the string, just skip it.
                        await protocol.ReadBinaryAsync(cancellationToken);
                        break;
                    case TType.Struct:
                        await protocol.ReadStructBeginAsync(cancellationToken);
                        while (true)
                        {
                            var field = await protocol.ReadFieldBeginAsync(cancellationToken);
                            if (field.Type == TType.Stop)
                            {
                                break;
                            }
                            await SkipAsync(protocol, field.Type, cancellationToken);
                            await protocol.ReadFieldEndAsync(cancellationToken);
                        }
                        await protocol.ReadStructEndAsync(cancellationToken);
                        break;
                    case TType.Map:
                        var map = await protocol.ReadMapBeginAsync(cancellationToken);
                        for (var i = 0; i < map.Count; i++)
                        {
                            await SkipAsync(protocol, map.KeyType, cancellationToken);
                            await SkipAsync(protocol, map.ValueType, cancellationToken);
                        }
                        await protocol.ReadMapEndAsync(cancellationToken);
                        break;
                    case TType.Set:
                        var set = await protocol.ReadSetBeginAsync(cancellationToken);
                        for (var i = 0; i < set.Count; i++)
                        {
                            await SkipAsync(protocol, set.ElementType, cancellationToken);
                        }
                        await protocol.ReadSetEndAsync(cancellationToken);
                        break;
                    case TType.List:
                        var list = await protocol.ReadListBeginAsync(cancellationToken);
                        for (var i = 0; i < list.Count; i++)
                        {
                            await SkipAsync(protocol, list.ElementType, cancellationToken);
                        }
                        await protocol.ReadListEndAsync(cancellationToken);
                        break;
                    default:
                        throw new TProtocolException(TProtocolException.INVALID_DATA, "Unknown data type " + type.ToString("d"));
                }
            }
            finally
            {
                protocol.DecrementRecursionDepth();
            }
        }
    }
}
