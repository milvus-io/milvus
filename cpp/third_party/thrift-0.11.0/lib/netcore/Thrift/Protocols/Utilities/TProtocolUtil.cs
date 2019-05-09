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

namespace Thrift.Protocols
{
    // ReSharper disable once InconsistentNaming
    public static class TProtocolUtil
    {
        public static async Task SkipAsync(TProtocol prot, TType type, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            prot.IncrementRecursionDepth();
            try
            {
                switch (type)
                {
                    case TType.Bool:
                        await prot.ReadBoolAsync(cancellationToken);
                        break;
                    case TType.Byte:
                        await prot.ReadByteAsync(cancellationToken);
                        break;
                    case TType.I16:
                        await prot.ReadI16Async(cancellationToken);
                        break;
                    case TType.I32:
                        await prot.ReadI32Async(cancellationToken);
                        break;
                    case TType.I64:
                        await prot.ReadI64Async(cancellationToken);
                        break;
                    case TType.Double:
                        await prot.ReadDoubleAsync(cancellationToken);
                        break;
                    case TType.String:
                        // Don't try to decode the string, just skip it.
                        await prot.ReadBinaryAsync(cancellationToken);
                        break;
                    case TType.Struct:
                        await prot.ReadStructBeginAsync(cancellationToken);
                        while (true)
                        {
                            var field = await prot.ReadFieldBeginAsync(cancellationToken);
                            if (field.Type == TType.Stop)
                            {
                                break;
                            }
                            await SkipAsync(prot, field.Type, cancellationToken);
                            await prot.ReadFieldEndAsync(cancellationToken);
                        }
                        await prot.ReadStructEndAsync(cancellationToken);
                        break;
                    case TType.Map:
                        var map = await prot.ReadMapBeginAsync(cancellationToken);
                        for (var i = 0; i < map.Count; i++)
                        {
                            await SkipAsync(prot, map.KeyType, cancellationToken);
                            await SkipAsync(prot, map.ValueType, cancellationToken);
                        }
                        await prot.ReadMapEndAsync(cancellationToken);
                        break;
                    case TType.Set:
                        var set = await prot.ReadSetBeginAsync(cancellationToken);
                        for (var i = 0; i < set.Count; i++)
                        {
                            await SkipAsync(prot, set.ElementType, cancellationToken);
                        }
                        await prot.ReadSetEndAsync(cancellationToken);
                        break;
                    case TType.List:
                        var list = await prot.ReadListBeginAsync(cancellationToken);
                        for (var i = 0; i < list.Count; i++)
                        {
                            await SkipAsync(prot, list.ElementType, cancellationToken);
                        }
                        await prot.ReadListEndAsync(cancellationToken);
                        break;
                    default:
                        throw new TProtocolException(TProtocolException.INVALID_DATA, "Unknown data type " + type.ToString("d"));
                }
            }
            finally
            {
                prot.DecrementRecursionDepth();
            }
        }
    }
}