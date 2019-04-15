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
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols.Entities;
using Thrift.Transports;

namespace Thrift.Protocols
{
    //TODO: implementation of TProtocol

    // ReSharper disable once InconsistentNaming
    public class TCompactProtocol : TProtocol
    {
        private const byte ProtocolId = 0x82;
        private const byte Version = 1;
        private const byte VersionMask = 0x1f; // 0001 1111
        private const byte TypeMask = 0xE0; // 1110 0000
        private const byte TypeBits = 0x07; // 0000 0111
        private const int TypeShiftAmount = 5;
        private static readonly TStruct AnonymousStruct = new TStruct(string.Empty);
        private static readonly TField Tstop = new TField(string.Empty, TType.Stop, 0);

        // ReSharper disable once InconsistentNaming
        private static readonly byte[] TTypeToCompactType = new byte[16];

        /// <summary>
        ///     Used to keep track of the last field for the current and previous structs, so we can do the delta stuff.
        /// </summary>
        private readonly Stack<short> _lastField = new Stack<short>(15);

        /// <summary>
        ///     If we encounter a boolean field begin, save the TField here so it can have the value incorporated.
        /// </summary>
        private TField? _booleanField;

        /// <summary>
        ///     If we Read a field header, and it's a boolean field, save the boolean value here so that ReadBool can use it.
        /// </summary>
        private bool? _boolValue;

        private short _lastFieldId;

        public TCompactProtocol(TClientTransport trans)
            : base(trans)
        {
            TTypeToCompactType[(int) TType.Stop] = Types.Stop;
            TTypeToCompactType[(int) TType.Bool] = Types.BooleanTrue;
            TTypeToCompactType[(int) TType.Byte] = Types.Byte;
            TTypeToCompactType[(int) TType.I16] = Types.I16;
            TTypeToCompactType[(int) TType.I32] = Types.I32;
            TTypeToCompactType[(int) TType.I64] = Types.I64;
            TTypeToCompactType[(int) TType.Double] = Types.Double;
            TTypeToCompactType[(int) TType.String] = Types.Binary;
            TTypeToCompactType[(int) TType.List] = Types.List;
            TTypeToCompactType[(int) TType.Set] = Types.Set;
            TTypeToCompactType[(int) TType.Map] = Types.Map;
            TTypeToCompactType[(int) TType.Struct] = Types.Struct;
        }

        public void Reset()
        {
            _lastField.Clear();
            _lastFieldId = 0;
        }

        public override async Task WriteMessageBeginAsync(TMessage message, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await Trans.WriteAsync(new[] {ProtocolId}, cancellationToken);
            await
                Trans.WriteAsync(
                    new[] {(byte) ((Version & VersionMask) | (((uint) message.Type << TypeShiftAmount) & TypeMask))},
                    cancellationToken);

            var bufferTuple = CreateWriteVarInt32((uint) message.SeqID);
            await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);

            await WriteStringAsync(message.Name, cancellationToken);
        }

        public override async Task WriteMessageEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        /// <summary>
        ///     Write a struct begin. This doesn't actually put anything on the wire. We
        ///     use it as an opportunity to put special placeholder markers on the field
        ///     stack so we can get the field id deltas correct.
        /// </summary>
        public override async Task WriteStructBeginAsync(TStruct @struct, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            _lastField.Push(_lastFieldId);
            _lastFieldId = 0;
        }

        public override async Task WriteStructEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            _lastFieldId = _lastField.Pop();
        }

        private async Task WriteFieldBeginInternalAsync(TField field, byte typeOverride,
            CancellationToken cancellationToken)
        {
            // if there's a exType override, use that.
            var typeToWrite = typeOverride == 0xFF ? GetCompactType(field.Type) : typeOverride;

            // check if we can use delta encoding for the field id
            if ((field.ID > _lastFieldId) && (field.ID - _lastFieldId <= 15))
            {
                var b = (byte) (((field.ID - _lastFieldId) << 4) | typeToWrite);
                // Write them together
                await Trans.WriteAsync(new[] {b}, cancellationToken);
            }
            else
            {
                // Write them separate
                await Trans.WriteAsync(new[] {typeToWrite}, cancellationToken);
                await WriteI16Async(field.ID, cancellationToken);
            }

            _lastFieldId = field.ID;
        }

        public override async Task WriteFieldBeginAsync(TField field, CancellationToken cancellationToken)
        {
            if (field.Type == TType.Bool)
            {
                _booleanField = field;
            }
            else
            {
                await WriteFieldBeginInternalAsync(field, 0xFF, cancellationToken);
            }
        }

        public override async Task WriteFieldEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task WriteFieldStopAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await Trans.WriteAsync(new[] {Types.Stop}, cancellationToken);
        }

        protected async Task WriteCollectionBeginAsync(TType elemType, int size, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            /*
            Abstract method for writing the start of lists and sets. List and sets on
             the wire differ only by the exType indicator.
            */

            if (size <= 14)
            {
                await Trans.WriteAsync(new[] {(byte) ((size << 4) | GetCompactType(elemType))}, cancellationToken);
            }
            else
            {
                await Trans.WriteAsync(new[] {(byte) (0xf0 | GetCompactType(elemType))}, cancellationToken);

                var bufferTuple = CreateWriteVarInt32((uint) size);
                await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
            }
        }

        public override async Task WriteListBeginAsync(TList list, CancellationToken cancellationToken)
        {
            await WriteCollectionBeginAsync(list.ElementType, list.Count, cancellationToken);
        }

        public override async Task WriteListEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task WriteSetBeginAsync(TSet set, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await WriteCollectionBeginAsync(set.ElementType, set.Count, cancellationToken);
        }

        public override async Task WriteSetEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task WriteBoolAsync(bool b, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            /*
            Write a boolean value. Potentially, this could be a boolean field, in
            which case the field header info isn't written yet. If so, decide what the
            right exType header is for the value and then Write the field header.
            Otherwise, Write a single byte.
            */

            if (_booleanField != null)
            {
                // we haven't written the field header yet
                await
                    WriteFieldBeginInternalAsync(_booleanField.Value, b ? Types.BooleanTrue : Types.BooleanFalse,
                        cancellationToken);
                _booleanField = null;
            }
            else
            {
                // we're not part of a field, so just Write the value.
                await Trans.WriteAsync(new[] {b ? Types.BooleanTrue : Types.BooleanFalse}, cancellationToken);
            }
        }

        public override async Task WriteByteAsync(sbyte b, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            await Trans.WriteAsync(new[] {(byte) b}, cancellationToken);
        }

        public override async Task WriteI16Async(short i16, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var bufferTuple = CreateWriteVarInt32(IntToZigzag(i16));
            await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
        }

        protected internal Tuple<byte[], int> CreateWriteVarInt32(uint n)
        {
            // Write an i32 as a varint.Results in 1 - 5 bytes on the wire.
            var i32Buf = new byte[5];
            var idx = 0;

            while (true)
            {
                if ((n & ~0x7F) == 0)
                {
                    i32Buf[idx++] = (byte) n;
                    break;
                }

                i32Buf[idx++] = (byte) ((n & 0x7F) | 0x80);
                n >>= 7;
            }

            return new Tuple<byte[], int>(i32Buf, idx);
        }

        public override async Task WriteI32Async(int i32, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var bufferTuple = CreateWriteVarInt32(IntToZigzag(i32));
            await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
        }

        protected internal Tuple<byte[], int> CreateWriteVarInt64(ulong n)
        {
            // Write an i64 as a varint. Results in 1-10 bytes on the wire.
            var buf = new byte[10];
            var idx = 0;

            while (true)
            {
                if ((n & ~(ulong) 0x7FL) == 0)
                {
                    buf[idx++] = (byte) n;
                    break;
                }
                buf[idx++] = (byte) ((n & 0x7F) | 0x80);
                n >>= 7;
            }

            return new Tuple<byte[], int>(buf, idx);
        }

        public override async Task WriteI64Async(long i64, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var bufferTuple = CreateWriteVarInt64(LongToZigzag(i64));
            await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
        }

        public override async Task WriteDoubleAsync(double d, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var data = new byte[8];
            FixedLongToBytes(BitConverter.DoubleToInt64Bits(d), data, 0);
            await Trans.WriteAsync(data, cancellationToken);
        }

        public override async Task WriteStringAsync(string str, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var bytes = Encoding.UTF8.GetBytes(str);

            var bufferTuple = CreateWriteVarInt32((uint) bytes.Length);
            await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
            await Trans.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
        }

        public override async Task WriteBinaryAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            var bufferTuple = CreateWriteVarInt32((uint) bytes.Length);
            await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
            await Trans.WriteAsync(bytes, 0, bytes.Length, cancellationToken);
        }

        public override async Task WriteMapBeginAsync(TMap map, CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return;
            }

            if (map.Count == 0)
            {
                await Trans.WriteAsync(new[] {(byte) 0}, cancellationToken);
            }
            else
            {
                var bufferTuple = CreateWriteVarInt32((uint) map.Count);
                await Trans.WriteAsync(bufferTuple.Item1, 0, bufferTuple.Item2, cancellationToken);
                await
                    Trans.WriteAsync(
                        new[] {(byte) ((GetCompactType(map.KeyType) << 4) | GetCompactType(map.ValueType))},
                        cancellationToken);
            }
        }

        public override async Task WriteMapEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task<TMessage> ReadMessageBeginAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<TMessage>(cancellationToken);
            }

            var protocolId = (byte) await ReadByteAsync(cancellationToken);
            if (protocolId != ProtocolId)
            {
                throw new TProtocolException($"Expected protocol id {ProtocolId:X} but got {protocolId:X}");
            }

            var versionAndType = (byte) await ReadByteAsync(cancellationToken);
            var version = (byte) (versionAndType & VersionMask);

            if (version != Version)
            {
                throw new TProtocolException($"Expected version {Version} but got {version}");
            }

            var type = (byte) ((versionAndType >> TypeShiftAmount) & TypeBits);
            var seqid = (int) await ReadVarInt32Async(cancellationToken);
            var messageName = await ReadStringAsync(cancellationToken);

            return new TMessage(messageName, (TMessageType) type, seqid);
        }

        public override async Task ReadMessageEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task<TStruct> ReadStructBeginAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<TStruct>(cancellationToken);
            }

            // some magic is here )

            _lastField.Push(_lastFieldId);
            _lastFieldId = 0;

            return AnonymousStruct;
        }

        public override async Task ReadStructEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }

            /*
            Doesn't actually consume any wire data, just removes the last field for
            this struct from the field stack.
            */

            // consume the last field we Read off the wire.
            _lastFieldId = _lastField.Pop();
        }

        public override async Task<TField> ReadFieldBeginAsync(CancellationToken cancellationToken)
        {
            // Read a field header off the wire.
            var type = (byte) await ReadByteAsync(cancellationToken);
            // if it's a stop, then we can return immediately, as the struct is over.
            if (type == Types.Stop)
            {
                return Tstop;
            }

            short fieldId;
            // mask off the 4 MSB of the exType header. it could contain a field id delta.
            var modifier = (short) ((type & 0xf0) >> 4);
            if (modifier == 0)
            {
                fieldId = await ReadI16Async(cancellationToken);
            }
            else
            {
                fieldId = (short) (_lastFieldId + modifier);
            }

            var field = new TField(string.Empty, GetTType((byte) (type & 0x0f)), fieldId);
            // if this happens to be a boolean field, the value is encoded in the exType
            if (IsBoolType(type))
            {
                _boolValue = (byte) (type & 0x0f) == Types.BooleanTrue;
            }

            // push the new field onto the field stack so we can keep the deltas going.
            _lastFieldId = field.ID;
            return field;
        }

        public override async Task ReadFieldEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task<TMap> ReadMapBeginAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled<TMap>(cancellationToken);
            }

            /*
            Read a map header off the wire. If the size is zero, skip Reading the key
            and value exType. This means that 0-length maps will yield TMaps without the
            "correct" types.
            */

            var size = (int) await ReadVarInt32Async(cancellationToken);
            var keyAndValueType = size == 0 ? (byte) 0 : (byte) await ReadByteAsync(cancellationToken);
            return new TMap(GetTType((byte) (keyAndValueType >> 4)), GetTType((byte) (keyAndValueType & 0xf)), size);
        }

        public override async Task ReadMapEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task<TSet> ReadSetBeginAsync(CancellationToken cancellationToken)
        {
            /*
            Read a set header off the wire. If the set size is 0-14, the size will
            be packed into the element exType header. If it's a longer set, the 4 MSB
            of the element exType header will be 0xF, and a varint will follow with the
            true size.
            */

            return new TSet(await ReadListBeginAsync(cancellationToken));
        }

        public override async Task<bool> ReadBoolAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<bool>(cancellationToken);
            }

            /*
            Read a boolean off the wire. If this is a boolean field, the value should
            already have been Read during ReadFieldBegin, so we'll just consume the
            pre-stored value. Otherwise, Read a byte.
            */

            if (_boolValue != null)
            {
                var result = _boolValue.Value;
                _boolValue = null;
                return result;
            }

            return await ReadByteAsync(cancellationToken) == Types.BooleanTrue;
        }

        public override async Task<sbyte> ReadByteAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<sbyte>(cancellationToken);
            }

            // Read a single byte off the wire. Nothing interesting here.
            var buf = new byte[1];
            await Trans.ReadAllAsync(buf, 0, 1, cancellationToken);
            return (sbyte) buf[0];
        }

        public override async Task<short> ReadI16Async(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<short>(cancellationToken);
            }

            return (short) ZigzagToInt(await ReadVarInt32Async(cancellationToken));
        }

        public override async Task<int> ReadI32Async(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<int>(cancellationToken);
            }

            return ZigzagToInt(await ReadVarInt32Async(cancellationToken));
        }

        public override async Task<long> ReadI64Async(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<long>(cancellationToken);
            }

            return ZigzagToLong(await ReadVarInt64Async(cancellationToken));
        }

        public override async Task<double> ReadDoubleAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<double>(cancellationToken);
            }

            var longBits = new byte[8];
            await Trans.ReadAllAsync(longBits, 0, 8, cancellationToken);

            return BitConverter.Int64BitsToDouble(BytesToLong(longBits));
        }

        public override async Task<string> ReadStringAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled<string>(cancellationToken);
            }

            // Reads a byte[] (via ReadBinary), and then UTF-8 decodes it.
            var length = (int) await ReadVarInt32Async(cancellationToken);

            if (length == 0)
            {
                return string.Empty;
            }

            var buf = new byte[length];
            await Trans.ReadAllAsync(buf, 0, length, cancellationToken);

            return Encoding.UTF8.GetString(buf);
        }

        public override async Task<byte[]> ReadBinaryAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<byte[]>(cancellationToken);
            }

            // Read a byte[] from the wire.
            var length = (int) await ReadVarInt32Async(cancellationToken);
            if (length == 0)
            {
                return new byte[0];
            }

            var buf = new byte[length];
            await Trans.ReadAllAsync(buf, 0, length, cancellationToken);
            return buf;
        }

        public override async Task<TList> ReadListBeginAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled<TList>(cancellationToken);
            }

            /*
            Read a list header off the wire. If the list size is 0-14, the size will
            be packed into the element exType header. If it's a longer list, the 4 MSB
            of the element exType header will be 0xF, and a varint will follow with the
            true size.
            */

            var sizeAndType = (byte) await ReadByteAsync(cancellationToken);
            var size = (sizeAndType >> 4) & 0x0f;
            if (size == 15)
            {
                size = (int) await ReadVarInt32Async(cancellationToken);
            }

            var type = GetTType(sizeAndType);
            return new TList(type, size);
        }

        public override async Task ReadListEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task ReadSetEndAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        private static byte GetCompactType(TType ttype)
        {
            // Given a TType value, find the appropriate TCompactProtocol.Types constant.
            return TTypeToCompactType[(int) ttype];
        }


        private async Task<uint> ReadVarInt32Async(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<uint>(cancellationToken);
            }

            /*
            Read an i32 from the wire as a varint. The MSB of each byte is set
            if there is another byte to follow. This can Read up to 5 bytes.
            */

            uint result = 0;
            var shift = 0;

            while (true)
            {
                var b = (byte) await ReadByteAsync(cancellationToken);
                result |= (uint) (b & 0x7f) << shift;
                if ((b & 0x80) != 0x80)
                {
                    break;
                }
                shift += 7;
            }

            return result;
        }

        private async Task<ulong> ReadVarInt64Async(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return await Task.FromCanceled<uint>(cancellationToken);
            }

            /*
            Read an i64 from the wire as a proper varint. The MSB of each byte is set
            if there is another byte to follow. This can Read up to 10 bytes.
            */

            var shift = 0;
            ulong result = 0;
            while (true)
            {
                var b = (byte) await ReadByteAsync(cancellationToken);
                result |= (ulong) (b & 0x7f) << shift;
                if ((b & 0x80) != 0x80)
                {
                    break;
                }
                shift += 7;
            }

            return result;
        }

        private static int ZigzagToInt(uint n)
        {
            return (int) (n >> 1) ^ -(int) (n & 1);
        }

        private static long ZigzagToLong(ulong n)
        {
            return (long) (n >> 1) ^ -(long) (n & 1);
        }

        private static long BytesToLong(byte[] bytes)
        {
            /*
            Note that it's important that the mask bytes are long literals,
            otherwise they'll default to ints, and when you shift an int left 56 bits,
            you just get a messed up int.
            */

            return
                ((bytes[7] & 0xffL) << 56) |
                ((bytes[6] & 0xffL) << 48) |
                ((bytes[5] & 0xffL) << 40) |
                ((bytes[4] & 0xffL) << 32) |
                ((bytes[3] & 0xffL) << 24) |
                ((bytes[2] & 0xffL) << 16) |
                ((bytes[1] & 0xffL) << 8) |
                (bytes[0] & 0xffL);
        }

        private static bool IsBoolType(byte b)
        {
            var lowerNibble = b & 0x0f;
            return (lowerNibble == Types.BooleanTrue) || (lowerNibble == Types.BooleanFalse);
        }

        private static TType GetTType(byte type)
        {
            // Given a TCompactProtocol.Types constant, convert it to its corresponding TType value.
            switch ((byte) (type & 0x0f))
            {
                case Types.Stop:
                    return TType.Stop;
                case Types.BooleanFalse:
                case Types.BooleanTrue:
                    return TType.Bool;
                case Types.Byte:
                    return TType.Byte;
                case Types.I16:
                    return TType.I16;
                case Types.I32:
                    return TType.I32;
                case Types.I64:
                    return TType.I64;
                case Types.Double:
                    return TType.Double;
                case Types.Binary:
                    return TType.String;
                case Types.List:
                    return TType.List;
                case Types.Set:
                    return TType.Set;
                case Types.Map:
                    return TType.Map;
                case Types.Struct:
                    return TType.Struct;
                default:
                    throw new TProtocolException($"Don't know what exType: {(byte) (type & 0x0f)}");
            }
        }

        private static ulong LongToZigzag(long n)
        {
            // Convert l into a zigzag long. This allows negative numbers to be represented compactly as a varint
            return (ulong) (n << 1) ^ (ulong) (n >> 63);
        }

        private static uint IntToZigzag(int n)
        {
            // Convert n into a zigzag int. This allows negative numbers to be represented compactly as a varint
            return (uint) (n << 1) ^ (uint) (n >> 31);
        }

        private static void FixedLongToBytes(long n, byte[] buf, int off)
        {
            // Convert a long into little-endian bytes in buf starting at off and going until off+7.
            buf[off + 0] = (byte) (n & 0xff);
            buf[off + 1] = (byte) ((n >> 8) & 0xff);
            buf[off + 2] = (byte) ((n >> 16) & 0xff);
            buf[off + 3] = (byte) ((n >> 24) & 0xff);
            buf[off + 4] = (byte) ((n >> 32) & 0xff);
            buf[off + 5] = (byte) ((n >> 40) & 0xff);
            buf[off + 6] = (byte) ((n >> 48) & 0xff);
            buf[off + 7] = (byte) ((n >> 56) & 0xff);
        }

        public class Factory : ITProtocolFactory
        {
            public TProtocol GetProtocol(TClientTransport trans)
            {
                return new TCompactProtocol(trans);
            }
        }

        /// <summary>
        ///     All of the on-wire exType codes.
        /// </summary>
        private static class Types
        {
            public const byte Stop = 0x00;
            public const byte BooleanTrue = 0x01;
            public const byte BooleanFalse = 0x02;
            public const byte Byte = 0x03;
            public const byte I16 = 0x04;
            public const byte I32 = 0x05;
            public const byte I64 = 0x06;
            public const byte Double = 0x07;
            public const byte Binary = 0x08;
            public const byte List = 0x09;
            public const byte Set = 0x0A;
            public const byte Map = 0x0B;
            public const byte Struct = 0x0C;
        }
    }
}