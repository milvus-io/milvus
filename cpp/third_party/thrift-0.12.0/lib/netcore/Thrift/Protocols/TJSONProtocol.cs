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
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;
using Thrift.Transports;

namespace Thrift.Protocols
{
    /// <summary>
    ///     JSON protocol implementation for thrift.
    ///     This is a full-featured protocol supporting Write and Read.
    ///     Please see the C++ class header for a detailed description of the
    ///     protocol's wire format.
    ///     Adapted from the Java version.
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public class TJsonProtocol : TProtocol
    {
        private const long Version = 1;

        // Temporary buffer used by several methods
        private readonly byte[] _tempBuffer = new byte[4];

        // Current context that we are in
        protected JSONBaseContext Context;

        // Stack of nested contexts that we may be in
        protected Stack<JSONBaseContext> ContextStack = new Stack<JSONBaseContext>();

        // Reader that manages a 1-byte buffer
        protected LookaheadReader Reader;

        // Default encoding
        protected Encoding Utf8Encoding = Encoding.UTF8;

        /// <summary>
        ///     TJsonProtocol Constructor
        /// </summary>
        public TJsonProtocol(TClientTransport trans)
            : base(trans)
        {
            Context = new JSONBaseContext(this);
            Reader = new LookaheadReader(this);
        }

        /// <summary>
        ///     Push a new JSON context onto the stack.
        /// </summary>
        protected void PushContext(JSONBaseContext c)
        {
            ContextStack.Push(Context);
            Context = c;
        }

        /// <summary>
        ///     Pop the last JSON context off the stack
        /// </summary>
        protected void PopContext()
        {
            Context = ContextStack.Pop();
        }

        /// <summary>
        ///     Read a byte that must match b[0]; otherwise an exception is thrown.
        ///     Marked protected to avoid synthetic accessor in JSONListContext.Read
        ///     and JSONPairContext.Read
        /// </summary>
        protected async Task ReadJsonSyntaxCharAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            var ch = await Reader.ReadAsync(cancellationToken);
            if (ch != bytes[0])
            {
                throw new TProtocolException(TProtocolException.INVALID_DATA, $"Unexpected character: {(char) ch}");
            }
        }

        /// <summary>
        ///     Write the bytes in array buf as a JSON characters, escaping as needed
        /// </summary>
        private async Task WriteJsonStringAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            await Context.WriteAsync(cancellationToken);
            await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);

            var len = bytes.Length;
            for (var i = 0; i < len; i++)
            {
                if ((bytes[i] & 0x00FF) >= 0x30)
                {
                    if (bytes[i] == TJSONProtocolConstants.Backslash[0])
                    {
                        await Trans.WriteAsync(TJSONProtocolConstants.Backslash, cancellationToken);
                        await Trans.WriteAsync(TJSONProtocolConstants.Backslash, cancellationToken);
                    }
                    else
                    {
                        await Trans.WriteAsync(bytes.ToArray(), i, 1, cancellationToken);
                    }
                }
                else
                {
                    _tempBuffer[0] = TJSONProtocolConstants.JsonCharTable[bytes[i]];
                    if (_tempBuffer[0] == 1)
                    {
                        await Trans.WriteAsync(bytes, i, 1, cancellationToken);
                    }
                    else if (_tempBuffer[0] > 1)
                    {
                        await Trans.WriteAsync(TJSONProtocolConstants.Backslash, cancellationToken);
                        await Trans.WriteAsync(_tempBuffer, 0, 1, cancellationToken);
                    }
                    else
                    {
                        await Trans.WriteAsync(TJSONProtocolConstants.EscSequences, cancellationToken);
                        _tempBuffer[0] = TJSONProtocolHelper.ToHexChar((byte) (bytes[i] >> 4));
                        _tempBuffer[1] = TJSONProtocolHelper.ToHexChar(bytes[i]);
                        await Trans.WriteAsync(_tempBuffer, 0, 2, cancellationToken);
                    }
                }
            }
            await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);
        }

        /// <summary>
        ///     Write out number as a JSON value. If the context dictates so, it will be
        ///     wrapped in quotes to output as a JSON string.
        /// </summary>
        private async Task WriteJsonIntegerAsync(long num, CancellationToken cancellationToken)
        {
            await Context.WriteAsync(cancellationToken);
            var str = num.ToString();

            var escapeNum = Context.EscapeNumbers();
            if (escapeNum)
            {
                await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }

            var bytes = Utf8Encoding.GetBytes(str);
            await Trans.WriteAsync(bytes, cancellationToken);

            if (escapeNum)
            {
                await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }
        }

        /// <summary>
        ///     Write out a double as a JSON value. If it is NaN or infinity or if the
        ///     context dictates escaping, Write out as JSON string.
        /// </summary>
        private async Task WriteJsonDoubleAsync(double num, CancellationToken cancellationToken)
        {
            await Context.WriteAsync(cancellationToken);
            var str = num.ToString("G17", CultureInfo.InvariantCulture);
            var special = false;

            switch (str[0])
            {
                case 'N': // NaN
                case 'I': // Infinity
                    special = true;
                    break;
                case '-':
                    if (str[1] == 'I')
                    {
                        // -Infinity
                        special = true;
                    }
                    break;
            }

            var escapeNum = special || Context.EscapeNumbers();

            if (escapeNum)
            {
                await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }

            await Trans.WriteAsync(Utf8Encoding.GetBytes(str), cancellationToken);

            if (escapeNum)
            {
                await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }
        }

        /// <summary>
        ///     Write out contents of byte array b as a JSON string with base-64 encoded
        ///     data
        /// </summary>
        private async Task WriteJsonBase64Async(byte[] bytes, CancellationToken cancellationToken)
        {
            await Context.WriteAsync(cancellationToken);
            await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);

            var len = bytes.Length;
            var off = 0;

            while (len >= 3)
            {
                // Encode 3 bytes at a time
                TBase64Helper.Encode(bytes, off, 3, _tempBuffer, 0);
                await Trans.WriteAsync(_tempBuffer, 0, 4, cancellationToken);
                off += 3;
                len -= 3;
            }

            if (len > 0)
            {
                // Encode remainder
                TBase64Helper.Encode(bytes, off, len, _tempBuffer, 0);
                await Trans.WriteAsync(_tempBuffer, 0, len + 1, cancellationToken);
            }

            await Trans.WriteAsync(TJSONProtocolConstants.Quote, cancellationToken);
        }

        private async Task WriteJsonObjectStartAsync(CancellationToken cancellationToken)
        {
            await Context.WriteAsync(cancellationToken);
            await Trans.WriteAsync(TJSONProtocolConstants.LeftBrace, cancellationToken);
            PushContext(new JSONPairContext(this));
        }

        private async Task WriteJsonObjectEndAsync(CancellationToken cancellationToken)
        {
            PopContext();
            await Trans.WriteAsync(TJSONProtocolConstants.RightBrace, cancellationToken);
        }

        private async Task WriteJsonArrayStartAsync(CancellationToken cancellationToken)
        {
            await Context.WriteAsync(cancellationToken);
            await Trans.WriteAsync(TJSONProtocolConstants.LeftBracket, cancellationToken);
            PushContext(new JSONListContext(this));
        }

        private async Task WriteJsonArrayEndAsync(CancellationToken cancellationToken)
        {
            PopContext();
            await Trans.WriteAsync(TJSONProtocolConstants.RightBracket, cancellationToken);
        }

        public override async Task WriteMessageBeginAsync(TMessage message, CancellationToken cancellationToken)
        {
            await WriteJsonArrayStartAsync(cancellationToken);
            await WriteJsonIntegerAsync(Version, cancellationToken);

            var b = Utf8Encoding.GetBytes(message.Name);
            await WriteJsonStringAsync(b, cancellationToken);

            await WriteJsonIntegerAsync((long) message.Type, cancellationToken);
            await WriteJsonIntegerAsync(message.SeqID, cancellationToken);
        }

        public override async Task WriteMessageEndAsync(CancellationToken cancellationToken)
        {
            await WriteJsonArrayEndAsync(cancellationToken);
        }

        public override async Task WriteStructBeginAsync(TStruct @struct, CancellationToken cancellationToken)
        {
            await WriteJsonObjectStartAsync(cancellationToken);
        }

        public override async Task WriteStructEndAsync(CancellationToken cancellationToken)
        {
            await WriteJsonObjectEndAsync(cancellationToken);
        }

        public override async Task WriteFieldBeginAsync(TField field, CancellationToken cancellationToken)
        {
            await WriteJsonIntegerAsync(field.ID, cancellationToken);
            await WriteJsonObjectStartAsync(cancellationToken);
            await WriteJsonStringAsync(TJSONProtocolHelper.GetTypeNameForTypeId(field.Type), cancellationToken);
        }

        public override async Task WriteFieldEndAsync(CancellationToken cancellationToken)
        {
            await WriteJsonObjectEndAsync(cancellationToken);
        }

        public override async Task WriteFieldStopAsync(CancellationToken cancellationToken)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                await Task.FromCanceled(cancellationToken);
            }
        }

        public override async Task WriteMapBeginAsync(TMap map, CancellationToken cancellationToken)
        {
            await WriteJsonArrayStartAsync(cancellationToken);
            await WriteJsonStringAsync(TJSONProtocolHelper.GetTypeNameForTypeId(map.KeyType), cancellationToken);
            await WriteJsonStringAsync(TJSONProtocolHelper.GetTypeNameForTypeId(map.ValueType), cancellationToken);
            await WriteJsonIntegerAsync(map.Count, cancellationToken);
            await WriteJsonObjectStartAsync(cancellationToken);
        }

        public override async Task WriteMapEndAsync(CancellationToken cancellationToken)
        {
            await WriteJsonObjectEndAsync(cancellationToken);
            await WriteJsonArrayEndAsync(cancellationToken);
        }

        public override async Task WriteListBeginAsync(TList list, CancellationToken cancellationToken)
        {
            await WriteJsonArrayStartAsync(cancellationToken);
            await WriteJsonStringAsync(TJSONProtocolHelper.GetTypeNameForTypeId(list.ElementType), cancellationToken);
            await WriteJsonIntegerAsync(list.Count, cancellationToken);
        }

        public override async Task WriteListEndAsync(CancellationToken cancellationToken)
        {
            await WriteJsonArrayEndAsync(cancellationToken);
        }

        public override async Task WriteSetBeginAsync(TSet set, CancellationToken cancellationToken)
        {
            await WriteJsonArrayStartAsync(cancellationToken);
            await WriteJsonStringAsync(TJSONProtocolHelper.GetTypeNameForTypeId(set.ElementType), cancellationToken);
            await WriteJsonIntegerAsync(set.Count, cancellationToken);
        }

        public override async Task WriteSetEndAsync(CancellationToken cancellationToken)
        {
            await WriteJsonArrayEndAsync(cancellationToken);
        }

        public override async Task WriteBoolAsync(bool b, CancellationToken cancellationToken)
        {
            await WriteJsonIntegerAsync(b ? 1 : 0, cancellationToken);
        }

        public override async Task WriteByteAsync(sbyte b, CancellationToken cancellationToken)
        {
            await WriteJsonIntegerAsync(b, cancellationToken);
        }

        public override async Task WriteI16Async(short i16, CancellationToken cancellationToken)
        {
            await WriteJsonIntegerAsync(i16, cancellationToken);
        }

        public override async Task WriteI32Async(int i32, CancellationToken cancellationToken)
        {
            await WriteJsonIntegerAsync(i32, cancellationToken);
        }

        public override async Task WriteI64Async(long i64, CancellationToken cancellationToken)
        {
            await WriteJsonIntegerAsync(i64, cancellationToken);
        }

        public override async Task WriteDoubleAsync(double d, CancellationToken cancellationToken)
        {
            await WriteJsonDoubleAsync(d, cancellationToken);
        }

        public override async Task WriteStringAsync(string s, CancellationToken cancellationToken)
        {
            var b = Utf8Encoding.GetBytes(s);
            await WriteJsonStringAsync(b, cancellationToken);
        }

        public override async Task WriteBinaryAsync(byte[] bytes, CancellationToken cancellationToken)
        {
            await WriteJsonBase64Async(bytes, cancellationToken);
        }

        /// <summary>
        ///     Read in a JSON string, unescaping as appropriate.. Skip Reading from the
        ///     context if skipContext is true.
        /// </summary>
        private async Task<byte[]> ReadJsonStringAsync(bool skipContext, CancellationToken cancellationToken)
        {
            using (var buffer = new MemoryStream())
            {
                var codeunits = new List<char>();


                if (!skipContext)
                {
                    await Context.ReadAsync(cancellationToken);
                }

                await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.Quote, cancellationToken);

                while (true)
                {
                    var ch = await Reader.ReadAsync(cancellationToken);
                    if (ch == TJSONProtocolConstants.Quote[0])
                    {
                        break;
                    }

                    // escaped?
                    if (ch != TJSONProtocolConstants.EscSequences[0])
                    {
                        await buffer.WriteAsync(new[] {ch}, 0, 1, cancellationToken);
                        continue;
                    }

                    // distinguish between \uXXXX and \?
                    ch = await Reader.ReadAsync(cancellationToken);
                    if (ch != TJSONProtocolConstants.EscSequences[1]) // control chars like \n
                    {
                        var off = Array.IndexOf(TJSONProtocolConstants.EscapeChars, (char) ch);
                        if (off == -1)
                        {
                            throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected control char");
                        }
                        ch = TJSONProtocolConstants.EscapeCharValues[off];
                        await buffer.WriteAsync(new[] {ch}, 0, 1, cancellationToken);
                        continue;
                    }

                    // it's \uXXXX
                    await Trans.ReadAllAsync(_tempBuffer, 0, 4, cancellationToken);

                    var wch = (short) ((TJSONProtocolHelper.ToHexVal(_tempBuffer[0]) << 12) +
                                       (TJSONProtocolHelper.ToHexVal(_tempBuffer[1]) << 8) +
                                       (TJSONProtocolHelper.ToHexVal(_tempBuffer[2]) << 4) +
                                       TJSONProtocolHelper.ToHexVal(_tempBuffer[3]));

                    if (char.IsHighSurrogate((char) wch))
                    {
                        if (codeunits.Count > 0)
                        {
                            throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected low surrogate char");
                        }
                        codeunits.Add((char) wch);
                    }
                    else if (char.IsLowSurrogate((char) wch))
                    {
                        if (codeunits.Count == 0)
                        {
                            throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected high surrogate char");
                        }

                        codeunits.Add((char) wch);
                        var tmp = Utf8Encoding.GetBytes(codeunits.ToArray());
                        await buffer.WriteAsync(tmp, 0, tmp.Length, cancellationToken);
                        codeunits.Clear();
                    }
                    else
                    {
                        var tmp = Utf8Encoding.GetBytes(new[] {(char) wch});
                        await buffer.WriteAsync(tmp, 0, tmp.Length, cancellationToken);
                    }
                }

                if (codeunits.Count > 0)
                {
                    throw new TProtocolException(TProtocolException.INVALID_DATA, "Expected low surrogate char");
                }

                return buffer.ToArray();
            }
        }

        /// <summary>
        ///     Read in a sequence of characters that are all valid in JSON numbers. Does
        ///     not do a complete regex check to validate that this is actually a number.
        /// </summary>
        private async Task<string> ReadJsonNumericCharsAsync(CancellationToken cancellationToken)
        {
            var strbld = new StringBuilder();
            while (true)
            {
                //TODO: workaround for primitive types with TJsonProtocol, think - how to rewrite into more easy form without exceptions
                try
                {
                    var ch = await Reader.PeekAsync(cancellationToken);
                    if (!TJSONProtocolHelper.IsJsonNumeric(ch))
                    {
                        break;
                    }
                    var c = (char)await Reader.ReadAsync(cancellationToken);
                    strbld.Append(c);
                }
                catch (TTransportException)
                {
                    break;
                }
            }
            return strbld.ToString();
        }

        /// <summary>
        ///     Read in a JSON number. If the context dictates, Read in enclosing quotes.
        /// </summary>
        private async Task<long> ReadJsonIntegerAsync(CancellationToken cancellationToken)
        {
            await Context.ReadAsync(cancellationToken);
            if (Context.EscapeNumbers())
            {
                await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }

            var str = await ReadJsonNumericCharsAsync(cancellationToken);
            if (Context.EscapeNumbers())
            {
                await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }

            try
            {
                return long.Parse(str);
            }
            catch (FormatException)
            {
                throw new TProtocolException(TProtocolException.INVALID_DATA, "Bad data encounted in numeric data");
            }
        }

        /// <summary>
        ///     Read in a JSON double value. Throw if the value is not wrapped in quotes
        ///     when expected or if wrapped in quotes when not expected.
        /// </summary>
        private async Task<double> ReadJsonDoubleAsync(CancellationToken cancellationToken)
        {
            await Context.ReadAsync(cancellationToken);
            if (await Reader.PeekAsync(cancellationToken) == TJSONProtocolConstants.Quote[0])
            {
                var arr = await ReadJsonStringAsync(true, cancellationToken);
                var dub = double.Parse(Utf8Encoding.GetString(arr, 0, arr.Length), CultureInfo.InvariantCulture);

                if (!Context.EscapeNumbers() && !double.IsNaN(dub) && !double.IsInfinity(dub))
                {
                    // Throw exception -- we should not be in a string in this case
                    throw new TProtocolException(TProtocolException.INVALID_DATA, "Numeric data unexpectedly quoted");
                }

                return dub;
            }

            if (Context.EscapeNumbers())
            {
                // This will throw - we should have had a quote if escapeNum == true
                await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.Quote, cancellationToken);
            }

            try
            {
                return double.Parse(await ReadJsonNumericCharsAsync(cancellationToken), CultureInfo.InvariantCulture);
            }
            catch (FormatException)
            {
                throw new TProtocolException(TProtocolException.INVALID_DATA, "Bad data encounted in numeric data");
            }
        }

        /// <summary>
        ///     Read in a JSON string containing base-64 encoded data and decode it.
        /// </summary>
        private async Task<byte[]> ReadJsonBase64Async(CancellationToken cancellationToken)
        {
            var b = await ReadJsonStringAsync(false, cancellationToken);
            var len = b.Length;
            var off = 0;
            var size = 0;

            // reduce len to ignore fill bytes
            while ((len > 0) && (b[len - 1] == '='))
            {
                --len;
            }

            // read & decode full byte triplets = 4 source bytes
            while (len > 4)
            {
                // Decode 4 bytes at a time
                TBase64Helper.Decode(b, off, 4, b, size); // NB: decoded in place
                off += 4;
                len -= 4;
                size += 3;
            }

            // Don't decode if we hit the end or got a single leftover byte (invalid
            // base64 but legal for skip of regular string exType)
            if (len > 1)
            {
                // Decode remainder
                TBase64Helper.Decode(b, off, len, b, size); // NB: decoded in place
                size += len - 1;
            }

            // Sadly we must copy the byte[] (any way around this?)
            var result = new byte[size];
            Array.Copy(b, 0, result, 0, size);
            return result;
        }

        private async Task ReadJsonObjectStartAsync(CancellationToken cancellationToken)
        {
            await Context.ReadAsync(cancellationToken);
            await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.LeftBrace, cancellationToken);
            PushContext(new JSONPairContext(this));
        }

        private async Task ReadJsonObjectEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.RightBrace, cancellationToken);
            PopContext();
        }

        private async Task ReadJsonArrayStartAsync(CancellationToken cancellationToken)
        {
            await Context.ReadAsync(cancellationToken);
            await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.LeftBracket, cancellationToken);
            PushContext(new JSONListContext(this));
        }

        private async Task ReadJsonArrayEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonSyntaxCharAsync(TJSONProtocolConstants.RightBracket, cancellationToken);
            PopContext();
        }

        public override async Task<TMessage> ReadMessageBeginAsync(CancellationToken cancellationToken)
        {
            var message = new TMessage();
            await ReadJsonArrayStartAsync(cancellationToken);
            if (await ReadJsonIntegerAsync(cancellationToken) != Version)
            {
                throw new TProtocolException(TProtocolException.BAD_VERSION, "Message contained bad version.");
            }

            var buf = await ReadJsonStringAsync(false, cancellationToken);
            message.Name = Utf8Encoding.GetString(buf, 0, buf.Length);
            message.Type = (TMessageType) await ReadJsonIntegerAsync(cancellationToken);
            message.SeqID = (int) await ReadJsonIntegerAsync(cancellationToken);
            return message;
        }

        public override async Task ReadMessageEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonArrayEndAsync(cancellationToken);
        }

        public override async Task<TStruct> ReadStructBeginAsync(CancellationToken cancellationToken)
        {
            await ReadJsonObjectStartAsync(cancellationToken);
            return new TStruct();
        }

        public override async Task ReadStructEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonObjectEndAsync(cancellationToken);
        }

        public override async Task<TField> ReadFieldBeginAsync(CancellationToken cancellationToken)
        {
            var field = new TField();
            var ch = await Reader.PeekAsync(cancellationToken);
            if (ch == TJSONProtocolConstants.RightBrace[0])
            {
                field.Type = TType.Stop;
            }
            else
            {
                field.ID = (short) await ReadJsonIntegerAsync(cancellationToken);
                await ReadJsonObjectStartAsync(cancellationToken);
                field.Type = TJSONProtocolHelper.GetTypeIdForTypeName(await ReadJsonStringAsync(false, cancellationToken));
            }
            return field;
        }

        public override async Task ReadFieldEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonObjectEndAsync(cancellationToken);
        }

        public override async Task<TMap> ReadMapBeginAsync(CancellationToken cancellationToken)
        {
            var map = new TMap();
            await ReadJsonArrayStartAsync(cancellationToken);
            map.KeyType = TJSONProtocolHelper.GetTypeIdForTypeName(await ReadJsonStringAsync(false, cancellationToken));
            map.ValueType = TJSONProtocolHelper.GetTypeIdForTypeName(await ReadJsonStringAsync(false, cancellationToken));
            map.Count = (int) await ReadJsonIntegerAsync(cancellationToken);
            await ReadJsonObjectStartAsync(cancellationToken);
            return map;
        }

        public override async Task ReadMapEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonObjectEndAsync(cancellationToken);
            await ReadJsonArrayEndAsync(cancellationToken);
        }

        public override async Task<TList> ReadListBeginAsync(CancellationToken cancellationToken)
        {
            var list = new TList();
            await ReadJsonArrayStartAsync(cancellationToken);
            list.ElementType = TJSONProtocolHelper.GetTypeIdForTypeName(await ReadJsonStringAsync(false, cancellationToken));
            list.Count = (int) await ReadJsonIntegerAsync(cancellationToken);
            return list;
        }

        public override async Task ReadListEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonArrayEndAsync(cancellationToken);
        }

        public override async Task<TSet> ReadSetBeginAsync(CancellationToken cancellationToken)
        {
            var set = new TSet();
            await ReadJsonArrayStartAsync(cancellationToken);
            set.ElementType = TJSONProtocolHelper.GetTypeIdForTypeName(await ReadJsonStringAsync(false, cancellationToken));
            set.Count = (int) await ReadJsonIntegerAsync(cancellationToken);
            return set;
        }

        public override async Task ReadSetEndAsync(CancellationToken cancellationToken)
        {
            await ReadJsonArrayEndAsync(cancellationToken);
        }

        public override async Task<bool> ReadBoolAsync(CancellationToken cancellationToken)
        {
            return await ReadJsonIntegerAsync(cancellationToken) != 0;
        }

        public override async Task<sbyte> ReadByteAsync(CancellationToken cancellationToken)
        {
            return (sbyte) await ReadJsonIntegerAsync(cancellationToken);
        }

        public override async Task<short> ReadI16Async(CancellationToken cancellationToken)
        {
            return (short) await ReadJsonIntegerAsync(cancellationToken);
        }

        public override async Task<int> ReadI32Async(CancellationToken cancellationToken)
        {
            return (int) await ReadJsonIntegerAsync(cancellationToken);
        }

        public override async Task<long> ReadI64Async(CancellationToken cancellationToken)
        {
            return await ReadJsonIntegerAsync(cancellationToken);
        }

        public override async Task<double> ReadDoubleAsync(CancellationToken cancellationToken)
        {
            return await ReadJsonDoubleAsync(cancellationToken);
        }

        public override async Task<string> ReadStringAsync(CancellationToken cancellationToken)
        {
            var buf = await ReadJsonStringAsync(false, cancellationToken);
            return Utf8Encoding.GetString(buf, 0, buf.Length);
        }

        public override async Task<byte[]> ReadBinaryAsync(CancellationToken cancellationToken)
        {
            return await ReadJsonBase64Async(cancellationToken);
        }

        /// <summary>
        ///     Factory for JSON protocol objects
        /// </summary>
        public class Factory : ITProtocolFactory
        {
            public TProtocol GetProtocol(TClientTransport trans)
            {
                return new TJsonProtocol(trans);
            }
        }

        /// <summary>
        ///     Base class for tracking JSON contexts that may require
        ///     inserting/Reading additional JSON syntax characters
        ///     This base context does nothing.
        /// </summary>
        protected class JSONBaseContext
        {
            protected TJsonProtocol Proto;

            public JSONBaseContext(TJsonProtocol proto)
            {
                Proto = proto;
            }

            public virtual async Task WriteAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    await Task.FromCanceled(cancellationToken);
                }
            }

            public virtual async Task ReadAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    await Task.FromCanceled(cancellationToken);
                }
            }

            public virtual bool EscapeNumbers()
            {
                return false;
            }
        }

        /// <summary>
        ///     Context for JSON lists. Will insert/Read commas before each item except
        ///     for the first one
        /// </summary>
        protected class JSONListContext : JSONBaseContext
        {
            private bool _first = true;

            public JSONListContext(TJsonProtocol protocol)
                : base(protocol)
            {
            }

            public override async Task WriteAsync(CancellationToken cancellationToken)
            {
                if (_first)
                {
                    _first = false;
                }
                else
                {
                    await Proto.Trans.WriteAsync(TJSONProtocolConstants.Comma, cancellationToken);
                }
            }

            public override async Task ReadAsync(CancellationToken cancellationToken)
            {
                if (_first)
                {
                    _first = false;
                }
                else
                {
                    await Proto.ReadJsonSyntaxCharAsync(TJSONProtocolConstants.Comma, cancellationToken);
                }
            }
        }

        /// <summary>
        ///     Context for JSON records. Will insert/Read colons before the value portion
        ///     of each record pair, and commas before each key except the first. In
        ///     addition, will indicate that numbers in the key position need to be
        ///     escaped in quotes (since JSON keys must be strings).
        /// </summary>
        // ReSharper disable once InconsistentNaming
        protected class JSONPairContext : JSONBaseContext
        {
            private bool _colon = true;

            private bool _first = true;

            public JSONPairContext(TJsonProtocol proto)
                : base(proto)
            {
            }

            public override async Task WriteAsync(CancellationToken cancellationToken)
            {
                if (_first)
                {
                    _first = false;
                    _colon = true;
                }
                else
                {
                    await Proto.Trans.WriteAsync(_colon ? TJSONProtocolConstants.Colon : TJSONProtocolConstants.Comma, cancellationToken);
                    _colon = !_colon;
                }
            }

            public override async Task ReadAsync(CancellationToken cancellationToken)
            {
                if (_first)
                {
                    _first = false;
                    _colon = true;
                }
                else
                {
                    await Proto.ReadJsonSyntaxCharAsync(_colon ? TJSONProtocolConstants.Colon : TJSONProtocolConstants.Comma, cancellationToken);
                    _colon = !_colon;
                }
            }

            public override bool EscapeNumbers()
            {
                return _colon;
            }
        }

        /// <summary>
        ///     Holds up to one byte from the transport
        /// </summary>
        protected class LookaheadReader
        {
            private readonly byte[] _data = new byte[1];

            private bool _hasData;
            protected TJsonProtocol Proto;

            public LookaheadReader(TJsonProtocol proto)
            {
                Proto = proto;
            }

            /// <summary>
            ///     Return and consume the next byte to be Read, either taking it from the
            ///     data buffer if present or getting it from the transport otherwise.
            /// </summary>
            public async Task<byte> ReadAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return await Task.FromCanceled<byte>(cancellationToken);
                }

                if (_hasData)
                {
                    _hasData = false;
                }
                else
                {
                    // find more easy way to avoid exception on reading primitive types
                    await Proto.Trans.ReadAllAsync(_data, 0, 1, cancellationToken);
                }
                return _data[0];
            }

            /// <summary>
            ///     Return the next byte to be Read without consuming, filling the data
            ///     buffer if it has not been filled alReady.
            /// </summary>
            public async Task<byte> PeekAsync(CancellationToken cancellationToken)
            {
                if (cancellationToken.IsCancellationRequested)
                {
                    return await Task.FromCanceled<byte>(cancellationToken);
                }

                if (!_hasData)
                {
                    // find more easy way to avoid exception on reading primitive types
                    await Proto.Trans.ReadAllAsync(_data, 0, 1, cancellationToken);
                }
                _hasData = true;
                return _data[0];
            }
        }
    }
}
