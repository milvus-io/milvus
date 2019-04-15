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

namespace Thrift.Protocols.Utilities
{
    // ReSharper disable once InconsistentNaming
    public static class TJSONProtocolConstants
    {
        //TODO Check for performance for reusing ImmutableArray from System.Collections.Immutable (https://blogs.msdn.microsoft.com/dotnet/2013/06/24/please-welcome-immutablearrayt/)
        // can be possible to get better performance and also better GC

        public static readonly byte[] Comma = {(byte) ','};
        public static readonly byte[] Colon = {(byte) ':'};
        public static readonly byte[] LeftBrace = {(byte) '{'};
        public static readonly byte[] RightBrace = {(byte) '}'};
        public static readonly byte[] LeftBracket = {(byte) '['};
        public static readonly byte[] RightBracket = {(byte) ']'};
        public static readonly byte[] Quote = {(byte) '"'};
        public static readonly byte[] Backslash = {(byte) '\\'};

        public static readonly byte[] JsonCharTable =
        {
            0, 0, 0, 0, 0, 0, 0, 0, (byte) 'b', (byte) 't', (byte) 'n', 0, (byte) 'f', (byte) 'r', 0, 0,
            0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            1, 1, (byte) '"', 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1
        };

        public static readonly char[] EscapeChars = "\"\\/bfnrt".ToCharArray();
        public static readonly byte[] EscapeCharValues = {(byte) '"', (byte) '\\', (byte) '/', (byte) '\b', (byte) '\f', (byte) '\n', (byte) '\r', (byte) '\t'};
        public static readonly byte[] EscSequences = {(byte) '\\', (byte) 'u', (byte) '0', (byte) '0'};

        public static class TypeNames
        {
            public static readonly byte[] NameBool = { (byte)'t', (byte)'f' };
            public static readonly byte[] NameByte = { (byte)'i', (byte)'8' };
            public static readonly byte[] NameI16 = { (byte)'i', (byte)'1', (byte)'6' };
            public static readonly byte[] NameI32 = { (byte)'i', (byte)'3', (byte)'2' };
            public static readonly byte[] NameI64 = { (byte)'i', (byte)'6', (byte)'4' };
            public static readonly byte[] NameDouble = { (byte)'d', (byte)'b', (byte)'l' };
            public static readonly byte[] NameStruct = { (byte)'r', (byte)'e', (byte)'c' };
            public static readonly byte[] NameString = { (byte)'s', (byte)'t', (byte)'r' };
            public static readonly byte[] NameMap = { (byte)'m', (byte)'a', (byte)'p' };
            public static readonly byte[] NameList = { (byte)'l', (byte)'s', (byte)'t' };
            public static readonly byte[] NameSet = { (byte)'s', (byte)'e', (byte)'t' };
        }
    }
}