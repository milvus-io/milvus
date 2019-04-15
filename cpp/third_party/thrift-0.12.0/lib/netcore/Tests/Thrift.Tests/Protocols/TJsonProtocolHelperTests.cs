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
using System.Linq;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Protocols.Utilities;

namespace Thrift.Tests.Protocols
{
    [TestClass]
    public class TJSONProtocolHelperTests
    {
        [TestMethod]
        public void GetTypeNameForTypeId_Test()
        {
            // input/output
            var sets = new List<Tuple<TType, byte[]>>
            {
                new Tuple<TType, byte[]>(TType.Bool, TJSONProtocolConstants.TypeNames.NameBool),
                new Tuple<TType, byte[]>(TType.Byte, TJSONProtocolConstants.TypeNames.NameByte),
                new Tuple<TType, byte[]>(TType.I16, TJSONProtocolConstants.TypeNames.NameI16),
                new Tuple<TType, byte[]>(TType.I32, TJSONProtocolConstants.TypeNames.NameI32),
                new Tuple<TType, byte[]>(TType.I64, TJSONProtocolConstants.TypeNames.NameI64),
                new Tuple<TType, byte[]>(TType.Double, TJSONProtocolConstants.TypeNames.NameDouble),
                new Tuple<TType, byte[]>(TType.String, TJSONProtocolConstants.TypeNames.NameString),
                new Tuple<TType, byte[]>(TType.Struct, TJSONProtocolConstants.TypeNames.NameStruct),
                new Tuple<TType, byte[]>(TType.Map, TJSONProtocolConstants.TypeNames.NameMap),
                new Tuple<TType, byte[]>(TType.Set, TJSONProtocolConstants.TypeNames.NameSet),
                new Tuple<TType, byte[]>(TType.List, TJSONProtocolConstants.TypeNames.NameList),
            };

            foreach (var t in sets)
            {
                Assert.IsTrue(TJSONProtocolHelper.GetTypeNameForTypeId(t.Item1) == t.Item2, $"Wrong mapping of TypeName {t.Item2} to TType: {t.Item1}");
            }
        }

        [TestMethod]
        [ExpectedException(typeof(TProtocolException))]
        public void GetTypeNameForTypeId_TStop_Test()
        {
            TJSONProtocolHelper.GetTypeNameForTypeId(TType.Stop);
        }

        [TestMethod]
        [ExpectedException(typeof(TProtocolException))]
        public void GetTypeNameForTypeId_NonExistingTType_Test()
        {
            TJSONProtocolHelper.GetTypeNameForTypeId((TType)100);
        }

        [TestMethod]
        public void GetTypeIdForTypeName_Test()
        {
            // input/output
            var sets = new List<Tuple<TType, byte[]>>
            {
                new Tuple<TType, byte[]>(TType.Bool, TJSONProtocolConstants.TypeNames.NameBool),
                new Tuple<TType, byte[]>(TType.Byte, TJSONProtocolConstants.TypeNames.NameByte),
                new Tuple<TType, byte[]>(TType.I16, TJSONProtocolConstants.TypeNames.NameI16),
                new Tuple<TType, byte[]>(TType.I32, TJSONProtocolConstants.TypeNames.NameI32),
                new Tuple<TType, byte[]>(TType.I64, TJSONProtocolConstants.TypeNames.NameI64),
                new Tuple<TType, byte[]>(TType.Double, TJSONProtocolConstants.TypeNames.NameDouble),
                new Tuple<TType, byte[]>(TType.String, TJSONProtocolConstants.TypeNames.NameString),
                new Tuple<TType, byte[]>(TType.Struct, TJSONProtocolConstants.TypeNames.NameStruct),
                new Tuple<TType, byte[]>(TType.Map, TJSONProtocolConstants.TypeNames.NameMap),
                new Tuple<TType, byte[]>(TType.Set, TJSONProtocolConstants.TypeNames.NameSet),
                new Tuple<TType, byte[]>(TType.List, TJSONProtocolConstants.TypeNames.NameList),
            };

            foreach (var t in sets)
            {
                Assert.IsTrue(TJSONProtocolHelper.GetTypeIdForTypeName(t.Item2) == t.Item1, $"Wrong mapping of TypeName {t.Item2} to TType: {t.Item1}");
            }
        }

        [TestMethod]
        [ExpectedException(typeof(TProtocolException))]
        public void GetTypeIdForTypeName_TStopTypeName_Test()
        {
            TJSONProtocolHelper.GetTypeIdForTypeName(new []{(byte)TType.Stop, (byte)TType.Stop});
        }

        [TestMethod]
        [ExpectedException(typeof(TProtocolException))]
        public void GetTypeIdForTypeName_NonExistingTypeName_Test()
        {
            TJSONProtocolHelper.GetTypeIdForTypeName(new byte[]{100});
        }

        [TestMethod]
        [ExpectedException(typeof(TProtocolException))]
        public void GetTypeIdForTypeName_EmptyName_Test()
        {
            TJSONProtocolHelper.GetTypeIdForTypeName(new byte[] {});
        }

        [TestMethod]
        public void IsJsonNumeric_Test()
        {
            // input/output
            var correctJsonNumeric = "+-.0123456789Ee";
            var incorrectJsonNumeric = "AaBcDd/*\\";

            var sets = correctJsonNumeric.Select(ch => new Tuple<byte, bool>((byte) ch, true)).ToList();
            sets.AddRange(incorrectJsonNumeric.Select(ch => new Tuple<byte, bool>((byte) ch, false)));

            foreach (var t in sets)
            {
                Assert.IsTrue(TJSONProtocolHelper.IsJsonNumeric(t.Item1) == t.Item2, $"Wrong mapping of Char {t.Item1} to bool: {t.Item2}");
            }
        }

        [TestMethod]
        public void ToHexVal_Test()
        {
            // input/output
            var chars = "0123456789abcdef";
            var expectedHexValues = new byte[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15};

            var sets = chars.Select((ch, i) => new Tuple<char, byte>(ch, expectedHexValues[i])).ToList();

            foreach (var t in sets)
            {
                var actualResult = TJSONProtocolHelper.ToHexVal((byte)t.Item1);
                Assert.IsTrue(actualResult == t.Item2, $"Wrong mapping of char byte {t.Item1} to it expected hex value: {t.Item2}. Actual hex value: {actualResult}");
            }
        }

        [TestMethod]
        [ExpectedException(typeof(TProtocolException))]
        public void ToHexVal_WrongInputChar_Test()
        {
            TJSONProtocolHelper.ToHexVal((byte)'s');
        }

        [TestMethod]
        public void ToHexChar_Test()
        {
            // input/output
            var hexValues = new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15 };
            var expectedChars = "0123456789abcdef";
            

            var sets = hexValues.Select((hv, i) => new Tuple<byte, char>(hv, expectedChars[i])).ToList();

            foreach (var t in sets)
            {
                var actualResult = TJSONProtocolHelper.ToHexChar(t.Item1);
                Assert.IsTrue(actualResult == t.Item2, $"Wrong mapping of hex value {t.Item1} to it expected char: {t.Item2}. Actual hex value: {actualResult}");
            }
        }
    }
}