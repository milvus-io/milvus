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
using System.Text;
using System.Threading.Tasks;
using KellermanSoftware.CompareNetObjects;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Thrift.Protocols;
using Thrift.Protocols.Entities;
using Thrift.Transports.Client;

namespace Thrift.IntegrationTests.Protocols
{
    [TestClass]
    public class ProtocolsOperationsTests
    {
        private readonly CompareLogic _compareLogic = new CompareLogic();

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol), TMessageType.Call)]
        [DataRow(typeof(TBinaryProtocol), TMessageType.Exception)]
        [DataRow(typeof(TBinaryProtocol), TMessageType.Oneway)]
        [DataRow(typeof(TBinaryProtocol), TMessageType.Reply)]
        [DataRow(typeof(TCompactProtocol), TMessageType.Call)]
        [DataRow(typeof(TCompactProtocol), TMessageType.Exception)]
        [DataRow(typeof(TCompactProtocol), TMessageType.Oneway)]
        [DataRow(typeof(TCompactProtocol), TMessageType.Reply)]
        [DataRow(typeof(TJsonProtocol), TMessageType.Call)]
        [DataRow(typeof(TJsonProtocol), TMessageType.Exception)]
        [DataRow(typeof(TJsonProtocol), TMessageType.Oneway)]
        [DataRow(typeof(TJsonProtocol), TMessageType.Reply)]
        public async Task WriteReadMessage_Test(Type protocolType, TMessageType messageType)
        {
            var expected = new TMessage(nameof(TMessage), messageType, 1);

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteMessageBeginAsync(expected);
                    await protocol.WriteMessageEndAsync();

                    stream.Seek(0, SeekOrigin.Begin);

                    var actualMessage = await protocol.ReadMessageBeginAsync();
                    await protocol.ReadMessageEndAsync();

                    var result = _compareLogic.Compare(expected, actualMessage);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        [ExpectedException(typeof(Exception))]
        public async Task WriteReadStruct_Test(Type protocolType)
        {
            var expected = new TStruct(nameof(TStruct));

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteStructBeginAsync(expected);
                    await protocol.WriteStructEndAsync();

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadStructBeginAsync();
                    await protocol.ReadStructEndAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }

            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        [ExpectedException(typeof(Exception))]
        public async Task WriteReadField_Test(Type protocolType)
        {
            var expected = new TField(nameof(TField), TType.String, 1);

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteFieldBeginAsync(expected);
                    await protocol.WriteFieldEndAsync();

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadFieldBeginAsync();
                    await protocol.ReadFieldEndAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadMap_Test(Type protocolType)
        {
            var expected = new TMap(TType.String, TType.String, 1);

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteMapBeginAsync(expected);
                    await protocol.WriteMapEndAsync();

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadMapBeginAsync();
                    await protocol.ReadMapEndAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }

        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadList_Test(Type protocolType)
        {
            var expected = new TList(TType.String, 1);

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteListBeginAsync(expected);
                    await protocol.WriteListEndAsync();

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadListBeginAsync();
                    await protocol.ReadListEndAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadSet_Test(Type protocolType)
        {
            var expected = new TSet(TType.String, 1);

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteSetBeginAsync(expected);
                    await protocol.WriteSetEndAsync();

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadSetBeginAsync();
                    await protocol.ReadSetEndAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadBool_Test(Type protocolType)
        {
            var expected = true;

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteBoolAsync(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadBoolAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadByte_Test(Type protocolType)
        {
            var expected = sbyte.MaxValue;

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteByteAsync(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadByteAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadI16_Test(Type protocolType)
        {
            var expected = short.MaxValue;

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteI16Async(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadI16Async();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadI32_Test(Type protocolType)
        {
            var expected = int.MaxValue;

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteI32Async(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadI32Async();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadI64_Test(Type protocolType)
        {
            var expected = long.MaxValue;

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteI64Async(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadI64Async();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadDouble_Test(Type protocolType)
        {
            var expected = double.MaxValue;

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteDoubleAsync(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadDoubleAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadString_Test(Type protocolType)
        {
            var expected = nameof(String);

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteStringAsync(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadStringAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        [DataTestMethod]
        [DataRow(typeof(TBinaryProtocol))]
        [DataRow(typeof(TCompactProtocol))]
        [DataRow(typeof(TJsonProtocol))]
        public async Task WriteReadBinary_Test(Type protocolType)
        {
            var expected = Encoding.UTF8.GetBytes(nameof(String));

            try
            {
                var tuple = GetProtocolInstance(protocolType);
                using (var stream = tuple.Item1)
                {
                    var protocol = tuple.Item2;

                    await protocol.WriteBinaryAsync(expected);

                    stream?.Seek(0, SeekOrigin.Begin);

                    var actual = await protocol.ReadBinaryAsync();

                    var result = _compareLogic.Compare(expected, actual);
                    Assert.IsTrue(result.AreEqual, result.DifferencesString);
                }
            }
            catch (Exception e)
            {
                throw new Exception($"Exception during testing of protocol: {protocolType.FullName}", e);
            }
        }

        private static Tuple<Stream, TProtocol> GetProtocolInstance(Type protocolType)
        {
            var memoryStream = new MemoryStream();
            var streamClientTransport = new TStreamClientTransport(memoryStream, memoryStream);
            var protocol = (TProtocol) Activator.CreateInstance(protocolType, streamClientTransport);
            return new Tuple<Stream, TProtocol>(memoryStream, protocol);
        }
    }
}
