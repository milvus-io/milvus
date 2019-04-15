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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using System.ServiceModel;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Thrift.Collections;
using Thrift.Protocols;
using Thrift.Transports;
using Thrift.Transports.Client;

namespace ThriftTest
{
    public class TestClient
    {
        private class TestParams
        {
            public int numIterations = 1;
            public IPAddress host = IPAddress.Any;
            public int port = 9090;
            public int numThreads = 1;
            public string url;
            public string pipe;
            public bool buffered;
            public bool framed;
            public string protocol;
            public bool encrypted = false;

            internal void Parse( List<string> args)
            {
                for (var i = 0; i < args.Count; ++i)
                {
                    if (args[i] == "-u")
                    {
                        url = args[++i];
                    }
                    else if (args[i] == "-n")
                    {
                        numIterations = Convert.ToInt32(args[++i]);
                    }
                    else if (args[i].StartsWith("--pipe="))
                    {
                        pipe = args[i].Substring(args[i].IndexOf("=") + 1);
                        Console.WriteLine("Using named pipes transport");
                    }
                    else if (args[i].StartsWith("--host="))
                    {
                        // check there for ipaddress
                        host = new IPAddress(Encoding.Unicode.GetBytes(args[i].Substring(args[i].IndexOf("=") + 1)));
                    }
                    else if (args[i].StartsWith("--port="))
                    {
                        port = int.Parse(args[i].Substring(args[i].IndexOf("=") + 1));
                    }
                    else if (args[i] == "-b" || args[i] == "--buffered" || args[i] == "--transport=buffered")
                    {
                        buffered = true;
                        Console.WriteLine("Using buffered sockets");
                    }
                    else if (args[i] == "-f" || args[i] == "--framed" || args[i] == "--transport=framed")
                    {
                        framed = true;
                        Console.WriteLine("Using framed transport");
                    }
                    else if (args[i] == "-t")
                    {
                        numThreads = Convert.ToInt32(args[++i]);
                    }
                    else if (args[i] == "--binary" || args[i] == "--protocol=binary")
                    {
                        protocol = "binary";
                        Console.WriteLine("Using binary protocol");
                    }
                    else if (args[i] == "--compact" || args[i] == "--protocol=compact")
                    {
                        protocol = "compact";
                        Console.WriteLine("Using compact protocol");
                    }
                    else if (args[i] == "--json" || args[i] == "--protocol=json")
                    {
                        protocol = "json";
                        Console.WriteLine("Using JSON protocol");
                    }
                    else if (args[i] == "--ssl")
                    {
                        encrypted = true;
                        Console.WriteLine("Using encrypted transport");
                    }
                    else
                    {
                        //throw new ArgumentException(args[i]);
                    }
                }
            }

            private static X509Certificate2 GetClientCert()
            {
                var clientCertName = "client.p12";
                var possiblePaths = new List<string>
                {
                    "../../../keys/",
                    "../../keys/",
                    "../keys/",
                    "keys/",
                };

                string existingPath = null;
                foreach (var possiblePath in possiblePaths)
                {
                    var path = Path.GetFullPath(possiblePath + clientCertName);
                    if (File.Exists(path))
                    {
                        existingPath = path;
                        break;
                    }
                }

                if (string.IsNullOrEmpty(existingPath))
                {
                    throw new FileNotFoundException($"Cannot find file: {clientCertName}");
                }
            
                var cert = new X509Certificate2(existingPath, "thrift");

                return cert;
            }
            
            public TClientTransport CreateTransport()
            {
                if (url == null)
                {
                    // endpoint transport
                    TClientTransport trans = null;

                    if (pipe != null)
                    {
                        trans = new TNamedPipeClientTransport(pipe);
                    }
                    else
                    {
                        if (encrypted)
                        {
                           var cert = GetClientCert();
                        
                            if (cert == null || !cert.HasPrivateKey)
                            {
                                throw new InvalidOperationException("Certificate doesn't contain private key");
                            }
                            
                            trans = new TTlsSocketClientTransport(host, port, 0, cert, 
                                (sender, certificate, chain, errors) => true,
                                null, SslProtocols.Tls | SslProtocols.Tls11 | SslProtocols.Tls12);
                        }
                        else
                        {
                            trans = new TSocketClientTransport(host, port);
                        }
                    }

                    // layered transport
                    if (buffered)
                    {
                        trans = new TBufferedClientTransport(trans);
                    }

                    if (framed)
                    {
                        trans = new TFramedClientTransport(trans);
                    }

                    return trans;
                }

                return new THttpClientTransport(new Uri(url), null);
            }

            public TProtocol CreateProtocol(TClientTransport transport)
            {
                if (protocol == "compact")
                {
                    return new TCompactProtocol(transport);
                }

                if (protocol == "json")
                {
                    return new TJsonProtocol(transport);
                }

                return new TBinaryProtocol(transport);
            }
        }


        private const int ErrorBaseTypes = 1;
        private const int ErrorStructs = 2;
        private const int ErrorContainers = 4;
        private const int ErrorExceptions = 8;
        private const int ErrorUnknown = 64;

        private class ClientTest
        {
            private readonly TClientTransport transport;
            private readonly ThriftTest.Client client;
            private readonly int numIterations;
            private bool done;

            public int ReturnCode { get; set; }

            public ClientTest(TestParams param)
            {
                transport = param.CreateTransport();
                client = new ThriftTest.Client(param.CreateProtocol(transport));
                numIterations = param.numIterations;
            }

            public void Execute()
            {
                var token = CancellationToken.None;

                if (done)
                {
                    Console.WriteLine("Execute called more than once");
                    throw new InvalidOperationException();
                }

                for (var i = 0; i < numIterations; i++)
                {
                    try
                    {
                        if (!transport.IsOpen)
                        {
                            transport.OpenAsync(token).GetAwaiter().GetResult();
                        }
                    }
                    catch (TTransportException ex)
                    {
                        Console.WriteLine("*** FAILED ***");
                        Console.WriteLine("Connect failed: " + ex.Message);
                        ReturnCode |= ErrorUnknown;
                        Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
                        continue;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("*** FAILED ***");
                        Console.WriteLine("Connect failed: " + ex.Message);
                        ReturnCode |= ErrorUnknown;
                        Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
                        continue;
                    }

                    try
                    {
                        ReturnCode |= ExecuteClientTestAsync(client).GetAwaiter().GetResult(); ;
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine("*** FAILED ***");
                        Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
                        ReturnCode |= ErrorUnknown;
                    }
                }
                try
                {
                    transport.Close();
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Error while closing transport");
                    Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
                }
                done = true;
            }
        }

        internal static void PrintOptionsHelp()
        {
            Console.WriteLine("Client options:");
            Console.WriteLine("  -u <URL>");
            Console.WriteLine("  -t <# of threads to run>        default = 1");
            Console.WriteLine("  -n <# of iterations>            per thread");
            Console.WriteLine("  --pipe=<pipe name>");
            Console.WriteLine("  --host=<IP address>");
            Console.WriteLine("  --port=<port number>");
            Console.WriteLine("  --transport=<transport name>    one of buffered,framed  (defaults to none)");
            Console.WriteLine("  --protocol=<protocol name>      one of compact,json  (defaults to binary)");
            Console.WriteLine("  --ssl");
            Console.WriteLine();
        }

        public static int Execute(List<string> args)
        {
            try
            {
                var param = new TestParams();

                try
                {
                    param.Parse(args);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("*** FAILED ***");
                    Console.WriteLine("Error while  parsing arguments");
                    Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
                    return ErrorUnknown;
                }

                var tests = Enumerable.Range(0, param.numThreads).Select(_ => new ClientTest(param)).ToArray();

                //issue tests on separate threads simultaneously
                var threads = tests.Select(test => new Task(test.Execute)).ToArray();
                var start = DateTime.Now;
                foreach (var t in threads)
                {
                    t.Start();
                }

                Task.WaitAll(threads);

                Console.WriteLine("Total time: " + (DateTime.Now - start));
                Console.WriteLine();
                return tests.Select(t => t.ReturnCode).Aggregate((r1, r2) => r1 | r2);
            }
            catch (Exception outerEx)
            {
                Console.WriteLine("*** FAILED ***");
                Console.WriteLine("Unexpected error");
                Console.WriteLine(outerEx.Message + " ST: " + outerEx.StackTrace);
                return ErrorUnknown;
            }
        }

        public static string BytesToHex(byte[] data)
        {
            return BitConverter.ToString(data).Replace("-", string.Empty);
        }

        public static byte[] PrepareTestData(bool randomDist)
        {
            var retval = new byte[0x100];
            var initLen = Math.Min(0x100, retval.Length);

            // linear distribution, unless random is requested
            if (!randomDist)
            {
                for (var i = 0; i < initLen; ++i)
                {
                    retval[i] = (byte)i;
                }
                return retval;
            }

            // random distribution
            for (var i = 0; i < initLen; ++i)
            {
                retval[i] = (byte)0;
            }
            var rnd = new Random();
            for (var i = 1; i < initLen; ++i)
            {
                while (true)
                {
                    var nextPos = rnd.Next() % initLen;
                    if (retval[nextPos] == 0)
                    {
                        retval[nextPos] = (byte)i;
                        break;
                    }
                }
            }
            return retval;
        }

        public static async Task<int> ExecuteClientTestAsync(ThriftTest.Client client)
        {
            var token = CancellationToken.None;
            var returnCode = 0;

            Console.Write("testVoid()");
            await client.testVoidAsync(token);
            Console.WriteLine(" = void");

            Console.Write("testString(\"Test\")");
            var s = await client.testStringAsync("Test", token);
            Console.WriteLine(" = \"" + s + "\"");
            if ("Test" != s)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            Console.Write("testBool(true)");
            var t = await client.testBoolAsync((bool)true, token);
            Console.WriteLine(" = " + t);
            if (!t)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }
            Console.Write("testBool(false)");
            var f = await client.testBoolAsync((bool)false, token);
            Console.WriteLine(" = " + f);
            if (f)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            Console.Write("testByte(1)");
            var i8 = await client.testByteAsync((sbyte)1, token);
            Console.WriteLine(" = " + i8);
            if (1 != i8)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            Console.Write("testI32(-1)");
            var i32 = await client.testI32Async(-1, token);
            Console.WriteLine(" = " + i32);
            if (-1 != i32)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            Console.Write("testI64(-34359738368)");
            var i64 = await client.testI64Async(-34359738368, token);
            Console.WriteLine(" = " + i64);
            if (-34359738368 != i64)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            // TODO: Validate received message
            Console.Write("testDouble(5.325098235)");
            var dub = await client.testDoubleAsync(5.325098235, token);
            Console.WriteLine(" = " + dub);
            if (5.325098235 != dub)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }
            Console.Write("testDouble(-0.000341012439638598279)");
            dub = await client.testDoubleAsync(-0.000341012439638598279, token);
            Console.WriteLine(" = " + dub);
            if (-0.000341012439638598279 != dub)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            var binOut = PrepareTestData(true);
            Console.Write("testBinary(" + BytesToHex(binOut) + ")");
            try
            {
                var binIn = await client.testBinaryAsync(binOut, token);
                Console.WriteLine(" = " + BytesToHex(binIn));
                if (binIn.Length != binOut.Length)
                {
                    Console.WriteLine("*** FAILED ***");
                    returnCode |= ErrorBaseTypes;
                }
                for (var ofs = 0; ofs < Math.Min(binIn.Length, binOut.Length); ++ofs)
                    if (binIn[ofs] != binOut[ofs])
                    {
                        Console.WriteLine("*** FAILED ***");
                        returnCode |= ErrorBaseTypes;
                    }
            }
            catch (Thrift.TApplicationException ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }

            // binary equals? only with hashcode option enabled ...
            Console.WriteLine("Test CrazyNesting");
            var one = new CrazyNesting();
            var two = new CrazyNesting();
            one.String_field = "crazy";
            two.String_field = "crazy";
            one.Binary_field = new byte[] { 0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0xFF };
            two.Binary_field = new byte[10] { 0x00, 0x01, 0x02, 0x04, 0x08, 0x10, 0x20, 0x40, 0x80, 0xFF };
            if (typeof(CrazyNesting).GetMethod("Equals")?.DeclaringType == typeof(CrazyNesting))
            {
                if (!one.Equals(two))
                {
                    Console.WriteLine("*** FAILED ***");
                    returnCode |= ErrorContainers;
                    throw new Exception("CrazyNesting.Equals failed");
                }
            }

            // TODO: Validate received message
            Console.Write("testStruct({\"Zero\", 1, -3, -5})");
            var o = new Xtruct();
            o.String_thing = "Zero";
            o.Byte_thing = (sbyte)1;
            o.I32_thing = -3;
            o.I64_thing = -5;
            var i = await client.testStructAsync(o, token);
            Console.WriteLine(" = {\"" + i.String_thing + "\", " + i.Byte_thing + ", " + i.I32_thing + ", " + i.I64_thing + "}");

            // TODO: Validate received message
            Console.Write("testNest({1, {\"Zero\", 1, -3, -5}, 5})");
            var o2 = new Xtruct2();
            o2.Byte_thing = (sbyte)1;
            o2.Struct_thing = o;
            o2.I32_thing = 5;
            var i2 = await client.testNestAsync(o2, token);
            i = i2.Struct_thing;
            Console.WriteLine(" = {" + i2.Byte_thing + ", {\"" + i.String_thing + "\", " + i.Byte_thing + ", " + i.I32_thing + ", " + i.I64_thing + "}, " + i2.I32_thing + "}");

            var mapout = new Dictionary<int, int>();
            for (var j = 0; j < 5; j++)
            {
                mapout[j] = j - 10;
            }
            Console.Write("testMap({");
            var first = true;
            foreach (var key in mapout.Keys)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.Write(", ");
                }
                Console.Write(key + " => " + mapout[key]);
            }
            Console.Write("})");

            var mapin = await client.testMapAsync(mapout, token);

            Console.Write(" = {");
            first = true;
            foreach (var key in mapin.Keys)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.Write(", ");
                }
                Console.Write(key + " => " + mapin[key]);
            }
            Console.WriteLine("}");

            // TODO: Validate received message
            var listout = new List<int>();
            for (var j = -2; j < 3; j++)
            {
                listout.Add(j);
            }
            Console.Write("testList({");
            first = true;
            foreach (var j in listout)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.Write(", ");
                }
                Console.Write(j);
            }
            Console.Write("})");

            var listin = await client.testListAsync(listout, token);

            Console.Write(" = {");
            first = true;
            foreach (var j in listin)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.Write(", ");
                }
                Console.Write(j);
            }
            Console.WriteLine("}");

            //set
            // TODO: Validate received message
            var setout = new THashSet<int>();
            for (var j = -2; j < 3; j++)
            {
                setout.Add(j);
            }
            Console.Write("testSet({");
            first = true;
            foreach (int j in setout)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.Write(", ");
                }
                Console.Write(j);
            }
            Console.Write("})");

            var setin = await client.testSetAsync(setout, token);

            Console.Write(" = {");
            first = true;
            foreach (int j in setin)
            {
                if (first)
                {
                    first = false;
                }
                else
                {
                    Console.Write(", ");
                }
                Console.Write(j);
            }
            Console.WriteLine("}");


            Console.Write("testEnum(ONE)");
            var ret = await client.testEnumAsync(Numberz.ONE, token);
            Console.WriteLine(" = " + ret);
            if (Numberz.ONE != ret)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorStructs;
            }

            Console.Write("testEnum(TWO)");
            ret = await client.testEnumAsync(Numberz.TWO, token);
            Console.WriteLine(" = " + ret);
            if (Numberz.TWO != ret)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorStructs;
            }

            Console.Write("testEnum(THREE)");
            ret = await client.testEnumAsync(Numberz.THREE, token);
            Console.WriteLine(" = " + ret);
            if (Numberz.THREE != ret)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorStructs;
            }

            Console.Write("testEnum(FIVE)");
            ret = await client.testEnumAsync(Numberz.FIVE, token);
            Console.WriteLine(" = " + ret);
            if (Numberz.FIVE != ret)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorStructs;
            }

            Console.Write("testEnum(EIGHT)");
            ret = await client.testEnumAsync(Numberz.EIGHT, token);
            Console.WriteLine(" = " + ret);
            if (Numberz.EIGHT != ret)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorStructs;
            }

            Console.Write("testTypedef(309858235082523)");
            var uid = await client.testTypedefAsync(309858235082523L, token);
            Console.WriteLine(" = " + uid);
            if (309858235082523L != uid)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorStructs;
            }

            // TODO: Validate received message
            Console.Write("testMapMap(1)");
            var mm = await client.testMapMapAsync(1, token);
            Console.Write(" = {");
            foreach (var key in mm.Keys)
            {
                Console.Write(key + " => {");
                var m2 = mm[key];
                foreach (var k2 in m2.Keys)
                {
                    Console.Write(k2 + " => " + m2[k2] + ", ");
                }
                Console.Write("}, ");
            }
            Console.WriteLine("}");

            // TODO: Validate received message
            var insane = new Insanity();
            insane.UserMap = new Dictionary<Numberz, long>();
            insane.UserMap[Numberz.FIVE] = 5000L;
            var truck = new Xtruct();
            truck.String_thing = "Truck";
            truck.Byte_thing = (sbyte)8;
            truck.I32_thing = 8;
            truck.I64_thing = 8;
            insane.Xtructs = new List<Xtruct>();
            insane.Xtructs.Add(truck);
            Console.Write("testInsanity()");
            var whoa = await client.testInsanityAsync(insane, token);
            Console.Write(" = {");
            foreach (var key in whoa.Keys)
            {
                var val = whoa[key];
                Console.Write(key + " => {");

                foreach (var k2 in val.Keys)
                {
                    var v2 = val[k2];

                    Console.Write(k2 + " => {");
                    var userMap = v2.UserMap;

                    Console.Write("{");
                    if (userMap != null)
                    {
                        foreach (var k3 in userMap.Keys)
                        {
                            Console.Write(k3 + " => " + userMap[k3] + ", ");
                        }
                    }
                    else
                    {
                        Console.Write("null");
                    }
                    Console.Write("}, ");

                    var xtructs = v2.Xtructs;

                    Console.Write("{");
                    if (xtructs != null)
                    {
                        foreach (var x in xtructs)
                        {
                            Console.Write("{\"" + x.String_thing + "\", " + x.Byte_thing + ", " + x.I32_thing + ", " + x.I32_thing + "}, ");
                        }
                    }
                    else
                    {
                        Console.Write("null");
                    }
                    Console.Write("}");

                    Console.Write("}, ");
                }
                Console.Write("}, ");
            }
            Console.WriteLine("}");

            sbyte arg0 = 1;
            var arg1 = 2;
            var arg2 = long.MaxValue;
            var multiDict = new Dictionary<short, string>();
            multiDict[1] = "one";

            var tmpMultiDict = new List<string>();
            foreach (var pair in multiDict)
                tmpMultiDict.Add(pair.Key +" => "+ pair.Value);

            var arg4 = Numberz.FIVE;
            long arg5 = 5000000;
            Console.Write("Test Multi(" + arg0 + "," + arg1 + "," + arg2 + ",{" + string.Join(",", tmpMultiDict) + "}," + arg4 + "," + arg5 + ")");
            var multiResponse = await client.testMultiAsync(arg0, arg1, arg2, multiDict, arg4, arg5, token);
            Console.Write(" = Xtruct(byte_thing:" + multiResponse.Byte_thing + ",String_thing:" + multiResponse.String_thing
                          + ",i32_thing:" + multiResponse.I32_thing + ",i64_thing:" + multiResponse.I64_thing + ")\n");

            try
            {
                Console.WriteLine("testException(\"Xception\")");
                await client.testExceptionAsync("Xception", token);
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
            }
            catch (Xception ex)
            {
                if (ex.ErrorCode != 1001 || ex.Message != "Xception")
                {
                    Console.WriteLine("*** FAILED ***");
                    returnCode |= ErrorExceptions;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }
            try
            {
                Console.WriteLine("testException(\"TException\")");
                await client.testExceptionAsync("TException", token);
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
            }
            catch (Thrift.TException)
            {
                // OK
            }
            catch (Exception ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }
            try
            {
                Console.WriteLine("testException(\"ok\")");
                await client.testExceptionAsync("ok", token);
                // OK
            }
            catch (Exception ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }

            try
            {
                Console.WriteLine("testMultiException(\"Xception\", ...)");
                await client.testMultiExceptionAsync("Xception", "ignore", token);
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
            }
            catch (Xception ex)
            {
                if (ex.ErrorCode != 1001 || ex.Message != "This is an Xception")
                {
                    Console.WriteLine("*** FAILED ***");
                    returnCode |= ErrorExceptions;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }
            try
            {
                Console.WriteLine("testMultiException(\"Xception2\", ...)");
                await client.testMultiExceptionAsync("Xception2", "ignore", token);
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
            }
            catch (Xception2 ex)
            {
                if (ex.ErrorCode != 2002 || ex.Struct_thing.String_thing != "This is an Xception2")
                {
                    Console.WriteLine("*** FAILED ***");
                    returnCode |= ErrorExceptions;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }
            try
            {
                Console.WriteLine("testMultiException(\"success\", \"OK\")");
                if ("OK" != (await client.testMultiExceptionAsync("success", "OK", token)).String_thing)
                {
                    Console.WriteLine("*** FAILED ***");
                    returnCode |= ErrorExceptions;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorExceptions;
                Console.WriteLine(ex.Message + " ST: " + ex.StackTrace);
            }

            var sw = new Stopwatch();
            sw.Start();
            Console.WriteLine("Test Oneway(1)");
            await client.testOnewayAsync(1, token);
            sw.Stop();
            if (sw.ElapsedMilliseconds > 1000)
            {
                Console.WriteLine("*** FAILED ***");
                returnCode |= ErrorBaseTypes;
            }

            Console.Write("Test Calltime()");
            var times = 50;
            sw.Reset();
            sw.Start();
            for (var k = 0; k < times; ++k)
                await client.testVoidAsync(token);
            sw.Stop();
            Console.WriteLine(" = {0} ms a testVoid() call", sw.ElapsedMilliseconds / times);
            return returnCode;
        }
    }
}
