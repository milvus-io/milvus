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
using Test;

namespace ThriftTest
{
    public class Program
    {
        public static int Main(string[] args)
        {
            try
            {
                Console.SetBufferSize(Console.BufferWidth, 4096);
            }
            catch (Exception)
            {
                Console.WriteLine("Failed to grow scroll-back buffer");
            }

            // split mode and options
            var subArgs = new List<string>(args);
            var firstArg = string.Empty;
            if (subArgs.Count > 0)
            { 
                firstArg = subArgs[0];
                subArgs.RemoveAt(0);
            }

            // run whatever mode is choosen
            switch(firstArg)
            {
                case "client":
                    return TestClient.Execute(subArgs);
                case "server":
                    return TestServer.Execute(subArgs);
                case "--help":
                    PrintHelp();
                    return 0;
                default:
                    PrintHelp();
                    return -1;
            }
        }

        private static void PrintHelp()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine("  ThriftTest  server  [options]'");
            Console.WriteLine("  ThriftTest  client  [options]'");
            Console.WriteLine("  ThriftTest  --help");
            Console.WriteLine("");

            TestServer.PrintOptionsHelp();
            TestClient.PrintOptionsHelp();
        }
    }
}


