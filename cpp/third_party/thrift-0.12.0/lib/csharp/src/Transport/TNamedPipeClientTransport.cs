/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

using System;
using System.IO.Pipes;
using System.Threading;

namespace Thrift.Transport
{
    public class TNamedPipeClientTransport : TTransport
    {
        private NamedPipeClientStream client;
        private string ServerName;
        private string PipeName;
        private int ConnectTimeout;

        public TNamedPipeClientTransport(string pipe, int timeout = Timeout.Infinite)
        {
            ServerName = ".";
            PipeName = pipe;
            ConnectTimeout = timeout;
        }

        public TNamedPipeClientTransport(string server, string pipe, int timeout = Timeout.Infinite)
        {
            ServerName = (server != "") ? server : ".";
            PipeName = pipe;
            ConnectTimeout = timeout;
        }

        public override bool IsOpen
        {
            get { return client != null && client.IsConnected; }
        }

        public override void Open()
        {
            if (IsOpen)
            {
                throw new TTransportException(TTransportException.ExceptionType.AlreadyOpen);
            }
            client = new NamedPipeClientStream(ServerName, PipeName, PipeDirection.InOut, PipeOptions.None);
            client.Connect(ConnectTimeout);
        }

        public override void Close()
        {
            if (client != null)
            {
                client.Close();
                client = null;
            }
        }

        public override int Read(byte[] buf, int off, int len)
        {
            if (client == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            return client.Read(buf, off, len);
        }

        public override void Write(byte[] buf, int off, int len)
        {
            if (client == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen);
            }

            // if necessary, send the data in chunks
            // there's a system limit around 0x10000 bytes that we hit otherwise
            // MSDN: "Pipe write operations across a network are limited to 65,535 bytes per write. For more information regarding pipes, see the Remarks section."
            var nBytes = Math.Min(len, 15 * 4096);  // 16 would exceed the limit
            while (nBytes > 0)
            {
                client.Write(buf, off, nBytes);

                off += nBytes;
                len -= nBytes;
                nBytes = Math.Min(len, nBytes);
            }
        }

        protected override void Dispose(bool disposing)
        {
            client.Dispose();
        }
    }
}
