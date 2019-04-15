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
using System.Net.Sockets;


namespace Thrift.Transport
{
    public class TServerSocket : TServerTransport
    {
        /// <summary>
        /// Underlying server with socket.
        /// </summary>
        private TcpListener server = null;

        /// <summary>
        /// Port to listen on.
        /// </summary>
        private int port = 0;

        /// <summary>
        /// Timeout for client sockets from accept.
        /// </summary>
        private int clientTimeout = 0;

        /// <summary>
        /// Whether or not to wrap new TSocket connections in buffers.
        /// </summary>
        private bool useBufferedSockets = false;

        /// <summary>
        /// Creates a server socket from underlying socket object.
        /// </summary>
        public TServerSocket(TcpListener listener)
            : this(listener, 0)
        {
        }

        /// <summary>
        /// Creates a server socket from underlying socket object.
        /// </summary>
        public TServerSocket(TcpListener listener, int clientTimeout)
        {
            this.server = listener;
            this.clientTimeout = clientTimeout;
        }

        /// <summary>
        /// Creates just a port listening server socket.
        /// </summary>
        public TServerSocket(int port)
            : this(port, 0)
        {
        }

        /// <summary>
        /// Creates just a port listening server socket.
        /// </summary>
        public TServerSocket(int port, int clientTimeout)
            : this(port, clientTimeout, false)
        {
        }

        public TServerSocket(int port, int clientTimeout, bool useBufferedSockets)
        {
            this.port = port;
            this.clientTimeout = clientTimeout;
            this.useBufferedSockets = useBufferedSockets;
            try
            {
                // Make server socket
                this.server = TSocketVersionizer.CreateTcpListener(this.port);
                this.server.Server.NoDelay = true;
            }
            catch (Exception ex)
            {
                server = null;
                throw new TTransportException("Could not create ServerSocket on port " + this.port + ".", ex);
            }
        }

        public override void Listen()
        {
            // Make sure not to block on accept
            if (server != null)
            {
                try
                {
                    server.Start();
                }
                catch (SocketException sx)
                {
                    throw new TTransportException("Could not accept on listening socket: " + sx.Message, sx);
                }
            }
        }

        protected override TTransport AcceptImpl()
        {
            if (server == null)
            {
                throw new TTransportException(TTransportException.ExceptionType.NotOpen, "No underlying server socket.");
            }
            try
            {
                TSocket result2 = null;
                TcpClient result = server.AcceptTcpClient();
                try
                {
                    result2 = new TSocket(result);
                    result2.Timeout = clientTimeout;
                    if (useBufferedSockets)
                    {
                        TBufferedTransport result3 = new TBufferedTransport(result2);
                        return result3;
                    }
                    else
                    {
                        return result2;
                    }
                }
                catch (System.Exception)
                {
                    // If a TSocket was successfully created, then let
                    // it do proper cleanup of the TcpClient object.
                    if (result2 != null)
                        result2.Dispose();
                    else //  Otherwise, clean it up ourselves.
                        ((IDisposable)result).Dispose();
                    throw;
                }
            }
            catch (Exception ex)
            {
                throw new TTransportException(ex.ToString(), ex);
            }
        }

        public override void Close()
        {
            if (server != null)
            {
                try
                {
                    server.Stop();
                }
                catch (Exception ex)
                {
                    throw new TTransportException("WARNING: Could not close server socket: " + ex, ex);
                }
                server = null;
            }
        }
    }
}
