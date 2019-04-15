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
using Thrift.Protocols;
using Thrift.Transports;

namespace Thrift.Server
{
    //TODO: replacement by event?

    /// <summary>
    ///     Interface implemented by server users to handle events from the server
    /// </summary>
    // ReSharper disable once InconsistentNaming
    public interface TServerEventHandler
    {
        /// <summary>
        ///     Called before the server begins */
        /// </summary>
        Task PreServeAsync(CancellationToken cancellationToken);

        /// <summary>
        ///     Called when a new client has connected and is about to being processing */
        /// </summary>
        Task<object> CreateContextAsync(TProtocol input, TProtocol output, CancellationToken cancellationToken);

        /// <summary>
        ///     Called when a client has finished request-handling to delete server context */
        /// </summary>
        Task DeleteContextAsync(object serverContext, TProtocol input, TProtocol output,
            CancellationToken cancellationToken);

        /// <summary>
        ///     Called when a client is about to call the processor */
        /// </summary>
        Task ProcessContextAsync(object serverContext, TClientTransport transport, CancellationToken cancellationToken);
    }
}