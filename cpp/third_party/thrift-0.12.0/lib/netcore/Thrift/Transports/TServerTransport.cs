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

namespace Thrift.Transports
{
    // ReSharper disable once InconsistentNaming
    public abstract class TServerTransport
    {
        public abstract void Listen();
        public abstract void Close();
        public abstract bool IsClientPending();

        protected virtual async Task<TClientTransport> AcceptImplementationAsync()
        {
            return await AcceptImplementationAsync(CancellationToken.None);
        }

        protected abstract Task<TClientTransport> AcceptImplementationAsync(CancellationToken cancellationToken);

        public async Task<TClientTransport> AcceptAsync()
        {
            return await AcceptAsync(CancellationToken.None);
        }

        public async Task<TClientTransport> AcceptAsync(CancellationToken cancellationToken)
        {
            var transport = await AcceptImplementationAsync(cancellationToken);

            if (transport == null)
            {
                throw new TTransportException($"{nameof(AcceptImplementationAsync)} should not return null");
            }

            return transport;
        }
    }
}