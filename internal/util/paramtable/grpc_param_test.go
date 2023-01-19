// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/stretchr/testify/assert"
)

func TestGrpcServerParams(t *testing.T) {
	role := typeutil.DataNodeRole
	base := &ComponentParam{}
	base.Init()
	var serverConfig GrpcServerConfig
	serverConfig.Init(role, &base.BaseTable)

	assert.Equal(t, serverConfig.Domain, role)
	t.Logf("Domain = %s", serverConfig.Domain)

	assert.NotEqual(t, serverConfig.IP, "")
	t.Logf("IP = %s", serverConfig.IP)

	assert.NotZero(t, serverConfig.Port.GetValue())
	t.Logf("Port = %d", serverConfig.Port.GetAsInt())

	t.Logf("Address = %s", serverConfig.GetAddress())

	assert.NotZero(t, serverConfig.ServerMaxRecvSize.GetAsInt())
	t.Logf("ServerMaxRecvSize = %d", serverConfig.ServerMaxRecvSize.GetAsInt())

	base.Remove(role + ".grpc.serverMaxRecvSize")
	assert.Equal(t, serverConfig.ServerMaxRecvSize.GetAsInt(), DefaultServerMaxRecvSize)

	base.Remove("grpc.serverMaxRecvSize")
	assert.Equal(t, serverConfig.ServerMaxRecvSize.GetAsInt(), DefaultServerMaxRecvSize)

	base.Save("grpc.serverMaxRecvSize", "a")
	assert.Equal(t, serverConfig.ServerMaxSendSize.GetAsInt(), DefaultServerMaxRecvSize)

	assert.NotZero(t, serverConfig.ServerMaxSendSize)
	t.Logf("ServerMaxSendSize = %d", serverConfig.ServerMaxSendSize.GetAsInt())

	base.Remove(role + ".grpc.serverMaxSendSize")
	assert.Equal(t, serverConfig.ServerMaxSendSize.GetAsInt(), DefaultServerMaxSendSize)

	base.Remove("grpc.serverMaxSendSize")
	assert.Equal(t, serverConfig.ServerMaxSendSize.GetAsInt(), DefaultServerMaxSendSize)

	base.Save("grpc.serverMaxSendSize", "a")
	assert.Equal(t, serverConfig.ServerMaxSendSize.GetAsInt(), DefaultServerMaxSendSize)
}
func TestGrpcClientParams(t *testing.T) {
	role := typeutil.DataNodeRole
	base := ComponentParam{}
	base.Init()
	var clientConfig GrpcClientConfig
	clientConfig.Init(role, &base.BaseTable)

	assert.Equal(t, clientConfig.Domain, role)
	t.Logf("Domain = %s", clientConfig.Domain)

	assert.NotEqual(t, clientConfig.IP, "")
	t.Logf("IP = %s", clientConfig.IP)

	assert.NotZero(t, clientConfig.Port.GetAsInt())
	t.Logf("Port = %d", clientConfig.Port.GetAsInt())

	t.Logf("Address = %s", clientConfig.GetAddress())

	assert.NotZero(t, clientConfig.ClientMaxRecvSize.GetAsInt())
	t.Logf("ClientMaxRecvSize = %d", clientConfig.ClientMaxRecvSize.GetAsInt())

	base.Remove("grpc.clientMaxRecvSize")
	base.Save(role+".grpc.clientMaxRecvSize", "1000")
	assert.Equal(t, clientConfig.ClientMaxRecvSize.GetAsInt(), 1000)

	base.Remove(role + ".grpc.clientMaxRecvSize")
	assert.Equal(t, clientConfig.ClientMaxRecvSize.GetAsInt(), DefaultClientMaxRecvSize)

	assert.NotZero(t, clientConfig.ClientMaxSendSize.GetAsInt())
	t.Logf("ClientMaxSendSize = %d", clientConfig.ClientMaxSendSize.GetAsInt())

	base.Remove("grpc.clientMaxSendSize")
	base.Save(role+".grpc.clientMaxSendSize", "2000")
	assert.Equal(t, clientConfig.ClientMaxSendSize.GetAsInt(), 2000)

	base.Remove(role + ".grpc.clientMaxSendSize")
	assert.Equal(t, clientConfig.ClientMaxSendSize.GetAsInt(), DefaultClientMaxSendSize)

	assert.Equal(t, clientConfig.DialTimeout.GetAsInt(), DefaultDialTimeout)
	base.Save("grpc.client.dialTimeout", "aaa")
	assert.Equal(t, clientConfig.DialTimeout.GetAsInt(), DefaultDialTimeout)
	base.Save("grpc.client.dialTimeout", "100")
	assert.Equal(t, clientConfig.DialTimeout.GetAsDuration(time.Millisecond), 100*time.Millisecond)

	assert.Equal(t, clientConfig.KeepAliveTime.GetAsInt(), DefaultKeepAliveTime)
	base.Save("grpc.client.keepAliveTime", "a")
	assert.Equal(t, clientConfig.KeepAliveTime.GetAsInt(), DefaultKeepAliveTime)
	base.Save("grpc.client.keepAliveTime", "200")
	assert.Equal(t, clientConfig.KeepAliveTime.GetAsDuration(time.Millisecond), 200*time.Millisecond)

	assert.Equal(t, clientConfig.KeepAliveTimeout.GetAsInt(), DefaultKeepAliveTimeout)
	base.Save("grpc.client.keepAliveTimeout", "a")
	assert.Equal(t, clientConfig.KeepAliveTimeout.GetAsInt(), DefaultKeepAliveTimeout)
	base.Save("grpc.client.keepAliveTimeout", "500")
	assert.Equal(t, clientConfig.KeepAliveTimeout.GetAsDuration(time.Millisecond), 500*time.Millisecond)

	assert.Equal(t, clientConfig.MaxAttempts.GetAsInt(), DefaultMaxAttempts)
	base.Save("grpc.client.maxMaxAttempts", "a")
	assert.Equal(t, clientConfig.MaxAttempts.GetAsInt(), DefaultMaxAttempts)
	base.Save("grpc.client.maxMaxAttempts", "1")
	assert.Equal(t, clientConfig.MaxAttempts.GetAsInt(), DefaultMaxAttempts)
	base.Save("grpc.client.maxMaxAttempts", "10")
	assert.Equal(t, clientConfig.MaxAttempts.GetAsInt(), DefaultMaxAttempts)
	base.Save("grpc.client.maxMaxAttempts", "4")
	assert.Equal(t, clientConfig.MaxAttempts.GetAsInt(), 4)

	base.Save("grpc.client.initialBackOff", "a")
	base.Save("grpc.client.initialBackOff", "2.0")

	assert.Equal(t, clientConfig.MaxBackoff.GetAsFloat(), DefaultMaxBackoff)
	base.Save("grpc.client.maxBackOff", "a")
	assert.Equal(t, clientConfig.MaxBackoff.GetAsFloat(), DefaultMaxBackoff)
	base.Save("grpc.client.maxBackOff", "50.0")
	assert.Equal(t, clientConfig.MaxBackoff.GetAsFloat(), 50.0)

	assert.Equal(t, clientConfig.BackoffMultiplier.GetAsFloat(), DefaultBackoffMultiplier)
	base.Save("grpc.client.backoffMultiplier", "a")
	assert.Equal(t, clientConfig.BackoffMultiplier.GetAsFloat(), DefaultBackoffMultiplier)
	base.Save("grpc.client.backoffMultiplier", "3.0")
	assert.Equal(t, clientConfig.BackoffMultiplier.GetAsFloat(), 3.0)

	assert.Equal(t, clientConfig.CompressionEnabled.GetAsBool(), DefaultCompressionEnabled)
	base.Save("grpc.client.CompressionEnabled", "a")
	assert.Equal(t, clientConfig.CompressionEnabled.GetAsBool(), DefaultCompressionEnabled)
	base.Save("grpc.client.CompressionEnabled", "true")
	assert.Equal(t, clientConfig.CompressionEnabled.GetAsBool(), true)

	base.Save("common.security.tlsMode", "1")
	base.Save("tls.serverPemPath", "/pem")
	base.Save("tls.serverKeyPath", "/key")
	base.Save("tls.caPemPath", "/ca")
	assert.Equal(t, clientConfig.TLSMode.GetAsInt(), 1)
	assert.Equal(t, clientConfig.ServerPemPath.GetValue(), "/pem")
	assert.Equal(t, clientConfig.ServerKeyPath.GetValue(), "/key")
	assert.Equal(t, clientConfig.CaPemPath.GetValue(), "/ca")
}
