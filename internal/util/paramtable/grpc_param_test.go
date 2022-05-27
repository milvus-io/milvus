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
	var Params GrpcServerConfig
	Params.InitOnce(role)

	assert.Equal(t, Params.Domain, role)
	t.Logf("Domain = %s", Params.Domain)

	assert.NotEqual(t, Params.IP, "")
	t.Logf("IP = %s", Params.IP)

	assert.NotZero(t, Params.Port)
	t.Logf("Port = %d", Params.Port)

	t.Logf("Address = %s", Params.GetAddress())

	assert.NotZero(t, Params.ServerMaxRecvSize)
	t.Logf("ServerMaxRecvSize = %d", Params.ServerMaxRecvSize)

	Params.Remove(role + ".grpc.serverMaxRecvSize")
	Params.initServerMaxRecvSize()
	assert.Equal(t, Params.ServerMaxRecvSize, DefaultServerMaxRecvSize)

	assert.NotZero(t, Params.ServerMaxSendSize)
	t.Logf("ServerMaxSendSize = %d", Params.ServerMaxSendSize)

	Params.Remove(role + ".grpc.serverMaxSendSize")
	Params.initServerMaxSendSize()
	assert.Equal(t, Params.ServerMaxSendSize, DefaultServerMaxSendSize)
}

func TestGrpcClientParams(t *testing.T) {
	role := typeutil.DataNodeRole
	var Params GrpcClientConfig
	Params.InitOnce(role)

	assert.Equal(t, Params.Domain, role)
	t.Logf("Domain = %s", Params.Domain)

	assert.NotEqual(t, Params.IP, "")
	t.Logf("IP = %s", Params.IP)

	assert.NotZero(t, Params.Port)
	t.Logf("Port = %d", Params.Port)

	t.Logf("Address = %s", Params.GetAddress())

	assert.NotZero(t, Params.ClientMaxRecvSize)
	t.Logf("ClientMaxRecvSize = %d", Params.ClientMaxRecvSize)

	Params.Remove("grpc.clientMaxRecvSize")
	Params.Save(role+".grpc.clientMaxRecvSize", "1000")
	Params.initClientMaxRecvSize()
	assert.Equal(t, Params.ClientMaxRecvSize, 1000)

	Params.Remove(role + ".grpc.clientMaxRecvSize")
	Params.initClientMaxRecvSize()
	assert.Equal(t, Params.ClientMaxRecvSize, DefaultClientMaxRecvSize)

	assert.NotZero(t, Params.ClientMaxSendSize)
	t.Logf("ClientMaxSendSize = %d", Params.ClientMaxSendSize)

	Params.Remove("grpc.clientMaxSendSize")
	Params.Save(role+".grpc.clientMaxSendSize", "2000")
	Params.initClientMaxSendSize()
	assert.Equal(t, Params.ClientMaxSendSize, 2000)

	Params.Remove(role + ".grpc.clientMaxSendSize")
	Params.initClientMaxSendSize()
	assert.Equal(t, Params.ClientMaxSendSize, DefaultClientMaxSendSize)

	Params.initDialTimeout()
	assert.Equal(t, Params.DialTimeout, DefaultDialTimeout)
	Params.Save("grpc.client.dialTimeout", "aaa")
	Params.initDialTimeout()
	assert.Equal(t, Params.DialTimeout, DefaultDialTimeout)
	Params.Save("grpc.client.dialTimeout", "100")
	Params.initDialTimeout()
	assert.Equal(t, Params.DialTimeout, 100*time.Millisecond)

	Params.initKeepAliveTime()
	assert.Equal(t, Params.KeepAliveTime, DefaultKeepAliveTime)
	Params.Save("grpc.client.keepAliveTime", "a")
	Params.initKeepAliveTime()
	assert.Equal(t, Params.KeepAliveTime, DefaultKeepAliveTime)
	Params.Save("grpc.client.keepAliveTime", "200")
	Params.initKeepAliveTime()
	assert.Equal(t, Params.KeepAliveTime, 200*time.Millisecond)

	Params.initKeepAliveTimeout()
	assert.Equal(t, Params.KeepAliveTimeout, DefaultKeepAliveTimeout)
	Params.Save("grpc.client.keepAliveTimeout", "a")
	Params.initKeepAliveTimeout()
	assert.Equal(t, Params.KeepAliveTimeout, DefaultKeepAliveTimeout)
	Params.Save("grpc.client.keepAliveTimeout", "500")
	Params.initKeepAliveTimeout()
	assert.Equal(t, Params.KeepAliveTimeout, 500*time.Millisecond)

	Params.Save("common.security.tlsMode", "1")
	Params.Save("tls.serverPemPath", "/pem")
	Params.Save("tls.serverKeyPath", "/key")
	Params.Save("tls.caPemPath", "/ca")
	Params.initTLSPath()
	assert.Equal(t, Params.TLSMode, 1)
	assert.Equal(t, Params.ServerPemPath, "/pem")
	assert.Equal(t, Params.ServerKeyPath, "/key")
	assert.Equal(t, Params.CaPemPath, "/ca")
}
