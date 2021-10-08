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

package grpcdatanode

import (
	"testing"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"

	"github.com/stretchr/testify/assert"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	Params.LoadFromEnv()
	assert.NotEqual(t, Params.IP, "")
	t.Logf("DataNode IP:%s", Params.IP)

	assert.NotEqual(t, Params.Port, 0)
	t.Logf("DataNode Port:%d", Params.Port)

	assert.NotNil(t, Params.listener)
	t.Logf("DataNode listener:%d", Params.listener)

	assert.NotEqual(t, Params.DataCoordAddress, "")
	t.Logf("DataCoordAddress:%s", Params.DataCoordAddress)

	assert.NotEqual(t, Params.RootCoordAddress, "")
	t.Logf("RootCoordAddress:%s", Params.RootCoordAddress)

	log.Info("TestParamTable", zap.Int("ServerMaxSendSize", Params.ServerMaxSendSize))
	log.Info("TestParamTable", zap.Int("ServerMaxRecvSize", Params.ServerMaxRecvSize))

	Params.Remove("dataNode.grpc.serverMaxSendSize")
	Params.initServerMaxSendSize()
	assert.Equal(t, Params.ServerMaxSendSize, grpcconfigs.DefaultServerMaxSendSize)

	Params.Remove("dataNode.grpc.serverMaxRecvSize")
	Params.initServerMaxRecvSize()
	assert.Equal(t, Params.ServerMaxRecvSize, grpcconfigs.DefaultServerMaxRecvSize)
}
