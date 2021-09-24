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

package grpcindexnodeclient

import (
	"testing"

	"github.com/milvus-io/milvus/internal/distributed/grpcconfigs"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestParamTable(t *testing.T) {
	Params.Init()

	log.Info("TestParamTable", zap.Int("ClientMaxSendSize", Params.ClientMaxSendSize))
	log.Info("TestParamTable", zap.Int("ClientMaxRecvSize", Params.ClientMaxRecvSize))

	Params.Remove("indexNode.grpc.clientMaxSendSize")
	Params.initClientMaxSendSize()
	assert.Equal(t, Params.ClientMaxSendSize, grpcconfigs.DefaultClientMaxSendSize)

	Params.Remove("indexNode.grpc.clientMaxRecvSize")
	Params.initClientMaxRecvSize()
	assert.Equal(t, Params.ClientMaxRecvSize, grpcconfigs.DefaultClientMaxRecvSize)
}
