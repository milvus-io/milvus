// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package coordinator

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/dependency"
	"github.com/milvus-io/milvus/pkg/v3/proto/pkindexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// The gRPC server registers the services before the mixcoord leaves standby,
// when datacoord does not exist yet.
func TestRegisterPKIndexCoordGRPCServiceBeforeActivation(t *testing.T) {
	paramtable.Init()
	core, err := NewMixCoordServer(context.Background(), dependency.NewDefaultFactory(true))
	require.NoError(t, err)
	require.Nil(t, core.datacoordServer)

	server := grpc.NewServer()
	defer server.Stop()
	core.RegisterPKIndexCoordGRPCService(server)
	assert.Contains(t, server.GetServiceInfo(), pkindexpb.PKIndexCoordService_ServiceDesc.ServiceName)

	resp, err := core.pkindexService.CommitFlush(context.Background(), &pkindexpb.CommitFlushRequest{
		CollectionId:       1,
		Vchannel:           "by-dev-rootcoord-dml_0_1v0",
		PchannelTerm:       1,
		CheckpointTimeTick: 1,
	})
	require.NoError(t, err)
	assert.ErrorIs(t, merr.Error(resp.GetStatus()), merr.ErrServiceNotReady)
}
