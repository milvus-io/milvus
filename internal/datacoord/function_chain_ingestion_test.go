// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package datacoord

import (
	"context"
	"testing"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/allocator"
	"github.com/milvus-io/milvus/internal/metastore/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestIngestionImportAckFailsTerminallyWithAuthoritativeSchema(t *testing.T) {
	paramtable.Init()
	for _, sourceHasBinding := range []bool{false, true} {
		t.Run(map[bool]string{false: "old proxy omitted binding", true: "replicated schema contains binding"}[sourceHasBinding], func(t *testing.T) {
			ctx := context.Background()
			chains := []*schemapb.FunctionChain{{Name: "ingestion", Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion}}
			handler := NewNMockHandler(t)
			handler.EXPECT().GetCollection(mock.Anything, int64(100)).Return(&collectionInfo{ID: 100, VChannelNames: []string{"v1"}, Schema: &schemapb.CollectionSchema{Name: "docs", FunctionChains: chains}}, nil)
			catalog := mocks.NewDataCoordCatalog(t)
			catalog.EXPECT().ListImportJobs(mock.Anything).Return(nil, nil)
			catalog.EXPECT().ListPreImportTasks(mock.Anything).Return(nil, nil)
			catalog.EXPECT().ListImportTasks(mock.Anything).Return(nil, nil)
			catalog.EXPECT().SaveImportJob(mock.Anything, mock.Anything).Return(nil)
			meta, err := NewImportMeta(ctx, catalog, nil, nil)
			require.NoError(t, err)
			ids := allocator.NewMockAllocator(t)
			ids.EXPECT().AllocN(mock.Anything).Return(int64(1000), int64(1002), nil)
			server := &Server{handler: handler, importMeta: meta, allocator: ids}
			server.stateCode.Store(commonpb.StateCode_Healthy)
			schema := &schemapb.CollectionSchema{Name: "docs"}
			if sourceHasBinding {
				schema.FunctionChains = chains
			}
			resp, err := server.createImportJobFromAck(ctx, &internalpb.ImportRequestInternal{CollectionID: 100, CollectionName: "docs", JobID: 2000, Schema: schema, Files: []*internalpb.ImportFile{{Paths: []string{"input.json"}}}}, true)
			require.NoError(t, err)
			require.NoError(t, merr.Error(resp.GetStatus()), "ACK completes instead of retrying an unsupported transform forever")
			job := meta.GetJob(ctx, 2000)
			require.NotNil(t, job)
			require.Equal(t, internalpb.ImportJobState_Failed, job.GetState())
			require.Contains(t, job.GetReason(), "ingestion function chain")
		})
	}
}
