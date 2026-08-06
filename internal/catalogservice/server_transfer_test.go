// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package catalogservice

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestServerStartCollectionTransferMapsStatusAndState(t *testing.T) {
	manager := &stubTransferManager{
		startResp: &StartCollectionTransferResponse{
			TransferID:   "transfer-1",
			State:        TransferStateDone,
			CollectionID: 100,
		},
	}
	server := NewServer(manager)

	resp, err := server.StartCollectionTransfer(context.Background(), &catalogpb.StartCollectionTransferRequest{
		TransferId:      "transfer-1",
		TransferEpoch:   10,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DbName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  1000,
	})
	require.NoError(t, err)
	require.NoError(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	require.Equal(t, catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_DONE, resp.GetState())
	require.Equal(t, int64(100), resp.GetCollectionId())
	require.Equal(t, StartCollectionTransferRequest{
		TransferID:      "transfer-1",
		TransferEpoch:   10,
		SourceNamespace: "milvus1",
		TargetNamespace: "milvus2",
		DBName:          "db",
		CollectionName:  "coll",
		CommitTs:        99,
		CacheExpireTs:   100,
		DrainTimeoutMs:  1000,
	}, manager.startReq)
}

func TestServerGetCollectionTransferReturnsStoredJob(t *testing.T) {
	manager := &stubTransferManager{
		job: &TransferJob{
			TransferID:   "transfer-1",
			CollectionID: 100,
			State:        TransferStateFailed,
			LastError:    "apply failed",
		},
	}
	server := NewServer(manager)

	resp, err := server.GetCollectionTransfer(context.Background(), &catalogpb.GetCollectionTransferRequest{TransferId: "transfer-1"})
	require.NoError(t, err)
	require.NoError(t, merr.CheckRPCCall(resp.GetStatus(), nil))
	require.Equal(t, catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_FAILED, resp.GetState())
	require.Equal(t, "apply failed", resp.GetLastError())
}

type stubTransferManager struct {
	startReq  StartCollectionTransferRequest
	startResp *StartCollectionTransferResponse
	startErr  error
	job       *TransferJob
	getErr    error
}

func (m *stubTransferManager) StartCollectionTransfer(ctx context.Context, req StartCollectionTransferRequest) (*StartCollectionTransferResponse, error) {
	m.startReq = req
	return m.startResp, m.startErr
}

func (m *stubTransferManager) GetCollectionTransfer(ctx context.Context, transferID string) (*TransferJob, error) {
	return m.job, m.getErr
}
