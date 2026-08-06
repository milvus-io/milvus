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

	"github.com/milvus-io/milvus/pkg/v3/proto/catalogpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type CollectionTransferManager interface {
	StartCollectionTransfer(ctx context.Context, req StartCollectionTransferRequest) (*StartCollectionTransferResponse, error)
	GetCollectionTransfer(ctx context.Context, transferID string) (*TransferJob, error)
}

type Server struct {
	catalogpb.UnimplementedCatalogServiceServer
	transferManager CollectionTransferManager
}

func NewServer(transferManager CollectionTransferManager) *Server {
	return &Server{transferManager: transferManager}
}

func (s *Server) StartCollectionTransfer(ctx context.Context, req *catalogpb.StartCollectionTransferRequest) (*catalogpb.StartCollectionTransferResponse, error) {
	resp, err := s.transferManager.StartCollectionTransfer(ctx, StartCollectionTransferRequest{
		TransferID:      req.GetTransferId(),
		TransferEpoch:   req.GetTransferEpoch(),
		SourceNamespace: req.GetSourceNamespace(),
		TargetNamespace: req.GetTargetNamespace(),
		DBName:          req.GetDbName(),
		CollectionName:  req.GetCollectionName(),
		CommitTs:        req.GetCommitTs(),
		CacheExpireTs:   req.GetCacheExpireTs(),
		DrainTimeoutMs:  req.GetDrainTimeoutMs(),
	})
	if err != nil {
		return &catalogpb.StartCollectionTransferResponse{Status: merr.Status(err)}, nil
	}
	return &catalogpb.StartCollectionTransferResponse{
		Status:       merr.Success(),
		TransferId:   resp.TransferID,
		State:        toProtoTransferState(resp.State),
		CollectionId: resp.CollectionID,
	}, nil
}

func (s *Server) GetCollectionTransfer(ctx context.Context, req *catalogpb.GetCollectionTransferRequest) (*catalogpb.GetCollectionTransferResponse, error) {
	job, err := s.transferManager.GetCollectionTransfer(ctx, req.GetTransferId())
	if err != nil {
		return &catalogpb.GetCollectionTransferResponse{Status: merr.Status(err)}, nil
	}
	return &catalogpb.GetCollectionTransferResponse{
		Status:       merr.Success(),
		TransferId:   job.TransferID,
		State:        toProtoTransferState(job.State),
		CollectionId: job.CollectionID,
		LastError:    job.LastError,
	}, nil
}

func toProtoTransferState(state TransferState) catalogpb.CollectionTransferState {
	switch state {
	case TransferStatePending:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_PENDING
	case TransferStatePrepared:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_PREPARED
	case TransferStateSourceDropped:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_SOURCE_DROPPED
	case TransferStateCatalogMoved:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_CATALOG_MOVED
	case TransferStateSourceDeactivated:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_SOURCE_DEACTIVATED
	case TransferStateTargetApplied:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_TARGET_APPLIED
	case TransferStateDone:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_DONE
	case TransferStateFailed:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_FAILED
	case TransferStateAborted:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_ABORTED
	case TransferStateCommitUncertain:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_COMMIT_UNCERTAIN
	default:
		return catalogpb.CollectionTransferState_COLLECTION_TRANSFER_STATE_UNSPECIFIED
	}
}
