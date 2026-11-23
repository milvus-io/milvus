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

package milvusclient

import (
	"context"
	"slices"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/client/v3/internal/merr"
)

// UpdateRecordsOption selects primary keys and one shared set of literal
// assignments. Validation of field types and request limits belongs to Proxy.
type UpdateRecordsOption interface {
	Request() (*milvuspb.UpdateRecordsRequest, error)
}

type updateRecordsOption struct {
	request milvuspb.UpdateRecordsRequest
}

// NewUpdateRecordsOption creates a PK-update request. Without WithPartition,
// the request applies to every partition. Values and key slices are borrowed;
// do not mutate them until Client.UpdateRecords returns.
func NewUpdateRecordsOption(collectionName string) *updateRecordsOption {
	return &updateRecordsOption{request: milvuspb.UpdateRecordsRequest{CollectionName: collectionName}}
}

func (opt *updateRecordsOption) WithPartition(partitionName string) *updateRecordsOption {
	opt.request.PartitionName = partitionName
	return opt
}

func (opt *updateRecordsOption) WithInt64IDs(ids []int64) *updateRecordsOption {
	opt.request.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids}}}
	return opt
}

func (opt *updateRecordsOption) WithStringIDs(ids []string) *updateRecordsOption {
	opt.request.PrimaryKeys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids}}}
	return opt
}

// WithAssignment appends one typed literal assignment. Repeating a field is
// rejected by Proxy; neither assignments nor primary keys are deduplicated here.
func (opt *updateRecordsOption) WithAssignment(fieldName string, literal *schemapb.UpdateLiteral) *updateRecordsOption {
	opt.request.Assignments = append(opt.request.Assignments, &milvuspb.UpdateAssignment{FieldName: fieldName, Literal: literal})
	return opt
}

func (opt *updateRecordsOption) WithNull(fieldName string) *updateRecordsOption {
	return opt.WithAssignment(fieldName, &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_SetNull{SetNull: true}})
}

func (opt *updateRecordsOption) Request() (*milvuspb.UpdateRecordsRequest, error) {
	return &opt.request, nil
}

type UpdateRecordsResult struct {
	// UpdateCount counts submitted PKs, not matched physical rows. A missing PK
	// is not inserted; query visibility follows the server's apply frontier.
	UpdateCount  int64
	Acknowledged bool
	Timestamp    uint64
}

// UpdateRecords applies literals without fetching existing rows or requiring a
// loaded collection. It does not retry schema mismatches or transport failures:
// another VChannel may already have committed, or a response may be lost after
// commit. Reissuing the request could overwrite a later writer's value.
func (c *Client) UpdateRecords(ctx context.Context, option UpdateRecordsOption, callOptions ...grpc.CallOption) (UpdateRecordsResult, error) {
	startTime := time.Now()
	result := UpdateRecordsResult{}
	req, err := option.Request()
	defer func() { c.recordOperation("UpdateRecords", req.GetCollectionName(), startTime, err) }()
	if err != nil {
		return result, err
	}
	// Override the client-wide retry interceptor for this non-idempotent RPC.
	callOptions = append(slices.Clone(callOptions), grpc_retry.Disable())
	err = c.callService(func(service milvuspb.MilvusServiceClient) error {
		resp, err := service.UpdateRecords(ctx, req, callOptions...)
		if err := merr.CheckRPCCall(resp, err); err != nil {
			return err
		}
		result.UpdateCount = resp.GetUpdateCnt()
		result.Acknowledged = resp.GetAcknowledged()
		result.Timestamp = resp.GetTimestamp()
		return nil
	})
	return result, err
}
