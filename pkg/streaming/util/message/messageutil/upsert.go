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

package messageutil

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// InsertRequest exposes read-only row content from Insert or Upsert.
// Consumers use the outer message's TimeTick and the returned assignment for visibility and placement.
// Consumers must copy metadata before they modify the returned request.
type InsertRequest interface {
	MustBody() *msgpb.InsertRequest
	Body(context.Context) (*msgpb.InsertRequest, error)
}

type upsertInsertRequest struct {
	message.ImmutableUpsertMessageV2
}

func (u upsertInsertRequest) MustBody() *msgpb.InsertRequest {
	body, err := u.Body(context.Background())
	if err != nil {
		panic(err)
	}
	return body
}

func (u upsertInsertRequest) Body(ctx context.Context) (*msgpb.InsertRequest, error) {
	body, err := u.ImmutableUpsertMessageV2.Body(ctx)
	if err != nil {
		return nil, err
	}
	if err := ValidateUpsertBodyFormat(body); err != nil {
		return nil, err
	}
	return body.GetInsert(), nil
}

// AsInsertRequest exposes the insert content and assignments without a synthetic WAL message.
// The returned content and assignments are read-only. Unsupported message types return false.
func AsInsertRequest(msg message.ImmutableMessage) (InsertRequest, []*messagespb.PartitionSegmentAssignment, bool) {
	if msg == nil {
		return nil, nil, false
	}
	switch msg.MessageType() {
	case message.MessageTypeInsert:
		insert := message.MustAsImmutableInsertMessageV1(msg)
		return insert, insert.Header().GetPartitions(), true
	case message.MessageTypeUpsert:
		upsert := message.MustAsImmutableUpsertMessageV2(msg)
		return upsertInsertRequest{upsert}, []*messagespb.PartitionSegmentAssignment{upsert.Header().GetPartition()}, true
	default:
		return nil, nil, false
	}
}

// AsDeleteRequest exposes explicit delete content with the final outer TimeTick.
// Primary keys remain shared and read-only. Unsupported message types return false.
func AsDeleteRequest(msg message.ImmutableMessage) (*msgpb.DeleteRequest, bool) {
	deletion, ok, err := DeleteRequest(context.Background(), msg)
	if err != nil {
		panic(err)
	}
	return deletion, ok
}

// DeleteRequest decodes delete content with cancellation support.
// Upsert projections replace nested timestamps and shard metadata without copying primary keys.
func DeleteRequest(ctx context.Context, msg message.ImmutableMessage) (*msgpb.DeleteRequest, bool, error) {
	if msg == nil {
		return nil, false, nil
	}
	var deletion *msgpb.DeleteRequest
	switch msg.MessageType() {
	case message.MessageTypeDelete:
		typed, err := message.AsImmutableDeleteMessageV1(msg)
		if err != nil {
			return nil, true, err
		}
		deletion, err = typed.Body(ctx)
		if err != nil {
			return nil, true, err
		}
	case message.MessageTypeUpsert:
		typed, err := message.AsImmutableUpsertMessageV2(msg)
		if err != nil {
			return nil, true, err
		}
		body, err := typed.Body(ctx)
		if err != nil {
			return nil, true, err
		}
		if err := ValidateUpsertBodyFormat(body); err != nil {
			return nil, true, err
		}
		deletion = body.GetDelete()
	default:
		return nil, false, nil
	}
	if deletion == nil {
		return nil, true, merr.WrapErrServiceInternalMsg("WAL message has no delete request")
	}
	// Construct a new protobuf wrapper rather than copying its internal synchronization state.
	projected := &msgpb.DeleteRequest{
		Base: &commonpb.MsgBase{}, ShardName: msg.VChannel(), DbName: deletion.GetDbName(),
		CollectionName: deletion.GetCollectionName(), PartitionName: deletion.GetPartitionName(),
		DbID: deletion.GetDbID(), CollectionID: deletion.GetCollectionID(), PartitionID: deletion.GetPartitionID(),
		Int64PrimaryKeys: deletion.GetInt64PrimaryKeys(), NumRows: deletion.GetNumRows(),
		PrimaryKeys: deletion.GetPrimaryKeys(), SerializedExprPlan: deletion.GetSerializedExprPlan(),
	}
	if deletion.GetBase() != nil {
		projected.Base = proto.Clone(deletion.GetBase()).(*commonpb.MsgBase)
	}
	projected.Base.Timestamp = msg.TimeTick()
	if deletion.GetNumRows() < 0 {
		return nil, true, merr.WrapErrServiceInternalMsg("WAL delete row count is negative")
	}
	count := int64(len(deletion.GetPrimaryKeys().GetIntId().GetData()) + len(deletion.GetPrimaryKeys().GetStrId().GetData()))
	if deletion.GetPrimaryKeys() == nil {
		count = int64(len(deletion.GetInt64PrimaryKeys()))
	}
	if msg.MessageType() == message.MessageTypeUpsert && count != deletion.GetNumRows() {
		return nil, true, merr.WrapErrServiceInternalMsg("Upsert delete row count differs from primary key count")
	}
	if msg.MessageType() == message.MessageTypeDelete {
		projected.SegmentId = deletion.GetSegmentId()
	}
	if len(deletion.GetSerializedExprPlan()) != 0 {
		count = 1
	}
	projected.Timestamps = make([]uint64, count)
	for i := range projected.Timestamps {
		projected.Timestamps[i] = msg.TimeTick()
	}
	return projected, true, nil
}

// ValidateUpsertBodyFormat checks the compound format before consumers expose either effect.
func ValidateUpsertBodyFormat(body *message.UpsertMessageBody) error {
	if body.GetInsert() == nil || body.GetDelete() == nil {
		return merr.WrapErrServiceInternalMsg("Upsert WAL message has incomplete compound content")
	}
	if body.GetInsert().GetVersion() != msgpb.InsertDataVersion_ColumnBased || len(body.GetInsert().GetRowData()) != 0 {
		return merr.WrapErrServiceInternalMsg("Upsert WAL insert requires column-based data without row data")
	}
	return nil
}
