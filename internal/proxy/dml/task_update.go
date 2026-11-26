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

package dml

import (
	"context"
	"encoding/binary"

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/proxy/taskmodel"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	walutil "github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var _ taskmodel.DMLTask = (*UpdateTask)(nil)

type UpdateTask struct {
	baseTask
	Condition
	request *milvuspb.UpdateRequest
	info    *collectionInfo
	msgs    []message.MutableMessage
	result  *milvuspb.MutationResult
	id      UniqueID
	ts      Timestamp
}

func NewUpdateTask(ctx context.Context, cache Cache, request *milvuspb.UpdateRequest) *UpdateTask {
	return &UpdateTask{
		baseTask:  baseTask{MetaCache: cache},
		Condition: NewTaskCondition(ctx),
		request:   request,
	}
}

func (t *UpdateTask) TraceCtx() context.Context         { return t.Ctx() }
func (t *UpdateTask) ID() UniqueID                      { return t.id }
func (t *UpdateTask) SetID(id UniqueID)                 { t.id = id }
func (t *UpdateTask) BeginTs() Timestamp                { return t.ts }
func (t *UpdateTask) EndTs() Timestamp                  { return t.ts }
func (t *UpdateTask) SetTs(ts Timestamp)                { t.ts = ts }
func (t *UpdateTask) Name() string                      { return "UpdateTask" }
func (t *UpdateTask) OnEnqueue() error                  { return nil }
func (t *UpdateTask) PostExecute(context.Context) error { return nil }
func (t *UpdateTask) Result() *milvuspb.MutationResult  { return t.result }

func (t *UpdateTask) Type() commonpb.MsgType { return commonpb.MsgType_Update }

func (t *UpdateTask) SetChannels() error {
	if t.request == nil {
		return merr.WrapErrParameterInvalidMsg("missing update request")
	}
	info, err := t.GetMetaCache().GetCollectionInfo(t.Ctx(), t.request.GetDbName(), t.request.GetCollectionName(), 0)
	t.info = info
	return err
}

func (t *UpdateTask) GetChannels() []pChan {
	if t.info == nil {
		return nil
	}
	return t.info.PChannels
}

// Prepare every shard before the first WAL append.
func (t *UpdateTask) PreExecute(ctx context.Context) error {
	request, info := t.request, t.info
	schema, err := info.GetWriteSchema(ctx)
	if err != nil {
		return err
	}
	// Namespace collections may route inserts by namespace instead of PK. They
	// need a namespace-aware mutation contract before this PK-only API can serve them.
	if schema.GetEnableNamespace() {
		return merr.WrapErrParameterInvalidMsg("PK-update does not support namespace collections")
	}
	if err := validateUpdatePKs(request.GetPrimaryKeys(), schema.CollectionSchema); err != nil {
		return err
	}
	assignments, err := validateUpdateAssignments(schema.CollectionSchema, request.GetAssignments())
	if err != nil {
		return err
	}
	partitionID := common.AllPartitionsID
	if request.GetPartitionName() != "" {
		if partitionKey, _ := typeutil.GetPartitionKeyFieldSchema(schema.CollectionSchema); partitionKey != nil {
			return merr.WrapErrParameterInvalidMsg("cannot specify a partition in partition-key mode")
		}
		partitionID, err = t.GetMetaCache().GetPartitionID(ctx, request.GetDbName(), request.GetCollectionName(), request.GetPartitionName())
		if err != nil {
			return err
		}
	}
	var cipher *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		cipher = hookutil.GetEzByCollProperties(schema.GetProperties(), info.CollID).AsMessageConfig()
	}
	t.msgs, err = buildUpdateMessages(info.CollID, schema.Version, partitionID, info.VChannels, request.GetPrimaryKeys(), assignments, cipher)
	return err
}

func (t *UpdateTask) Execute(ctx context.Context) error {
	resp := streaming.WAL().AppendMessages(ctx, t.msgs...)
	invalidateMismatchedWriteSchema(ctx, t.GetMetaCache(), t.info.CollID, resp)
	if err := resp.UnwrapFirstError(); err != nil {
		if status.AsStreamingError(err).IsSchemaVersionMismatch() {
			return merr.ErrCollectionSchemaMismatch
		}
		return err
	}
	t.result = &milvuspb.MutationResult{
		Status:       merr.Success(),
		Acknowledged: true,
		Timestamp:    resp.MaxTimeTick(),
		UpdateCnt:    int64(typeutil.GetSizeOfIDs(t.request.GetPrimaryKeys())),
	}
	return nil
}

func buildUpdateMessages(collectionID int64, schemaVersion int32, partitionID int64, channels []string, ids *schemapb.IDs, assignments []*messagespb.UpdateAssignment, cipher *message.CipherConfig) ([]message.MutableMessage, error) {
	params := paramtable.Get()
	// Match Insert/Delete: after Proxy packing is disabled, SN owns physical
	// chunking. Do not consult backend limits on that path.
	limit := 0
	if params.ProxyCfg.SplitChunkProxy.GetAsBool() {
		if backendLimit := params.WALMaxMessageSize(walutil.MustSelectWALName().String()); backendLimit > 0 {
			_, reserve := params.PulsarCfg.GetMessageSizeLimitsFor(backendLimit)
			limit = backendLimit - reserve
		}
	}
	hashes, err := typeutil.HashPK2Channels(ids, channels)
	if err != nil {
		return nil, err
	}
	groups := make([]*schemapb.IDs, len(channels))
	for i, hash := range hashes {
		if groups[hash] == nil {
			groups[hash] = &schemapb.IDs{}
		}
		typeutil.AppendIDs(groups[hash], ids, i)
	}
	var result []message.MutableMessage
	maxInsertSize := params.QuotaConfig.MaxInsertSize.GetAsInt()
	remaining := maxInsertSize
	header := &messagespb.UpdateMessageHeader{CollectionId: collectionID, SchemaVersion: proto.Int32(schemaVersion), PartitionId: partitionID}
	body := &messagespb.UpdateMessageBody{Assignments: assignments}
	fixedBodySize := proto.Size(body)
	// All length-delimited fields on the PK path have one-byte tags.
	fieldOverhead := protowire.SizeTag(1) + protowire.SizeVarint(uint64(limit))
	for channelIndex, keys := range groups {
		if keys == nil {
			continue
		}
		build := func(b *messagespb.UpdateMessageBody, c *message.CipherConfig) (message.MutableMessage, error) {
			return message.NewUpdateMessageBuilderV2().WithVChannel(channels[channelIndex]).WithHeader(header).
				WithBody(b).WithCipher(c).BuildMutable()
		}
		pkBudget := 0
		if limit > 0 {
			envelope, err := build(&messagespb.UpdateMessageBody{}, nil)
			if err != nil {
				return nil, err
			}
			// At most four wrappers: payload, primary_keys, IDs oneof, packed integers.
			pkBudget = limit - proto.Size(envelope.IntoMessageProto()) - fixedBodySize - 4*fieldOverhead
			if cipher != nil {
				const cipherReserve = 1024 // Per message, for ciphertext expansion and its header.
				pkBudget -= cipherReserve
			}
		}
		for start, count := 0, typeutil.GetSizeOfIDs(keys); start < count; {
			end := count
			if limit > 0 {
				end = updatePKBatchEnd(keys, start, pkBudget, fieldOverhead)
			}
			body.PrimaryKeys = sliceUpdatePKs(keys, start, end)
			msg, err := build(body, cipher)
			if err != nil {
				return nil, err
			}
			size := proto.Size(msg.IntoMessageProto())
			if limit > 0 && size > limit {
				return nil, merr.Wrapf(merr.ErrParameterTooLarge, "encoded update message is %d bytes, limit is %d bytes", size, limit)
			}
			if maxInsertSize != -1 {
				if size > remaining {
					return nil, merr.Wrapf(merr.ErrParameterTooLarge, "packed update messages exceed quotaAndLimits.limits.maxInsertSize (%d bytes)", maxInsertSize)
				}
				remaining -= size
			}
			result = append(result, msg)
			start = end
		}
	}
	return result, nil
}

// Try at least one PK so that the encoded size decides whether a single key fits.
func updatePKBatchEnd(keys *schemapb.IDs, start, budget, fieldOverhead int) int {
	if ints := keys.GetIntId(); ints != nil {
		count := max(1, budget/binary.MaxVarintLen64)
		return start + min(count, len(ints.Data)-start)
	}
	strs := keys.GetStrId().GetData()
	for i := start; i < len(strs); i++ {
		budget -= len(strs[i]) + fieldOverhead
		if budget < 0 {
			return max(start+1, i)
		}
	}
	return len(strs)
}

func sliceUpdatePKs(ids *schemapb.IDs, start, end int) *schemapb.IDs {
	if ids.GetIntId() != nil {
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids.GetIntId().GetData()[start:end]}}}
	}
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids.GetStrId().GetData()[start:end]}}}
}
