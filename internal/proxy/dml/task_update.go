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

	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
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

// Update prepares every shard before starting any WAL append. The WAL
// producer groups fragments on one VChannel into its existing transaction.
func Update(ctx context.Context, cache Cache, request *milvuspb.UpdateRequest) (*milvuspb.MutationResult, error) {
	if request == nil {
		return nil, merr.WrapErrParameterInvalidMsg("missing update request")
	}
	info, err := cache.GetCollectionInfo(ctx, request.GetDbName(), request.GetCollectionName(), 0)
	if err != nil {
		return nil, err
	}
	schema, err := info.GetWriteSchema(ctx)
	if err != nil {
		return nil, err
	}
	// Namespace collections may route inserts by namespace instead of PK. They
	// need a namespace-aware mutation contract before this PK-only API can serve them.
	if schema.GetEnableNamespace() {
		return nil, merr.WrapErrParameterInvalidMsg("PK-update does not support namespace collections")
	}
	if err := validateUpdatePKs(request.GetPrimaryKeys(), schema.CollectionSchema); err != nil {
		return nil, err
	}
	assignments, err := validateUpdateAssignments(schema.CollectionSchema, request.GetAssignments())
	if err != nil {
		return nil, err
	}
	partitionID := int64(common.AllPartitionsID)
	if request.GetPartitionName() != "" {
		if partitionKey, _ := typeutil.GetPartitionKeyFieldSchema(schema.CollectionSchema); partitionKey != nil {
			return nil, merr.WrapErrParameterInvalidMsg("cannot specify a partition in partition-key mode")
		}
		partitionID, err = cache.GetPartitionID(ctx, request.GetDbName(), request.GetCollectionName(), request.GetPartitionName())
		if err != nil {
			return nil, err
		}
	}
	var cipher *message.CipherConfig
	if hookutil.IsClusterEncryptionEnabled() {
		cipher = hookutil.GetEzByCollProperties(schema.GetProperties(), info.CollID).AsMessageConfig()
	}
	msgs, err := buildUpdateMessages(info.CollID, schema.Version, partitionID, info.VChannels, request.GetPrimaryKeys(), assignments, cipher)
	if err != nil {
		return nil, err
	}
	resp := streaming.WAL().AppendMessages(ctx, msgs...)
	invalidateMismatchedWriteSchema(ctx, cache, info.CollID, resp)
	if err := resp.UnwrapFirstError(); err != nil {
		if status.AsStreamingError(err).IsSchemaVersionMismatch() {
			return nil, merr.ErrCollectionSchemaMismatch
		}
		return nil, err
	}
	return &milvuspb.MutationResult{Status: merr.Success(), Acknowledged: true, Timestamp: resp.MaxTimeTick(), UpdateCnt: int64(typeutil.GetSizeOfIDs(request.GetPrimaryKeys()))}, nil
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
	remaining := params.ProxyGrpcServerCfg.ServerMaxRecvSize.GetAsInt()
	header := &messagespb.UpdateMessageHeader{CollectionId: collectionID, SchemaVersion: proto.Int32(schemaVersion)}
	body := &messagespb.UpdateMessageBody{PartitionId: partitionID, Assignments: assignments}
	fixedBodySize := proto.Size(body)
	for channelIndex, keys := range groups {
		if keys == nil {
			continue
		}
		channel := channels[channelIndex]
		build := func(b *messagespb.UpdateMessageBody, c *message.CipherConfig) (message.MutableMessage, error) {
			return message.NewUpdateMessageBuilderV2().WithVChannel(channel).WithHeader(header).
				WithBody(b).WithCipher(c).BuildMutable()
		}
		var emit func(start, end int) error
		emit = func(start, end int) error {
			body.PrimaryKeys = sliceUpdatePKs(keys, start, end)
			msg, err := build(body, cipher)
			if err != nil {
				return err
			}
			size := proto.Size(msg.IntoMessageProto())
			if limit > 0 && size > limit {
				if end-start == 1 {
					return merr.WrapErrParameterTooLarge("one PK with shared assignments exceeds the update message size limit")
				}
				// Only encryption can expand a batch that fit the plaintext budget.
				mid := start + (end-start)/2
				if err := emit(start, mid); err != nil {
					return err
				}
				return emit(mid, end)
			}
			if size > remaining {
				return merr.WrapErrParameterTooLarge("prepared update messages exceed the total byte budget")
			}
			remaining -= size
			result = append(result, msg)
			return nil
		}

		start, count := 0, typeutil.GetSizeOfIDs(keys)
		if limit > 0 {
			envelope, err := build(&messagespb.UpdateMessageBody{}, nil)
			if err != nil {
				return nil, err
			}
			bodyBudget := bytesFieldCapacity(limit - proto.Size(envelope.IntoMessageProto()))
			idsBudget := bytesFieldCapacity(bodyBudget - fixedBodySize)
			pkBudget := bytesFieldCapacity(idsBudget) // IDs.int_id or IDs.str_id
			if keys.GetIntId() != nil {
				pkBudget = bytesFieldCapacity(pkBudget) // LongArray.data is packed
			}
			available := pkBudget
			for i := 0; i < count; i++ {
				keySize := updatePKWireSize(keys, i)
				if i > start && keySize > available {
					if err := emit(start, i); err != nil {
						return nil, err
					}
					start, available = i, pkBudget
				}
				available -= keySize
			}
		}
		if err := emit(start, count); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// bytesFieldCapacity returns the payload bytes that fit after a one-byte
// protobuf tag and its varint length prefix.
func bytesFieldCapacity(budget int) int {
	n := budget - 2
	for n > 0 && 1+protowire.SizeBytes(n) > budget {
		n--
	}
	return n
}

func updatePKWireSize(keys *schemapb.IDs, i int) int {
	if ints := keys.GetIntId(); ints != nil {
		return protowire.SizeVarint(uint64(ints.Data[i]))
	}
	return protowire.SizeTag(1) + protowire.SizeBytes(len(keys.GetStrId().Data[i]))
}

func sliceUpdatePKs(ids *schemapb.IDs, start, end int) *schemapb.IDs {
	if ids.GetIntId() != nil {
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids.GetIntId().GetData()[start:end]}}}
	}
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids.GetStrId().GetData()[start:end]}}}
}
