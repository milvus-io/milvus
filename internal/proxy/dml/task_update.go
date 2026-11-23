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

// UpdateRecords prepares every shard before starting any WAL append. The WAL
// producer groups fragments on one VChannel into its existing transaction.
func UpdateRecords(ctx context.Context, cache Cache, request *milvuspb.UpdateRecordsRequest) (*milvuspb.MutationResult, error) {
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
	for channelIndex, keys := range groups {
		if keys == nil {
			continue
		}
		build := func(start, end int) (message.MutableMessage, error) {
			return message.NewUpdateMessageBuilderV2().WithVChannel(channels[channelIndex]).WithCipher(cipher).
				WithHeader(&messagespb.UpdateMessageHeader{CollectionId: collectionID, SchemaVersion: proto.Int32(schemaVersion)}).
				WithBody(&messagespb.UpdateMessageBody{PartitionId: partitionID, PrimaryKeys: sliceUpdatePKs(keys, start, end), Assignments: assignments}).BuildMutable()
		}
		fixedSize := 0
		if limit > 0 {
			base, err := build(0, 0)
			if err != nil {
				return nil, err
			}
			fixedSize = proto.Size(base.IntoMessageProto())
		}
		for start, count := 0, typeutil.GetSizeOfIDs(keys); start < count; {
			end := count
			if limit > 0 {
				// Accumulate PK bytes as Delete does; assignments are fixed per pack.
				// Allow one key through for an exact indivisible-message size check.
				end = start
				for size := fixedSize; end < count; end++ {
					pkSize, _ := typeutil.GetId(keys, end)
					rowSize := 16 + pkSize // PK plus protobuf framing headroom.
					if end > start && size+rowSize > limit {
						break
					}
					size += rowSize
				}
			}
			var selected message.MutableMessage
			var size int
			for {
				msg, err := build(start, end)
				if err != nil {
					return nil, err
				}
				size = proto.Size(msg.IntoMessageProto())
				if limit == 0 || size <= limit {
					selected = msg
					break
				}
				if end-start == 1 {
					return nil, merr.WrapErrParameterTooLarge("one PK with shared assignments exceeds the update message size limit")
				}
				// Like Insert, shrink only if final encoding/encryption outgrows the
				// estimate. Do not search for a maximally filled message.
				end = start + (end-start)/2
			}
			// Assignments are encoded in every fragment. Bound the retained total,
			// not only the incoming request, before starting any WAL append.
			if size > remaining {
				return nil, merr.WrapErrParameterTooLarge("prepared update messages exceed the total byte budget")
			}
			remaining -= size
			result = append(result, selected)
			start = end
		}
	}
	return result, nil
}

func sliceUpdatePKs(ids *schemapb.IDs, start, end int) *schemapb.IDs {
	if ids.GetIntId() != nil {
		return &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: ids.GetIntId().GetData()[start:end]}}}
	}
	return &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: ids.GetStrId().GetData()[start:end]}}}
}
