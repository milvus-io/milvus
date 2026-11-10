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
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/proxy/dql"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// validateUpsertNamespace checks the namespace restrictions of the selected WAL format.
func validateUpsertNamespace(namespace *string, schema *schemapb.CollectionSchema) error {
	if namespace != nil {
		return merr.WrapErrParameterInvalidMsg("Upsert WAL does not support namespace requests")
	}
	return message.ValidateUpsertSchema(schema)
}

// repackInsertMessages encodes ordinary Insert rows without delete effects or CAS.
func repackInsertMessages(ctx context.Context, cache Cache, channels []string, source *msgstream.InsertMsg, ids *schemapb.IDs, partitionKeys *schemapb.FieldData, schema *schemapb.CollectionSchema, version int32, cipher *message.CipherConfig, key message.IdempotencyKey) ([]message.MutableMessage, error) {
	return packInsertOrUpsertMessages(ctx, cache, channels, source, ids, partitionKeys, schema, version, cipher, nil, key, nil, 0, message.MessageTypeInsert)
}

// repackUpsertMessages encodes rows with explicit delete effects and optional CAS.
func repackUpsertMessages(ctx context.Context, cache Cache, channels []string, source *msgstream.InsertMsg, ids *schemapb.IDs, partitionKeys *schemapb.FieldData, schema *schemapb.CollectionSchema, version int32, cipher *message.CipherConfig, cas map[string]*messagespb.PartialUpdateCAS, key message.IdempotencyKey, deleted *schemapb.IDs, deletePartition int64) ([]message.MutableMessage, error) {
	return packInsertOrUpsertMessages(ctx, cache, channels, source, ids, partitionKeys, schema, version, cipher, cas, key, deleted, deletePartition, message.MessageTypeUpsert)
}

// packInsertOrUpsertMessages builds one logical write per nonempty VChannel.
// Partition placement is independent of the explicit Upsert deletion scope.
func packInsertOrUpsertMessages(ctx context.Context, cache Cache, channels []string, source *msgstream.InsertMsg, ids *schemapb.IDs, partitionKeys *schemapb.FieldData, schema *schemapb.CollectionSchema, version int32, cipher *message.CipherConfig, cas map[string]*messagespb.PartialUpdateCAS, key message.IdempotencyKey, deleted *schemapb.IDs, deletePartition int64, typ message.MessageType) ([]message.MutableMessage, error) {
	hashes, err := typeutil.HashPK2Channels(ids, channels)
	if err != nil {
		return nil, err
	}
	source.HashValues = hashes
	routing := groupRowsByChannel(hashes, len(channels))
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	partitions, partitionIndexes, err := resolveInsertPartitions(ctx, cache, source, partitionKeys)
	if err != nil {
		return nil, err
	}
	columns := make([]*schemapb.FieldData, 0, len(source.GetFieldsData()))
	for _, field := range source.GetFieldsData() {
		if field.GetFieldId() > 1 && field.GetFieldId() != pk.GetFieldID() {
			columns = append(columns, field)
		}
	}
	viewSource := &msgpb.InsertRequest{NumRows: source.GetNumRows(), RowIDs: source.GetRowIDs(), Timestamps: source.GetTimestamps(), FieldsData: columns, Version: msgpb.InsertDataVersion_ColumnBased}
	// Current Proxy deletes refer to preserved existing keys. Group their
	// occurrences by VChannel in original delete order.
	deleteOffsets, err := matchUpsertDeletes(ids, deleted, source.HashValues)
	if err != nil {
		return nil, err
	}
	result := make([]message.MutableMessage, 0, len(channels))
	for channelIndex, channel := range channels {
		selected := routing[channelIndex]
		if len(selected) == 0 {
			continue
		}
		rows, assignments, err := packInsertRows(source, ids, selected, key, partitions, partitionIndexes)
		if err != nil {
			return nil, err
		}
		var built message.MutableMessage
		switch typ {
		case message.MessageTypeInsert:
			encoder, err := fastpb.NewInsertMessageViewEncoder(&messagespb.InsertMessageBody{Rows: rows}, viewSource, selected)
			if err != nil {
				return nil, err
			}
			built, err = message.NewInsertMessageBuilderV2().WithVChannel(channel).WithHeader(&message.InsertMessageHeaderV2{CollectionId: source.GetCollectionID(), Partitions: assignments, SchemaVersion: &version}).WithBodyEncoder(encoder).WithCipher(cipher).WithIdempotencyKey(key).BuildMutable()
			if err != nil {
				return nil, err
			}
		case message.MessageTypeUpsert:
			body := &message.UpsertMessageBody{Rows: rows}
			if cas != nil {
				proof, ok := cas[channel]
				if !ok || proof == nil {
					return nil, merr.WrapErrServiceInternalMsg("missing partial update CAS proof")
				}
				body.PartialUpdateCas = proof
			}
			offsets := deleteOffsets[uint32(channelIndex)]
			if len(offsets) > 0 {
				deleteKeys, err := idsByOffsets(deleted, offsets)
				if err != nil {
					return nil, err
				}
				effect := &messagespb.UpsertDeleteEffect{KeySource: &messagespb.UpsertDeleteEffect_PrimaryKeys{PrimaryKeys: deleteKeys}}
				if deletePartition == -1 {
					effect.Scope = &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}
				} else {
					effect.Scope = &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: deletePartition}
				}
				body.DeleteEffects = []*messagespb.UpsertDeleteEffect{effect}
			}
			encoder, err := fastpb.NewUpsertViewEncoder(body, viewSource, selected)
			if err != nil {
				return nil, err
			}
			builder := message.NewUpsertMessageBuilderV2().WithVChannel(channel).WithHeader(&message.UpsertMessageHeader{CollectionId: source.GetCollectionID(), Partitions: assignments, SchemaVersion: &version, DeleteRows: uint64(len(offsets))}).WithBodyEncoder(encoder).WithCipher(cipher).WithIdempotencyKey(key)
			if body.PartialUpdateCas != nil {
				builder.WithProperty("_puc", "")
			}
			built, err = builder.BuildMutable()
			if err != nil {
				return nil, err
			}
		default:
			panic("unexpected row write message type")
		}
		result = append(result, built)
	}
	return result, nil
}

// groupRowsByChannel preserves request order within each routed VChannel.
func groupRowsByChannel(channels []uint32, channelCount int) [][]int {
	groups := make([][]int, channelCount)
	for row, channel := range channels {
		groups[channel] = append(groups[channel], row)
	}
	return groups
}

// resolveInsertPartitions resolves the request placement before VChannel packing.
// A fixed partition has no per-row indexes.
func resolveInsertPartitions(ctx context.Context, cache Cache, source *msgstream.InsertMsg, partitionKeys *schemapb.FieldData) ([]int64, []uint32, error) {
	if partitionKeys == nil {
		partition, err := cache.GetPartitionID(ctx, source.GetDbName(), source.CollectionName, source.PartitionName)
		return []int64{partition}, nil, err
	}
	names, err := getDefaultPartitionsInPartitionKeyMode(ctx, cache, source.GetDbName(), source.CollectionName)
	if err != nil {
		return nil, nil, err
	}
	indexes, err := typeutil.HashKey2Partitions(partitionKeys, names)
	if err != nil {
		return nil, nil, err
	}
	partitions := make([]int64, len(names))
	for index, name := range names {
		partitions[index], err = cache.GetPartitionID(ctx, source.GetDbName(), source.CollectionName, name)
		if err != nil {
			return nil, nil, err
		}
	}
	return partitions, indexes, nil
}

// packInsertRows selects row identity and builds this VChannel's partition table.
func packInsertRows(source *msgstream.InsertMsg, ids *schemapb.IDs, selected []int, key message.IdempotencyKey, partitions []int64, partitionIndexes []uint32) (*messagespb.InsertRows, []*message.PartitionSegmentAssignment, error) {
	keys, err := idsByOffsets(ids, selected)
	if err != nil {
		return nil, nil, err
	}
	rows := &messagespb.InsertRows{PrimaryKeys: keys, RowIdsEncoding: &messagespb.InsertRows_RowIds{RowIds: &schemapb.LongArray{Data: make([]int64, len(selected))}}}
	if key != "" {
		rows.RequestRowMapping = &messagespb.RequestRowMapping{RowOffsets: make([]uint32, len(selected))}
	}
	for index, row := range selected {
		rows.GetRowIds().Data[index] = source.GetRowIDs()[row]
		if rows.RequestRowMapping != nil {
			rows.RequestRowMapping.RowOffsets[index] = uint32(row)
		}
	}
	if partitionIndexes == nil {
		return rows, []*message.PartitionSegmentAssignment{{PartitionId: partitions[0], Rows: uint64(len(selected))}}, nil
	}
	var assignments []*message.PartitionSegmentAssignment
	localIndexes := make(map[uint32]uint32)
	for index, row := range selected {
		partition := partitionIndexes[row]
		local, exists := localIndexes[partition]
		if !exists {
			local = uint32(len(assignments))
			localIndexes[partition] = local
			assignments = append(assignments, &message.PartitionSegmentAssignment{PartitionId: partitions[partition]})
			if local == 1 {
				rows.PartitionIndexes = make([]uint32, len(selected))
			}
		}
		assignments[local].Rows++
		if rows.PartitionIndexes != nil {
			rows.PartitionIndexes[index] = local
		}
	}
	return rows, assignments, nil
}

// matchUpsertDeletes groups each delete occurrence by its preserved row's
// VChannel. channels comes from PK routing, so duplicate keys share a channel.
// Appending in original delete order keeps occurrences ordered.
func matchUpsertDeletes(inserted, deleted *schemapb.IDs, channels []uint32) (map[uint32][]int, error) {
	if typeutil.GetSizeOfIDs(deleted) == 0 {
		return nil, nil
	}
	offsets := make(map[uint32][]int)
	byInt := make(map[int64]uint32)
	byString := make(map[string]uint32)
	for row, key := range inserted.GetIntId().GetData() {
		byInt[key] = channels[row]
	}
	for row, key := range inserted.GetStrId().GetData() {
		byString[key] = channels[row]
	}
	for offset, key := range deleted.GetIntId().GetData() {
		channel, ok := byInt[key]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Upsert deletion has no corresponding preserved row")
		}
		offsets[channel] = append(offsets[channel], offset)
	}
	for offset, key := range deleted.GetStrId().GetData() {
		channel, ok := byString[key]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Upsert deletion has no corresponding preserved row")
		}
		offsets[channel] = append(offsets[channel], offset)
	}
	return offsets, nil
}

// checkMaxDeleteSize rejects a materialized per-vchannel delete message whose
// protobuf body exceeds quotaAndLimits.limits.maxDeleteSize.
func checkMaxDeleteSize(ctx context.Context, size int) error {
	maxDeleteSize := paramtable.Get().QuotaConfig.MaxDeleteSize.GetAsInt()
	if maxDeleteSize == -1 || size <= maxDeleteSize {
		return nil
	}
	mlog.Warn(ctx, "materialized delete message exceeds maxDeleteSize",
		mlog.Int("message size", size),
		mlog.Int("maxDeleteSize", maxDeleteSize))
	return merr.WrapErrAsInputError(merr.WrapErrParameterTooLarge(
		fmt.Sprintf("delete materialized message size %d exceeds maxDeleteSize %d", size, maxDeleteSize)))
}

func repackDeleteMessages(
	ctx context.Context,
	primaryKeys *schemapb.IDs,
	vChannels []string,
	idAllocator allocator.Interface,
	ts uint64,
	collectionID int64,
	collectionName string,
	partitionID int64,
	partitionName string,
	dbName string,
	namespace *string,
	schema *schemapb.CollectionSchema,
) ([]*msgstream.DeleteMsg, int64, error) {
	var hashValues []uint32
	// Delete tombstones are PK+timestamp based. Namespace can narrow routing,
	// but it is not part of the tombstone identity; PKs must stay unique across
	// namespaces in the same collection.
	channelID, ok, err := dql.NamespaceShardingChannelID(schema, namespace, vChannels)
	if err != nil {
		return nil, 0, err
	}
	if ok {
		size := typeutil.GetSizeOfIDs(primaryKeys)
		hashValues = make([]uint32, size)
		for i := 0; i < size; i++ {
			hashValues[i] = channelID
		}
	} else {
		hashValues, err = typeutil.HashPK2Channels(primaryKeys, vChannels)
		if err != nil {
			return nil, 0, err
		}
	}
	// Route first, then construct each complete logical message once.
	groups := groupRowsByChannel(hashValues, len(vChannels))
	messages := make([]*msgstream.DeleteMsg, 0, len(vChannels))
	for channel, vchannel := range vChannels {
		selected := groups[channel]
		if len(selected) == 0 {
			continue
		}
		keys, err := idsByOffsets(primaryKeys, selected)
		if err != nil {
			return nil, 0, err
		}
		timestamps := make([]uint64, len(selected))
		hashes := make([]uint32, len(selected))
		for index := range selected {
			timestamps[index] = ts
			hashes[index] = uint32(channel)
		}
		messages = append(messages, &msgstream.DeleteMsg{
			BaseMsg: msgstream.BaseMsg{Ctx: ctx, HashValues: hashes},
			DeleteRequest: &msgpb.DeleteRequest{
				Base: commonpbutil.NewMsgBase(
					commonpbutil.WithMsgType(commonpb.MsgType_Delete),
					commonpbutil.WithTimeStamp(ts),
					commonpbutil.WithSourceID(paramtable.GetNodeID()),
				),
				ShardName: vchannel, CollectionName: collectionName,
				PartitionName: partitionName, DbName: dbName,
				CollectionID: collectionID, PartitionID: partitionID,
				PrimaryKeys: keys, Timestamps: timestamps, NumRows: int64(len(selected)),
			},
		})
	}
	start, _, err := idAllocator.Alloc(uint32(len(messages)))
	if err != nil {
		return nil, 0, err
	}
	maxMessageSize := 0
	for index, msg := range messages {
		msg.Base.MsgID = start + int64(index)
		maxMessageSize = max(maxMessageSize, msg.Size())
	}
	if err := checkMaxDeleteSize(ctx, maxMessageSize); err != nil {
		return nil, 0, err
	}
	return messages, int64(len(hashValues)), nil
}

func idsByOffsets(ids *schemapb.IDs, rowOffsets []int) (*schemapb.IDs, error) {
	if ids == nil {
		return nil, merr.WrapErrServiceInternalMsg("missing mutation result ids")
	}
	if intIDs := ids.GetIntId(); intIDs != nil {
		data := intIDs.GetData()
		selected := make([]int64, 0, len(rowOffsets))
		for _, offset := range rowOffsets {
			if offset < 0 || offset >= len(data) {
				return nil, merr.WrapErrServiceInternalMsg("row offset %d out of int id range %d", offset, len(data))
			}
			selected = append(selected, data[offset])
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: selected}},
		}, nil
	}
	if strIDs := ids.GetStrId(); strIDs != nil {
		data := strIDs.GetData()
		selected := make([]string, 0, len(rowOffsets))
		for _, offset := range rowOffsets {
			if offset < 0 || offset >= len(data) {
				return nil, merr.WrapErrServiceInternalMsg("row offset %d out of string id range %d", offset, len(data))
			}
			selected = append(selected, data[offset])
		}
		return &schemapb.IDs{
			IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: selected}},
		}, nil
	}
	return nil, merr.WrapErrServiceInternalMsg("unsupported mutation result ids type")
}

func getDefaultPartitionsInPartitionKeyMode(ctx context.Context, metaCache Cache, dbName string, collectionName string) ([]string, error) {
	partitions, err := metaCache.GetPartitions(ctx, dbName, collectionName)
	if err != nil {
		return nil, err
	}

	// Make sure the order of the partition names got every time is the same
	partitionNames, _, err := typeutil.RearrangePartitionsForPartitionKey(partitions)
	if err != nil {
		return nil, err
	}

	return partitionNames, nil
}
