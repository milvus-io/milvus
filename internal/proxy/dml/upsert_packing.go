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
	"sort"

	"google.golang.org/protobuf/encoding/protowire"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// repackUpsertMessages groups only by VChannel and encoded size. Partition IDs
// remain per-row placement, independent of explicit deletion scope.
func repackUpsertMessages(ctx context.Context, cache Cache, channels []string, source *msgstream.InsertMsg, ids *schemapb.IDs, partitionKeys *schemapb.FieldData, schema *schemapb.CollectionSchema, version int32, cipher *message.CipherConfig, cas map[string]*messagespb.PartialUpdateCAS, idempotency *insertIdempotencyDecoration, deleted *schemapb.IDs, deletePartition int64) ([]message.MutableMessage, error) {
	if source.Namespace != nil {
		return nil, merr.WrapErrParameterInvalidMsg("Upsert WAL does not support namespace requests")
	}
	if err := message.ValidateUpsertSchema(schema); err != nil {
		return nil, err
	}
	routing, err := assignChannelsByPK(ids, channels, source)
	if err != nil {
		return nil, err
	}
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	var partitions []int64
	var fixedPartition int64
	if partitionKeys == nil {
		id, err := cache.GetPartitionID(ctx, source.GetDbName(), source.CollectionName, source.PartitionName)
		if err != nil {
			return nil, err
		}
		fixedPartition = id
	} else {
		partitions = make([]int64, source.GetNumRows())
		names, err := getDefaultPartitionsInPartitionKeyMode(ctx, cache, source.GetDbName(), source.CollectionName)
		if err != nil {
			return nil, err
		}
		hashes, err := typeutil.HashKey2Partitions(partitionKeys, names)
		if err != nil {
			return nil, err
		}
		table := make([]int64, len(names))
		for i, name := range names {
			id, err := cache.GetPartitionID(ctx, source.GetDbName(), source.CollectionName, name)
			if err != nil {
				return nil, err
			}
			table[i] = id
		}
		for i, index := range hashes {
			partitions[i] = table[index]
		}
	}
	columns := make([]*schemapb.FieldData, 0, len(source.GetFieldsData()))
	for _, field := range source.GetFieldsData() {
		if field.GetFieldId() > 1 && field.GetFieldId() != pk.GetFieldID() {
			columns = append(columns, field)
		}
	}
	viewSource := &msgpb.InsertRequest{NumRows: source.GetNumRows(), RowIDs: source.GetRowIDs(), Timestamps: source.GetTimestamps(), FieldsData: columns, Version: msgpb.InsertDataVersion_ColumnBased}
	// Deletes prepared by the current Proxy refer to preserved existing keys.
	// Newly allocated AutoIDs have no old-version delete effect.
	// Assign every delete occurrence once to the first inserted occurrence of
	// its PK. A size-plan boundary never consumes or rescans a key's offsets.
	deleteRows, err := matchUpsertDeletes(ids, deleted)
	if err != nil {
		return nil, err
	}
	var result []message.MutableMessage
	for _, channel := range channels {
		rows := routing[channel]
		if len(rows) == 0 {
			continue
		}
		cursor, err := fastpb.NewUpsertViewCursor(viewSource)
		if err != nil {
			return nil, err
		}
		fixedBytes := 0
		bounded := paramtable.Get().ProxyCfg.SplitChunkProxy.GetAsBool() && paramtable.Get().PulsarCfg.MaxMessageSize.GetAsInt() > 0
		if bounded {
			// Build only a tiny envelope probe. No candidate column payload is encoded
			// or encrypted. The reserve covers changing packed-length varints.
			probeBody := &message.UpsertMessageBody{PartialUpdateCas: cas[channel]}
			probeBuilder := message.NewUpsertMessageBuilderV2().WithVChannel(channel).WithHeader(&message.UpsertMessageHeader{CollectionId: source.GetCollectionID(), SchemaVersion: &version}).WithBody(probeBody).WithCipher(cipher).WithIdempotencyKey(idempotency.idempotencyKey())
			if cas != nil {
				probeBuilder.WithProperty("_puc", "")
			}
			probe, err := probeBuilder.BuildMutable()
			if err != nil {
				return nil, err
			}
			fixedBytes = upsertWireSize(probe) + 32
		}
		prefix := make([]int, len(rows)+1)
		for index, row := range rows {
			if !bounded {
				continue
			}
			size, err := upsertPlainRowMetadataSize(ids, row, source.RowIDs[row], uint32(len(rows)), uint32(row))
			if err != nil {
				return nil, err
			}
			if deleteRows != nil {
				effectsSize, err := upsertDeleteMetadataSize(deleted, deleteRows[row], deletePartition)
				if err != nil {
					return nil, err
				}
				size += effectsSize
			}
			// Keep the original row framing and partition/envelope reserve.
			prefix[index+1] = prefix[index] + size + 64
		}
		for len(rows) > 0 {
			limit := paramtable.Get().PulsarCfg.MaxMessageSize.GetAsInt()
			if !paramtable.Get().ProxyCfg.SplitChunkProxy.GetAsBool() {
				limit = 0
			}
			encoder, count, err := cursor.NextEncoder(rows, limit, prefix, fixedBytes)
			if err != nil {
				return nil, err
			}
			selected := rows[:count]
			keys, err := idsByOffsets(ids, selected)
			if err != nil {
				return nil, err
			}
			body := &message.UpsertMessageBody{PrimaryKeys: keys, RowIds: make([]int64, count)}
			header := &message.UpsertMessageHeader{CollectionId: source.GetCollectionID(), SchemaVersion: &version}
			table := map[int64]uint32{}
			if idempotency.enabled() {
				body.RequestRowMapping = &messagespb.UpsertRequestRowMapping{RowOffsets: make([]uint32, count)}
			}
			if partitions == nil {
				header.Partitions = []*message.PartitionSegmentAssignment{{PartitionId: fixedPartition, Rows: uint64(count)}}
			}
			for i, row := range selected {
				body.RowIds[i] = source.GetRowIDs()[row]
				if body.RequestRowMapping != nil {
					body.RequestRowMapping.RowOffsets[i] = uint32(row)
				}
				if partitions == nil {
					continue
				}
				part := partitions[row]
				index, ok := table[part]
				if !ok {
					index = uint32(len(header.Partitions))
					table[part] = index
					header.Partitions = append(header.Partitions, &message.PartitionSegmentAssignment{PartitionId: part})
					// Earlier rows all use index zero. Allocate only when a
					// second partition makes per-row indexes necessary.
					if index == 1 {
						body.PartitionIndexes = make([]uint32, count)
					}
				}
				header.Partitions[index].Rows++
				if body.PartitionIndexes != nil {
					body.PartitionIndexes[i] = index
				}
			}
			if cas != nil {
				proof, ok := cas[channel]
				if !ok || proof == nil {
					return nil, merr.WrapErrServiceInternalMsg("missing partial update CAS proof")
				}
				body.PartialUpdateCas = proof
			}
			var deleteOffsets []int
			if deleteRows != nil {
				for _, row := range selected {
					deleteOffsets = append(deleteOffsets, deleteRows[row]...)
				}
			}
			sort.Ints(deleteOffsets)
			if len(deleteOffsets) > 0 {
				keys, err := idsByOffsets(deleted, deleteOffsets)
				if err != nil {
					return nil, err
				}
				effect := &messagespb.UpsertDeleteEffect{PrimaryKeys: keys}
				if deletePartition == -1 {
					effect.Scope = &messagespb.UpsertDeleteEffect_AllPartitions{AllPartitions: &messagespb.UpsertAllPartitions{}}
				} else {
					effect.Scope = &messagespb.UpsertDeleteEffect_PartitionId{PartitionId: deletePartition}
				}
				body.DeleteEffects = []*messagespb.UpsertDeleteEffect{effect}
				header.DeleteRows = uint64(len(deleteOffsets))
			}
			if err := encoder.SetMetadata(body); err != nil {
				return nil, err
			}
			builder := message.NewUpsertMessageBuilderV2().WithHeader(header).WithBodyEncoder(encoder).WithVChannel(channel).WithCipher(cipher).WithIdempotencyKey(idempotency.idempotencyKey())
			if body.PartialUpdateCas != nil {
				builder.WithProperty("_puc", "")
			}
			built, err := builder.BuildMutable()
			if err != nil {
				return nil, err
			}
			// A plain indivisible row retains backend chunking. Other writes
			// must fit after the final cipher/envelope is known. Never reencode.
			actualSize := upsertWireSize(built)
			chunkedRow := count == 1 && cas == nil && !idempotency.enabled()
			if limit > 0 && actualSize > limit && !chunkedRow {
				return nil, merr.WrapErrParameterTooLarge("one Upsert row or cipher envelope exceeds the WAL message size limit")
			}
			result = append(result, built)
			rows = rows[count:]
			prefix = prefix[count:]
		}
	}
	return result, nil
}

// matchUpsertDeletes visits each inserted key and each delete occurrence once.
// Original delete offsets preserve occurrences and ordering across size chunks.
func matchUpsertDeletes(inserted, deleted *schemapb.IDs) ([][]int, error) {
	if typeutil.GetSizeOfIDs(deleted) == 0 {
		return nil, nil
	}
	rows := make([][]int, typeutil.GetSizeOfIDs(inserted))
	byInt := make(map[int64]int)
	byString := make(map[string]int)
	for row, key := range inserted.GetIntId().GetData() {
		if _, ok := byInt[key]; !ok {
			byInt[key] = row
		}
	}
	for row, key := range inserted.GetStrId().GetData() {
		if _, ok := byString[key]; !ok {
			byString[key] = row
		}
	}
	for offset, key := range deleted.GetIntId().GetData() {
		row, ok := byInt[key]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Upsert deletion has no corresponding preserved row")
		}
		rows[row] = append(rows[row], offset)
	}
	for offset, key := range deleted.GetStrId().GetData() {
		row, ok := byString[key]
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("Upsert deletion has no corresponding preserved row")
		}
		rows[row] = append(rows[row], offset)
	}
	return rows, nil
}

func upsertWireSize(msg message.MutableMessage) int {
	wire := msg.IntoMessageProto()
	size := len(wire.Payload)
	for key, value := range wire.Properties {
		size += len(key) + len(value)
	}
	return size
}

// upsertPlainRowMetadataSize equals proto.Size of the existing conservative
// one-row metadata object. It keeps its framing and budget unchanged.
func upsertPlainRowMetadataSize(ids *schemapb.IDs, row int, rowID int64, partitionIndex, requestOffset uint32) (int, error) {
	keysSize, err := upsertKeysSize(ids, []int{row})
	if err != nil {
		return 0, err
	}
	mappingSize := protowire.SizeTag(1) + protowire.SizeBytes(protowire.SizeVarint(uint64(requestOffset)))
	return protowire.SizeTag(1) + protowire.SizeBytes(keysSize) +
		protowire.SizeTag(2) + protowire.SizeBytes(protowire.SizeVarint(uint64(rowID))) +
		protowire.SizeTag(3) + protowire.SizeBytes(protowire.SizeVarint(uint64(partitionIndex))) +
		protowire.SizeTag(7) + protowire.SizeBytes(mappingSize), nil
}

// upsertKeysSize computes IDs framing without copying a nonempty selection.
func upsertKeysSize(ids *schemapb.IDs, offsets []int) (int, error) {
	if ids == nil {
		return 0, merr.WrapErrServiceInternalMsg("missing mutation result ids")
	}
	if values := ids.GetIntId(); values != nil {
		payload := 0
		for _, offset := range offsets {
			if offset < 0 || offset >= len(values.GetData()) {
				return 0, merr.WrapErrServiceInternalMsg("row offset %d out of int id range %d", offset, len(values.GetData()))
			}
			payload += protowire.SizeVarint(uint64(values.GetData()[offset]))
		}
		arraySize := protowire.SizeTag(1) + protowire.SizeBytes(payload)
		return protowire.SizeTag(1) + protowire.SizeBytes(arraySize), nil
	}
	if values := ids.GetStrId(); values != nil {
		arraySize := 0
		for _, offset := range offsets {
			if offset < 0 || offset >= len(values.GetData()) {
				return 0, merr.WrapErrServiceInternalMsg("row offset %d out of string id range %d", offset, len(values.GetData()))
			}
			arraySize += protowire.SizeTag(1) + protowire.SizeBytes(len(values.GetData()[offset]))
		}
		return protowire.SizeTag(2) + protowire.SizeBytes(arraySize), nil
	}
	return 0, merr.WrapErrServiceInternalMsg("unsupported mutation result ids type")
}

// upsertDeleteMetadataSize preserves the original sizing object's partition_id
// oneof, including -1. The actual message uses all_partitions for that scope.
func upsertDeleteMetadataSize(ids *schemapb.IDs, offsets []int, partitionID int64) (int, error) {
	if len(offsets) == 0 {
		return 0, nil
	}
	keysSize, err := upsertKeysSize(ids, offsets)
	if err != nil {
		return 0, err
	}
	effectSize := protowire.SizeTag(2) + protowire.SizeVarint(uint64(partitionID)) + protowire.SizeTag(3) + protowire.SizeBytes(keysSize)
	return protowire.SizeTag(5) + protowire.SizeBytes(effectSize), nil
}
