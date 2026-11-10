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

package message

import (
	"context"
	"sync"

	"github.com/cockroachdb/errors"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/fastpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// ReadInsertOrUpsertProjection reads PK, RowID, partition indexes, delete effects,
// request row mapping, CAS and column descriptors without decoding ordinary
// column values. Descriptors contain only field IDs. The returned projection
// is read-only and does not retain the plaintext payload.
func ReadInsertOrUpsertProjection(ctx context.Context, msg BasicMessage) (*InsertOrUpsertProjection, error) {
	if msg == nil || !IsInsertOrUpsertMessage(msg) {
		return nil, merr.WrapErrServiceInternalMsg("expected an Insert V2 or Upsert V2 message")
	}
	reader, ok := msg.(interface {
		readInsertOrUpsertProjection(context.Context) (*InsertOrUpsertProjection, error)
	})
	if !ok {
		return nil, merr.WrapErrServiceInternalMsg("row projection reader is unavailable")
	}
	projection, err := reader.readInsertOrUpsertProjection(ctx)
	if err != nil {
		return nil, err
	}
	return projection, nil
}

func (m *messageImpl) readInsertOrUpsertProjection(ctx context.Context) (*InsertOrUpsertProjection, error) {
	if m.insertOrUpsertProjectionCache == nil {
		m.insertOrUpsertProjectionCache = newBodyCacheSlot()
	}
	return cachedInsertOrUpsertProjection(ctx, m, m.insertOrUpsertProjectionCache)
}

func (m *immutableMessageImpl) readInsertOrUpsertProjection(ctx context.Context) (*InsertOrUpsertProjection, error) {
	cache := globalBodyCache()
	shard := &cache.shards[m.bodyCache.shard]
	shard.mu.Lock()
	if m.bodyCache.insertOrUpsertProjectionCache == nil {
		m.bodyCache.insertOrUpsertProjectionCache = &bodyCacheSlot{shard: m.bodyCache.shard}
	}
	slot := m.bodyCache.insertOrUpsertProjectionCache
	shard.mu.Unlock()
	return cachedInsertOrUpsertProjection(ctx, m, slot)
}

func cachedInsertOrUpsertProjection(ctx context.Context, msg interface {
	BasicMessage
	decodePayload(context.Context) ([]byte, error)
}, slot *bodyCacheSlot,
) (*InsertOrUpsertProjection, error) {
	result, err := globalBodyCache().get(ctx, slot, func() (proto.Message, int, error) {
		payload, err := msg.decodePayload(ctx)
		if err != nil {
			return nil, 0, err
		}
		body, err := decodeInsertOrUpsertProjection(payload, msg.MessageType() == MessageTypeUpsert)
		if err != nil {
			return nil, 0, errors.Wrap(ErrMalformedBody, err.Error())
		}
		// Include slice storage and column wrappers in addition to encoded values.
		// The shared cache applies its existing object-overhead multiplier.
		size := proto.Size(body) + 8*len(body.GetRows().GetRowIds().GetData())*(2+len(body.GetRows().GetColumns())) + 4*len(body.GetRows().GetPartitionIndexes()) + 8*UpsertPrimaryKeyCount(body.GetRows().GetPrimaryKeys()) + 192*len(body.GetRows().GetColumns())
		for _, effect := range body.GetDeleteEffects() {
			size += 8*UpsertPrimaryKeyCount(effect.GetPrimaryKeys()) + 64
		}
		size += 4 * len(body.GetRows().GetRequestRowMapping().GetRowOffsets())
		return &InsertOrUpsertProjection{UpsertMessageBody: body}, size, nil
	})
	if err != nil {
		return nil, err
	}
	return result.(*InsertOrUpsertProjection), nil
}

func upsertWireFields(payload []byte, visit func(protowire.Number, protowire.Type, []byte, []byte) error) error {
	for len(payload) > 0 {
		number, kind, n := protowire.ConsumeTag(payload)
		if n < 0 {
			return protowire.ParseError(n)
		}
		length := protowire.ConsumeFieldValue(number, kind, payload[n:])
		if length < 0 {
			return protowire.ParseError(length)
		}
		if err := visit(number, kind, payload[:n+length], payload[n:n+length]); err != nil {
			return err
		}
		payload = payload[n+length:]
	}
	return nil
}

func decodeInsertOrUpsertProjection(payload []byte, upsert bool) (*UpsertMessageBody, error) {
	body := &UpsertMessageBody{}
	merge := proto.UnmarshalOptions{Merge: true}
	err := upsertWireFields(payload, func(number protowire.Number, kind protowire.Type, encoded, value []byte) error {
		if number == 1 {
			if kind != protowire.BytesType {
				return merr.WrapErrServiceInternalMsg("row-write rows have an invalid wire type")
			}
			raw, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return protowire.ParseError(n)
			}
			if body.Rows == nil {
				body.Rows = &InsertRows{}
			}
			return decodeInsertRowsProjection(raw, body.Rows)
		}
		if upsert && (number == 2 || number == 3) {
			return merge.Unmarshal(encoded, body)
		}
		return nil
	})
	if err == nil {
		err = fastpb.NormalizeInsertOrUpsertBody(body)
	}
	return body, err
}

func decodeInsertRowsProjection(payload []byte, rows *InsertRows) error {
	merge := proto.UnmarshalOptions{Merge: true}
	return upsertWireFields(payload, func(number protowire.Number, kind protowire.Type, encoded, value []byte) error {
		switch number {
		case 4:
			if kind != protowire.BytesType {
				return merr.WrapErrServiceInternalMsg("row-write column has an invalid wire type")
			}
			raw, n := protowire.ConsumeBytes(value)
			if n < 0 {
				return protowire.ParseError(n)
			}
			column := &schemapb.FieldData{}
			if err := upsertWireFields(raw, func(field protowire.Number, _ protowire.Type, encoded, _ []byte) error {
				if field == 5 {
					return merge.Unmarshal(encoded, column)
				}
				return nil
			}); err != nil {
				return err
			}
			rows.Columns = append(rows.Columns, column)
		case 1, 2, 3, 5, 7:
			return merge.Unmarshal(encoded, rows)
		}
		return nil
	})
}

// ReplaceUpsertDeleteEffects replaces explicit effects while copying the other
// encoded fields unchanged. Encryption still covers the complete payload.
func ReplaceUpsertDeleteEffects(ctx context.Context, msg MutableUpsertMessageV2, effects []*messagespb.UpsertDeleteEffect) error {
	raw, err := msg.(interface {
		decodePayload(context.Context) ([]byte, error)
	}).decodePayload(ctx)
	if err != nil {
		return err
	}
	// Reuse this plaintext: replacement invalidates any existing projection.
	projection, err := decodeInsertOrUpsertProjection(raw, true)
	if err != nil {
		return errors.Wrap(ErrMalformedBody, err.Error())
	}
	payload := make([]byte, 0, len(raw))
	if err := upsertWireFields(raw, func(number protowire.Number, _ protowire.Type, encoded, _ []byte) error {
		if number != 2 {
			payload = append(payload, encoded...)
		}
		return nil
	}); err != nil {
		return errors.Wrap(ErrMalformedBody, err.Error())
	}
	suffix, err := proto.Marshal(&UpsertMessageBody{DeleteEffects: fastpb.CompactDeleteEffects(projection.GetRows().GetPrimaryKeys(), effects)})
	if err != nil {
		return err
	}
	payload = append(payload, suffix...)
	if err := msg.(interface{ replaceUpsertPayload([]byte) error }).replaceUpsertPayload(payload); err != nil {
		return err
	}
	header := msg.Header()
	header.DeleteRows = 0
	for _, effect := range effects {
		keys := effect.GetPrimaryKeys()
		if effect.GetAllInsertedKeys() != nil {
			keys = projection.GetRows().GetPrimaryKeys()
		}
		header.DeleteRows += uint64(UpsertPrimaryKeyCount(keys))
	}
	msg.OverwriteHeader(header)
	return nil
}

func (m *messageImpl) replaceUpsertPayload(payload []byte) error {
	var cipherHeader string
	if ch, err := m.decodeCipherHeader(); err != nil {
		return err
	} else if ch != nil {
		encryptor, key, err := mustGetCipher().GetEncryptor(ch.EzId, ch.CollectionId)
		if err != nil {
			return err
		}
		size := len(payload)
		payload, err = encryptor.Encrypt(payload)
		if err != nil {
			return err
		}
		cipherHeader, err = EncodeProto(&messagespb.CipherHeader{EzId: ch.EzId, CollectionId: ch.CollectionId, SafeKey: key, PayloadBytes: int64(size)})
		if err != nil {
			return err
		}
	}
	if cipherHeader != "" {
		m.properties.Set(messageCipherHeader, cipherHeader)
	}
	m.payload = payload
	m.insertOrUpsertProjectionCache = nil
	return nil
}

// InsertOrUpsertProjection owns derived row mappings with the cached projection. It never
// retains ordinary column values or plaintext. Eviction releases derived mappings too.
type InsertOrUpsertProjection struct {
	*UpsertMessageBody
	mu           sync.Mutex
	placementKey string
	rows         [][]int64
	placementErr error
	compact      map[int64][]int64
}

func insertOrUpsertProjectionOf(ctx context.Context, msg BasicMessage) (*InsertOrUpsertProjection, *InsertOrUpsertHeader, error) {
	projection, err := ReadInsertOrUpsertProjection(ctx, msg)
	if err != nil {
		return nil, nil, err
	}
	header, err := ReadInsertOrUpsertHeader(msg)
	return projection, header, err
}

func insertOrUpsertProjectionKey(header *InsertOrUpsertHeader) string {
	var key []byte
	key = protowire.AppendVarint(key, uint64(header.GetCollectionId()))
	key = protowire.AppendVarint(key, uint64(header.GetSchemaVersion()))
	if header.SchemaVersion == nil {
		key = append(key, 0)
	} else {
		key = append(key, 1)
	}
	key = protowire.AppendVarint(key, header.GetDeleteRows())
	key = protowire.AppendVarint(key, uint64(len(header.GetPartitions())))
	for _, part := range header.GetPartitions() {
		key = protowire.AppendVarint(key, uint64(part.GetPartitionId()))
		key = protowire.AppendVarint(key, part.GetRows())
	}
	return string(key)
}

// ReadInsertOrUpsertRowSelections shares validated placement across segment consumers.
// Returned selections are immutable. Physical segment IDs do not affect them.
func ReadInsertOrUpsertRowSelections(ctx context.Context, msg BasicMessage) ([][]int64, error) {
	projection, header, err := insertOrUpsertProjectionOf(ctx, msg)
	if err != nil {
		return nil, err
	}
	key := insertOrUpsertProjectionKey(header)
	projection.mu.Lock()
	defer projection.mu.Unlock()
	if projection.placementKey == "" {
		projection.rows, projection.placementErr = UpsertSelections(&UpsertMessageHeader{CollectionId: header.CollectionId, Partitions: header.Partitions, SchemaVersion: header.SchemaVersion, DeleteRows: header.DeleteRows}, projection.UpsertMessageBody)
		projection.placementKey = key
	}
	if projection.placementKey != key {
		return UpsertSelections(&UpsertMessageHeader{CollectionId: header.CollectionId, Partitions: header.Partitions, SchemaVersion: header.SchemaVersion, DeleteRows: header.DeleteRows}, projection.UpsertMessageBody)
	}
	return projection.rows, projection.placementErr
}

// ReadInsertOrUpsertCompactRows shares nullable-vector logical-to-physical positions.
// The cache stores positions only and does not retain the supplied column.
func ReadInsertOrUpsertCompactRows(ctx context.Context, msg BasicMessage, fieldID int64, valid []bool) ([]int64, error) {
	projection, err := ReadInsertOrUpsertProjection(ctx, msg)
	if err != nil {
		return nil, err
	}
	if len(valid) != len(projection.GetRows().GetRowIds().GetData()) {
		return nil, merr.WrapErrServiceInternalMsg("nullable vector validity count differs from logical rows")
	}
	projection.mu.Lock()
	defer projection.mu.Unlock()
	if positions, ok := projection.compact[fieldID]; ok {
		return positions, nil
	}
	positions := make([]int64, len(valid))
	cursor := int64(0)
	for i, exists := range valid {
		positions[i] = -1
		if exists {
			positions[i] = cursor
			cursor++
		}
	}
	if projection.compact == nil {
		projection.compact = make(map[int64][]int64)
	}
	projection.compact[fieldID] = positions
	return positions, nil
}
