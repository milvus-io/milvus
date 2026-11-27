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

package l0materializer

import (
	"context"
	"maps"
	"slices"
	"sort"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/messageutil"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	defaultMaterializeMaxRows  = 500000
	defaultMaterializeMaxBytes = 32 * 1024 * 1024
)

type Materializer interface {
	Materialize(context.Context, MaterializeRequest) error
}

type MaterializeRequest struct {
	// Captured when scheduling; retries must use the same output format.
	LegacyFormat bool
	// Historical schemas pinned while the WAL bodies are observed.
	Schemas        map[uint64]*schemapb.CollectionSchema
	VChannel       string
	TargetTimeTick uint64
	Entries        []*streamingpb.TransformLogEntry
	// WAL consumers supply each Delete's complete position, used for both
	// endpoints of an output L0. Summary consumers only supply timestamps.
	DeletePositions map[uint64]*msgpb.MsgPosition
	MaxRows         uint64
	MaxBytes        uint64
}

type SyncMaterializer struct {
	chunkManager storage.ChunkManager
	allocator    allocator.Interface
	metaWriter   syncmgr.MetaWriter
}

func NewSyncMaterializer(
	chunkManager storage.ChunkManager,
	allocator allocator.Interface,
	metaWriter syncmgr.MetaWriter,
) *SyncMaterializer {
	return &SyncMaterializer{
		chunkManager: chunkManager,
		allocator:    allocator,
		metaWriter:   metaWriter,
	}
}

func (m *SyncMaterializer) Materialize(ctx context.Context, req MaterializeRequest) error {
	if len(req.Entries) == 0 {
		return nil
	}
	if m.chunkManager == nil {
		return merr.WrapErrServiceInternalMsg("chunk manager is nil")
	}
	if m.allocator == nil {
		return merr.WrapErrServiceInternalMsg("id allocator is nil")
	}
	if m.metaWriter == nil {
		return merr.WrapErrServiceInternalMsg("meta writer is nil")
	}
	collectionID := funcutil.GetCollectionIDFromVChannel(req.VChannel)
	if collectionID <= 0 {
		return merr.WrapErrServiceInternalMsg("invalid vchannel %q for L0 materialization", req.VChannel)
	}
	groups, err := splitMaterializeGroups(req, collectionID)
	if err != nil {
		return err
	}
	for _, group := range groups {
		if err := m.materializeGroup(ctx, req, collectionID, group); err != nil {
			return err
		}
	}
	return nil
}

func (m *SyncMaterializer) materializeGroup(
	ctx context.Context,
	req MaterializeRequest,
	collectionID int64,
	group materializeGroup,
) error {
	segmentID, err := m.allocator.AllocOne()
	if err != nil {
		return err
	}
	vchannel := req.VChannel
	startPosition := &msgpb.MsgPosition{ChannelName: vchannel, Timestamp: group.fromTimeTick}
	if pos := req.DeletePositions[group.fromTimeTick]; pos != nil {
		startPosition = proto.Clone(pos).(*msgpb.MsgPosition)
	}
	checkpoint := &msgpb.MsgPosition{ChannelName: vchannel, Timestamp: group.toTimeTick}
	if pos := req.DeletePositions[group.toTimeTick]; pos != nil {
		checkpoint = proto.Clone(pos).(*msgpb.MsgPosition)
	}
	schema := materializeSchema(group.delta.Layout.PKType)
	schema.Properties = req.Schemas[group.fromTimeTick].GetProperties()
	metaCache := newMaterializeMetaCache(collectionID, vchannel, schema)
	metaCache.AddSegment(&datapb.SegmentInfo{
		ID:             segmentID,
		CollectionID:   collectionID,
		PartitionID:    group.delta.Layout.PartitionID,
		InsertChannel:  vchannel,
		StartPosition:  startPosition,
		State:          commonpb.SegmentState_Growing,
		Level:          datapb.SegmentLevel_L0,
		StorageVersion: storage.StorageV2,
	}, func(_ *datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NoneBm25StatsFactory, metacache.SetStartPosRecorded(false))

	pack := (&syncmgr.SyncPack{}).
		WithCollectionID(collectionID).
		WithPartitionID(group.delta.Layout.PartitionID).
		WithChannelName(vchannel).
		WithSegmentID(segmentID).
		WithStartPosition(startPosition).
		WithCheckpoint(checkpoint).
		WithTimeRange(group.fromTimeTick, group.toTimeTick).
		WithLevel(datapb.SegmentLevel_L0).
		WithDataSource(metrics.StreamingDataSourceLabel).
		WithBatchRows(0).
		WithFlush()
	if req.LegacyFormat && !slices.ContainsFunc(group.delta.Rows, func(r deltacodec.Row) bool { return r.Operation == deltacodec.Update }) {
		deletes := &storage.DeleteData{}
		for _, row := range group.delta.Rows {
			for _, pk := range storage.ParseIDs2PrimaryKeys(row.PrimaryKeys) {
				deletes.Append(pk, row.TimeTick)
			}
		}
		pack.WithDeleteData(deletes)
	} else {
		// Replay must preserve committed updates even if admission has been disabled.
		pack.WithMutationDelta(&group.delta, group.bytes)
	}

	task := syncmgr.NewSyncTask().
		WithAllocator(m.allocator).
		WithChunkManager(m.chunkManager).
		WithMetaWriter(m.metaWriter).
		WithMetaCache(metaCache).
		WithSchema(schema).
		WithSyncPack(pack).
		WithStorageConfig(packed.CreateStorageConfig())

	mlog.Info(ctx, "materialize mutations into l0 segment",
		mlog.FieldCollectionID(collectionID),
		mlog.FieldPartitionID(group.delta.Layout.PartitionID),
		mlog.FieldSegmentID(segmentID),
		mlog.String("vchannel", vchannel),
		mlog.Uint64("targetTimeTick", req.TargetTimeTick),
		mlog.Uint64("rows", group.logicalRows),
		mlog.Uint64("bytes", group.bytes),
	)
	// Bound coordinator client retries; the WAL task scheduler owns later attempts.
	return task.Run(retry.WithMaxAttemptsContext(ctx, 3))
}

type materializeGroup struct {
	delta        storage.MutationDelta
	logicalRows  uint64
	fromTimeTick uint64
	toTimeTick   uint64
	bytes        uint64
}

func materializeSchema(pkType schemapb.DataType) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "transformlog_pk",
				DataType:     pkType,
				IsPrimaryKey: true,
			},
		},
	}
}

func newMaterializeMetaCache(collectionID int64, vchannel string, schema *schemapb.CollectionSchema) metacache.MetaCache {
	return metacache.NewMetaCache(&datapb.ChannelWatchInfo{
		Vchan: &datapb.VchannelInfo{
			CollectionID: collectionID,
			ChannelName:  vchannel,
		},
		Schema: schema,
	}, func(_ *datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NoneBm25StatsFactory)
}

// splitMaterializeGroups validates every mutation before writing any output.
// A TimeTick may exceed the limits but never straddles files in the same scope.
func splitMaterializeGroups(req MaterializeRequest, collectionID int64) ([]materializeGroup, error) {
	maxRows, maxBytes := req.MaxRows, req.MaxBytes
	if maxRows == 0 {
		maxRows = defaultMaterializeMaxRows
	}
	if maxBytes == 0 {
		maxBytes = defaultMaterializeMaxBytes
	}
	var groups []materializeGroup
	current := map[int64]*materializeGroup{}
	finish := func(scope int64) {
		g := current[scope]
		sort.Slice(g.delta.Layout.Fields, func(i, j int) bool { return g.delta.Layout.Fields[i].ID < g.delta.Layout.Fields[j].ID })
		groups = append(groups, *g)
		delete(current, scope)
	}
	type tickGroup struct {
		rows        []deltacodec.Row
		bytes, keys uint64
		fields      map[int64]deltacodec.Field
	}
	for begin := 0; begin < len(req.Entries); {
		tt := req.Entries[begin].GetTimeTick()
		schema := req.Schemas[tt]
		var pkType schemapb.DataType
		if schema != nil {
			pk, err := typeutil.GetPrimaryFieldSchema(schema)
			if err != nil {
				return nil, merr.Wrap(err, "delta PK schema")
			}
			pkType = pk.GetDataType()
		}
		fields := map[int64]*schemapb.FieldSchema{}
		for _, f := range schema.GetFields() {
			fields[f.GetFieldID()] = f
		}
		scopes := map[int64]*tickGroup{}
		var ordered []int64
		add := func(scope int64, r deltacodec.Row, bytes int) error {
			count := messageutil.PrimaryKeyCount(r.PrimaryKeys)
			if count == 0 && r.Operation == deltacodec.Delete {
				return nil
			}
			rowPKType := schemapb.DataType_Int64
			if r.PrimaryKeys.GetStrId() != nil {
				rowPKType = schemapb.DataType_VarChar
			}
			// A historical delete may carry only its typed PKs and TimeTick.
			if pkType == schemapb.DataType_None {
				pkType = rowPKType
			}
			if count == 0 || rowPKType != pkType {
				return merr.WrapErrDataIntegrityMsg("delta primary keys mismatch schema at %d", tt)
			}
			g := scopes[scope]
			if g == nil {
				g = &tickGroup{fields: map[int64]deltacodec.Field{}}
				scopes[scope] = g
				ordered = append(ordered, scope)
			}
			for _, a := range r.Assignments {
				f := fields[a.GetFieldId()]
				if f == nil {
					return merr.WrapErrDataIntegrityMsg("missing delta field %d at %d", a.GetFieldId(), tt)
				}
				field := deltacodec.Field{ID: f.GetFieldID(), Type: f.GetDataType(), Element: f.GetElementType()}
				if field.Type == schemapb.DataType_Array {
					field.ElementNullable = proto.Bool(f.GetElementNullable())
				}
				g.fields[field.ID] = field
			}
			g.rows = append(g.rows, r)
			g.bytes += uint64(bytes)
			g.keys += uint64(count)
			return nil
		}
		for begin < len(req.Entries) && req.Entries[begin].GetTimeTick() == tt {
			e := req.Entries[begin]
			begin++
			for _, b := range e.GetMutations().GetBlocks() {
				if d := b.GetDelete(); d != nil {
					row := deltacodec.Row{TimeTick: tt, Operation: deltacodec.Delete, PrimaryKeys: d.GetPrimaryKeys()}
					if err := add(d.GetPartitionId(), row, proto.Size(d)); err != nil {
						return nil, err
					}
					continue
				}
				u := b.GetUpdate()
				if schema == nil {
					return nil, merr.WrapErrDataIntegrityMsg("missing delta schema at %d", tt)
				}
				if u == nil || len(u.GetAssignments()) == 0 || b.SchemaVersion == nil || b.GetSchemaVersion() != schema.GetVersion() {
					return nil, merr.WrapErrDataIntegrityMsg("invalid delta payload or schema version at %d", tt)
				}
				if err := add(b.GetPartitionId(), deltacodec.Row{TimeTick: tt, Operation: deltacodec.Update, PrimaryKeys: u.GetPrimaryKeys(), Assignments: u.GetAssignments()}, proto.Size(b)); err != nil {
					return nil, err
				}
			}
		}
		for _, scope := range ordered {
			batch := scopes[scope]
			g := current[scope]
			if g != nil && (g.delta.Layout.PKType != pkType || g.delta.Layout.SchemaVersion != schema.GetVersion() || g.bytes+batch.bytes > maxBytes || g.logicalRows+batch.keys > maxRows) {
				finish(scope)
				g = nil
			}
			if g == nil {
				layout := deltacodec.Layout{CollectionID: collectionID, VChannel: req.VChannel, PartitionID: scope, SchemaVersion: schema.GetVersion(), PKType: pkType}
				if err := layout.ValidateIdentity(); err != nil {
					return nil, err
				}
				g = &materializeGroup{fromTimeTick: tt, delta: storage.MutationDelta{Layout: layout}}
				current[scope] = g
			}
			g.toTimeTick = tt
			g.logicalRows += batch.keys
			g.bytes += batch.bytes
			g.delta.Rows = append(g.delta.Rows, batch.rows...)
			for _, f := range g.delta.Layout.Fields {
				delete(batch.fields, f.ID)
			}
			for _, f := range batch.fields {
				g.delta.Layout.Fields = append(g.delta.Layout.Fields, f)
			}
		}
	}
	for _, scope := range slices.Sorted(maps.Keys(current)) {
		finish(scope)
	}
	return groups, nil
}
