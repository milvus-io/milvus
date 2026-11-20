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

package compactor

import (
	"cmp"
	"context"
	"slices"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	binlogio "github.com/milvus-io/milvus/internal/flushcommon/io"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type scopedMutation struct {
	deltacodec.Row
	layout deltacodec.Layout
}

// A range-capable backend probes only the signature before native footer
// discovery. Download-only binlog providers retain their complete-object API.
type mutationSource struct {
	cm       storage.ChunkManager
	download func(context.Context, []string) ([][]byte, error)
}

func compactionMutationSource(cm storage.ChunkManager, io binlogio.BinlogIO) mutationSource {
	if direct, ok := io.(storage.ChunkManager); ok {
		return mutationSource{cm: direct}
	}
	if io != nil {
		return mutationSource{cm: cm, download: io.Download}
	}
	return mutationSource{cm: cm}
}

func readSegmentMutations(ctx context.Context, source mutationSource, cfg *indexpb.StorageConfig, plan *datapb.CompactionPlan, seg *datapb.CompactionSegmentBinlogs, pk *schemapb.FieldSchema) ([]scopedMutation, error) {
	files := seg.GetDeltalogs()
	version := seg.GetStorageVersion()
	if path := seg.GetManifest(); path != "" {
		paths, err := packed.GetDeltaLogPathsFromManifest(path, cfg)
		if err != nil {
			return nil, err
		}
		files = nil
		for _, path := range paths {
			files = append(files, &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{{LogPath: path}}})
		}
		version = storage.StorageV3
	}
	if len(files) == 0 {
		return nil, nil
	}
	plugin, err := hookutil.GetCPluginContext(plan.GetPluginContext(), seg.GetCollectionID())
	if err != nil {
		return nil, err
	}
	fields := make(map[int64]*schemapb.FieldSchema)
	for _, field := range plan.GetSchema().GetFields() {
		fields[field.GetFieldID()] = field
	}
	var rows []scopedMutation
	for _, group := range files {
		for _, file := range group.GetBinlogs() {
			visit := func(l deltacodec.Layout, batch []deltacodec.Row) error {
				if l.CollectionID == 0 {
					l.CollectionID, l.VChannel, l.PartitionID, l.SchemaVersion = seg.GetCollectionID(), plan.GetChannel(), seg.GetPartitionID(), plan.GetSchema().GetVersion()
				} else if l.CollectionID != seg.GetCollectionID() || l.VChannel != plan.GetChannel() || l.PartitionID != seg.GetPartitionID() {
					return merr.WrapErrDataIntegrityMsg("delta identity does not match segment %d", seg.GetSegmentID())
				}
				for _, f := range l.Fields {
					field := fields[f.ID]
					if field == nil || field.GetDataType() != f.Type || (f.Type == schemapb.DataType_Array && field.GetElementType() != f.Element) {
						return merr.WrapErrDataIntegrityMsg("delta field %d does not match compaction schema", f.ID)
					}
				}
				for _, row := range batch {
					if seg.GetLevel() == datapb.SegmentLevel_L0 {
						row.TimeTick = tsoutil.EffectiveTimestamp(row.TimeTick, seg.GetCommitTimestamp())
						if through := plan.GetMaterializedThrough(); through != 0 && row.TimeTick > through {
							return merr.WrapErrDataIntegrityMsg("mutation exceeds frozen frontier %d", through)
						}
					}
					rows = append(rows, scopedMutation{row, l})
				}
				return nil
			}
			if seg.GetManifest() != "" {
				if err := storage.ReadPackedMutationDelta(ctx, file, pk.GetDataType(), nil, cfg, plugin, visit); err != nil {
					return nil, merr.Wrapf(err, "read deltalog %q for segment %d", file.GetLogPath(), seg.GetSegmentID())
				}
				continue
			}
			if file.GetMutationCounts() == nil && source.download != nil {
				objects, err := source.download(ctx, []string{file.GetLogPath()})
				if err != nil {
					return nil, merr.Wrapf(err, "download deltalog %q for segment %d", file.GetLogPath(), seg.GetSegmentID())
				}
				if len(objects) != 1 {
					return nil, merr.WrapErrDataIntegrityMsg("expected one downloaded delta object, got %d", len(objects))
				}
				if err := storage.ReadDownloadedMutationDelta(ctx, objects[0], file, version, pk.GetDataType(), cfg, plugin, visit); err != nil {
					return nil, merr.Wrapf(err, "read deltalog %q for segment %d", file.GetLogPath(), seg.GetSegmentID())
				}
				continue
			}
			if source.cm == nil {
				return nil, merr.WrapErrServiceInternalMsg("missing chunk manager for packed compaction delta")
			}
			err := storage.ReadMutationDelta(ctx, source.cm, file, version, pk.GetDataType(), nil, cfg, plugin, visit)
			if err != nil {
				return nil, merr.Wrapf(err, "read deltalog %q for segment %d", file.GetLogPath(), seg.GetSegmentID())
			}
		}
	}
	return rows, nil
}

func orderMutations(rows []scopedMutation) {
	slices.SortStableFunc(rows, func(a, b scopedMutation) int {
		if c := cmp.Compare(a.TimeTick, b.TimeTick); c != 0 {
			return c
		}
		return cmp.Compare(a.SourceOrdinal, b.SourceOrdinal)
	})
}

type segmentMutations struct {
	deletes map[any]uint64
	updates map[any][]deltacodec.Row
	fields  map[int64]*schemapb.FieldSchema
	pk      *schemapb.FieldSchema
	commit  uint64
}

func loadRewriteMutations(ctx context.Context, source mutationSource, cfg *indexpb.StorageConfig, plan *datapb.CompactionPlan, seg *datapb.CompactionSegmentBinlogs, pk *schemapb.FieldSchema) (*segmentMutations, error) {
	rows, err := readSegmentMutations(ctx, source, cfg, plan, seg, pk)
	if err != nil {
		return nil, err
	}
	orderMutations(rows)
	m := &segmentMutations{deletes: map[any]uint64{}, updates: map[any][]deltacodec.Row{}, fields: map[int64]*schemapb.FieldSchema{}, pk: pk, commit: seg.GetCommitTimestamp()}
	for _, f := range plan.GetSchema().GetFields() {
		m.fields[f.GetFieldID()] = f
	}
	for _, r := range rows {
		for i := range typeutil.GetSizeOfIDs(r.PrimaryKeys) {
			k := typeutil.GetPK(r.PrimaryKeys, int64(i))
			if r.Operation == deltacodec.Delete {
				m.deletes[k] = r.TimeTick
			} else {
				m.updates[k] = append(m.updates[k], r.Row)
			}
		}
	}
	return m, nil
}

func (m *segmentMutations) apply(rec storage.Record) (storage.Record, error) {
	if len(m.updates) == 0 {
		return rec, nil
	}
	times := rec.Column(common.TimeStampField).(*array.Int64)
	keys := rec.Column(m.pk.GetFieldID())
	changes := map[int64]map[int]*schemapb.UpdateLiteral{}
	for i := range rec.Len() {
		var key any
		switch k := keys.(type) {
		case *array.Int64:
			key = k.Value(i)
		case *array.String:
			key = k.Value(i)
		default:
			return nil, merr.WrapErrDataIntegrityMsg("invalid compaction key column %T", keys)
		}
		birth := tsoutil.EffectiveTimestamp(uint64(times.Value(i)), m.commit)
		if birth < m.deletes[key] {
			continue
		}
		for _, r := range m.updates[key] {
			if birth >= r.TimeTick {
				continue
			}
			for _, a := range r.Assignments {
				id := a.GetFieldId()
				if changes[id] == nil {
					changes[id] = make(map[int]*schemapb.UpdateLiteral)
				}
				changes[id][i] = a.GetLiteral()
			}
		}
	}
	if len(changes) == 0 {
		return rec, nil
	}
	cols := map[int64]arrow.Array{}
	for id, values := range changes {
		base, exists := storage.TryRecordColumn(rec, id)
		if !exists {
			releaseArrowArrays(cols)
			return nil, merr.WrapErrDataIntegrityMsg("missing compacted field %d", id)
		}
		col, err := storage.ApplyMutationLiterals(base, m.fields[id], values)
		if err != nil {
			releaseArrowArrays(cols)
			return nil, err
		}
		cols[id] = col
	}
	return &materializedRecord{base: rec, computed: cols}, nil
}

type mutationRecordReader struct {
	storage.RecordReader
	m       *segmentMutations
	current storage.Record
}

func (m *segmentMutations) wrapReader(reader storage.RecordReader) storage.RecordReader {
	if len(m.updates) == 0 {
		return reader
	}
	return &mutationRecordReader{RecordReader: reader, m: m}
}

func (r *mutationRecordReader) Next() (storage.Record, error) {
	cleanupMaterializedRecord(r.current)
	r.current = nil
	rec, err := r.RecordReader.Next()
	if err != nil {
		return nil, err
	}
	r.current, err = r.m.apply(rec)
	return r.current, err
}

func (r *mutationRecordReader) Close() error {
	cleanupMaterializedRecord(r.current)
	r.current = nil
	return r.RecordReader.Close()
}

// resolvedCopySchema is used only by row-copy builders after the segment
// reader filled absent columns. Applying a default again here would replace
// a stored NULL, including one from an earlier completed compaction.
func resolvedCopySchema(schema *schemapb.CollectionSchema) *schemapb.CollectionSchema {
	for _, field := range schema.GetFields() {
		if field.GetNullable() && field.GetDefaultValue() != nil {
			copy := proto.Clone(schema).(*schemapb.CollectionSchema)
			for _, f := range copy.GetFields() {
				if f.GetNullable() {
					f.DefaultValue = nil
				}
			}
			return copy
		}
	}
	return schema
}
