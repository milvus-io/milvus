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

package storage

import (
	"context"
	"fmt"
	"io"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestMutationDeltaNativeProjection(t *testing.T) {
	paramtable.Init()
	dir := t.TempDir()
	ctx := context.Background()
	cm := NewLocalChunkManager(objectstorage.RootPath(dir))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	path := filepath.Join(dir, "delta_log", "1", "10", "22", "31")
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{3, 7}}}}
	row := func(tt uint64, ordinal uint32, field int64, v *schemapb.UpdateLiteral) deltacodec.Row {
		r := deltacodec.Row{TimeTick: tt, SourceOrdinal: ordinal, PrimaryKeys: keys, Operation: deltacodec.Delete}
		if field != 0 {
			r.Operation = deltacodec.Update
			r.Assignments = []*messagespb.UpdateAssignment{{FieldId: field, Literal: v}}
		}
		return r
	}
	empty := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: ""}}}}
	val := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 21}}}}
	null := &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}
	data := MutationDelta{Layout: deltacodec.Layout{CollectionID: 1, PartitionID: 10, VChannel: "p_1v0", SchemaVersion: 9, PKType: schemapb.DataType_Int64, Fields: []deltacodec.Field{{ID: 105, Type: schemapb.DataType_Int64}, {ID: 106, Type: schemapb.DataType_VarChar}}}, Rows: []deltacodec.Row{row(100, 2, 105, val), row(100, 4, 106, empty), row(100, 6, 105, null), row(110, 0, 0, nil)}}
	size, err := WriteMutationDelta(ctx, path, data, cfg, nil)
	require.NoError(t, err)
	actual, err := cm.Size(ctx, path)
	require.NoError(t, err)
	require.Equal(t, size, actual)
	binlog := &datapb.Binlog{LogPath: path, EntriesNum: 4, MutationCounts: &datapb.DeltaMutationCounts{UpdateKeys: 6, DeleteKeys: 2}}
	for _, ids := range [][]int64{nil, {105}, {}} {
		var got []deltacodec.Row
		err = ReadMutationDelta(ctx, cm, binlog, StorageV2, data.Layout.PKType, ids, cfg, nil, func(l deltacodec.Layout, rows []deltacodec.Row) error {
			require.Equal(t, data.Layout.Project(ids), l)
			got = append(got, rows...)
			return nil
		})
		require.NoError(t, err)
		require.Len(t, got, 4)
		for i, r := range got {
			want := data.Rows[i]
			require.Equal(t, want.SourceOrdinal, r.SourceOrdinal)
			require.True(t, proto.Equal(keys, r.PrimaryKeys))
			require.Equal(t, want.Operation, r.Operation)
			if len(want.Assignments) > 0 && (ids == nil || (len(ids) > 0 && want.Assignments[0].FieldId == 105)) {
				require.Len(t, r.Assignments, 1)
				require.True(t, proto.Equal(want.Assignments[0], r.Assignments[0]))
			} else {
				require.Empty(t, r.Assignments)
			}
		}
	}
	corrupt := filepath.Join(dir, "corrupt")
	require.NoError(t, cm.Write(ctx, corrupt, []byte("bad footer")))
	binlog.LogPath = corrupt
	require.ErrorContains(t, ReadMutationDelta(ctx, cm, binlog, StorageV2, data.Layout.PKType, nil, cfg, nil, nil), "signature")
}

func TestMutationDeltaLegacyFormats(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	dir := t.TempDir()
	cm := NewLocalChunkManager(objectstorage.RootPath(dir))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	for _, spec := range []struct {
		name        string
		format      string
		write, read int64
	}{{"json", "json", StorageV1, StorageV1}, {"legacy-parquet", "parquet", StorageV1, StorageV2}, {"v2", "", StorageV2, StorageV2}, {"packed-v3", "", StorageV2, StorageV3}} {
		t.Run(spec.name, func(t *testing.T) {
			option := &paramtable.Get().DataNodeCfg.DeltalogFormat
			old := option.SwapTempValue(spec.format)
			defer option.SwapTempValue(old)
			path := filepath.Join(dir, spec.name)
			writer, err := NewDeltalogWriter(ctx, 1, 2, 3, 4, schemapb.DataType_Int64, path, WithVersion(spec.write), WithStorageConfig(cfg), WithUploader(cm.MultiWrite))
			require.NoError(t, err)
			rec, _, _, err := BuildDeleteRecord([]PrimaryKey{NewInt64PrimaryKey(12), NewInt64PrimaryKey(14)}, []uint64{85, 90})
			require.NoError(t, err)
			require.NoError(t, writer.Write(rec))
			rec.Release()
			require.NoError(t, writer.Close())
			var got []deltacodec.Row
			err = ReadMutationDelta(ctx, cm, &datapb.Binlog{LogPath: path, EntriesNum: 2}, spec.read, schemapb.DataType_Int64, []int64{}, cfg, nil, func(_ deltacodec.Layout, rs []deltacodec.Row) error { got = append(got, rs...); return nil })
			require.NoError(t, err)
			require.Len(t, got, 2)
			for i, r := range got {
				require.Equal(t, deltacodec.Delete, r.Operation)
				require.Equal(t, []int64{int64(12 + 2*i)}, r.PrimaryKeys.GetIntId().GetData())
				require.Zero(t, r.SourceOrdinal)
				require.Empty(t, r.Assignments)
			}
		})
	}
}

// Exercise the same four-common-column files, native scan windows and sparse
// decoder used by materialization and dispatch. Arrow bytes count each borrowed
// batch while live; protobuf bytes measure owned key and assignment payloads.
func TestWideSparseMutationDelta(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	dir := t.TempDir()
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	path := filepath.Join(dir, "wide")
	d := MutationDelta{Layout: deltacodec.Layout{CollectionID: 1, PartitionID: -1, VChannel: "p_1v0", SchemaVersion: 7, PKType: schemapb.DataType_Int64}}
	for i := 0; i < 20; i++ {
		typ := schemapb.DataType_Int64
		if i >= 10 {
			typ = schemapb.DataType_VarChar
		}
		d.Layout.Fields = append(d.Layout.Fields, deltacodec.Field{ID: int64(i + 105), Type: typ})
	}
	for i := 0; i < 10000; i++ {
		r := deltacodec.Row{TimeTick: uint64(100 + i), Operation: deltacodec.Update, PrimaryKeys: &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{int64(2 * i), int64(2*i + 1)}}}}}
		for j := 0; j < 1+i%2; j++ {
			index := (i + 7*j) % 20
			v := &schemapb.UpdateLiteral{}
			switch {
			case (i+j)%5 == 0:
				v.Value = &schemapb.UpdateLiteral_NullValue{}
			case index < 10:
				v.Value = &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: int64(i)}}}
			default:
				v.Value = &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: fmt.Sprintf("%032d", i)}}}
			}
			r.Assignments = append(r.Assignments, &messagespb.UpdateAssignment{FieldId: int64(index + 105), Literal: v})
		}
		d.Rows = append(d.Rows, r)
	}
	size, err := WriteMutationDelta(ctx, path, d, cfg, nil)
	require.NoError(t, err)
	t.Logf("complete native file bytes=%d", size)
	var fullBytes int64
	for _, ids := range [][]int64{nil, {105}, {117}} {
		reader, projected, err := openProjectedMutationFile(path, int64(len(d.Rows)), d.Layout, ids, cfg, nil)
		require.NoError(t, err)
		var rows, assignments int
		var decoded, peak, owned int64
		for {
			batch, err := reader.ReadNext()
			if err == io.EOF {
				break
			}
			require.NoError(t, err)
			var bytes int64
			for _, col := range batch.Columns() {
				bytes += int64(col.Data().SizeInBytes())
			}
			decoded += bytes
			peak = max(peak, bytes)
			rs, err := deltacodec.Decode(projected, batch)
			require.NoError(t, err)
			rows += len(rs)
			for _, r := range rs {
				assignments += len(r.Assignments)
				owned += int64(proto.Size(r.PrimaryKeys))
				for _, a := range r.Assignments {
					owned += int64(proto.Size(a))
				}
			}
		}
		require.NoError(t, reader.Close())
		require.Equal(t, 10000, rows)
		if ids == nil {
			require.Equal(t, 15000, assignments)
			fullBytes = decoded
		} else {
			require.Less(t, decoded, fullBytes)
			require.Positive(t, assignments)
		}
		t.Logf("projection=%v rows=%d assignments=%d decoded Arrow bytes=%d peak batch bytes=%d owned key/assignment protobuf bytes=%d", ids, rows, assignments, decoded, peak, owned)
	}
}

func TestMutationDeltaRejectsCorruptFooterAndCancellation(t *testing.T) {
	paramtable.Init()
	ctx := context.Background()
	dir := t.TempDir()
	cm := NewLocalChunkManager(objectstorage.RootPath(dir))
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: dir}
	layout := deltacodec.Layout{CollectionID: 1, VChannel: "p_1v0", PartitionID: -1, PKType: schemapb.DataType_Int64, Fields: []deltacodec.Field{{ID: 105, Type: schemapb.DataType_Int64}}}
	schema, err := layout.Schema()
	require.NoError(t, err)
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{7}}}}
	row := deltacodec.Row{Operation: deltacodec.Update, TimeTick: 10, PrimaryKeys: keys, Assignments: []*messagespb.UpdateAssignment{{FieldId: 105, Literal: &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_NullValue{}}}}}
	rec, err := deltacodec.Encode(layout, []deltacodec.Row{row})
	require.NoError(t, err)
	defer rec.Release()
	path := filepath.Join(dir, "bad-schema")
	writer, err := packed.NewPackedWriter([]string{path}, schema, deltaReadWindow, packed.DefaultMultiPartUploadSize, []storagecommon.ColumnGroup{{Columns: []int{0, 1, 2, 3, 4}}}, cfg, nil)
	require.NoError(t, err)
	for key, value := range map[string]string{"delta_format_version": "1", "schema_version": "0", "collection_id": "1", "vchannel": layout.VChannel, "partition_scope": "-1", "pk_type": "Int64", "update_fields": fmt.Sprintf(`[{"ID":105,"Type":%d}]`, schemapb.DataType_VarChar)} {
		require.NoError(t, writer.AddMetadata(key, value))
	}
	require.NoError(t, writer.WriteRecordBatch(rec))
	require.NoError(t, writer.Close())
	err = ReadMutationDelta(ctx, cm, &datapb.Binlog{LogPath: path}, StorageV2, layout.PKType, nil, cfg, nil, nil)
	require.ErrorContains(t, err, "does not match its Parquet schema")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	size, err := WriteMutationDelta(canceled, filepath.Join(dir, "cancelled"), MutationDelta{Layout: layout, Rows: []deltacodec.Row{row}}, cfg, nil)
	require.ErrorIs(t, err, context.Canceled)
	require.Zero(t, size)
	require.ErrorIs(t, ReadMutationDelta(canceled, cm, &datapb.Binlog{LogPath: path}, StorageV2, layout.PKType, nil, cfg, nil, nil), context.Canceled)
	// A single oversized mutation stays whole. A following large assignment
	// starts a new Arrow batch instead of multiplying transient allocation.
	row.Assignments[0].Literal = &schemapb.UpdateLiteral{Value: &schemapb.UpdateLiteral_ScalarValue{ScalarValue: &schemapb.ValueField{Data: &schemapb.ValueField_StringData{StringData: strings.Repeat("x", 1024)}}}}
	require.Equal(t, 1, deltaBatchEnd([]deltacodec.Row{row, row}, 0, 1, 128))
}

func TestTypedDeltaDeleteKeyAccounting(t *testing.T) {
	delta := &datapb.FieldBinlog{Binlogs: []*datapb.Binlog{
		{EntriesNum: 3},
		{EntriesNum: 100, MutationCounts: &datapb.DeltaMutationCounts{DeleteKeys: 0, UpdateKeys: 300}},
		{EntriesNum: 2, MutationCounts: &datapb.DeltaMutationCounts{DeleteKeys: 11, UpdateKeys: 6}},
	}}
	collector := NewStatisticsCollector()
	collector.Digest(nil, delta, 0, 0, 1, 2)
	require.EqualValues(t, 14, collector.Publish().GetDeleteNumRows())
	require.EqualValues(t, 14, BuildStatsFromFieldBinlogs(nil, nil, nil, []*datapb.FieldBinlog{delta}).GetDeleteNumRows())
}
