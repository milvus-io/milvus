// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package binlog

import (
	"context"
	"io"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// SnapshotL0Deletes contains immutable per-source row masks covering shared L0
// and segment-local deletes. Only fully prepared masks are published to readers;
// bounded timestamp batches remain private to preparation and are not retained.
type SnapshotL0Deletes struct {
	masks map[snapshotMaskKey]*rowDeleteMask
}

// A manifest can be consumed with different source commit timestamps. Never
// share a mask by manifest alone: identical PKs need different visibility tests.
type snapshotMaskKey struct {
	manifest string
	commit   uint64
}

func maskKey(source *internalpb.SnapshotImportSource) snapshotMaskKey {
	return snapshotMaskKey{source.GetManifestPath(), source.GetSourceCommitTimestamp()}
}

const maskBlockWords = 64

// Row positions are counted BEFORE range/delete filtering. Fixed-size blocks
// avoid copying a growing bitmap and let the first raw scan determine its size;
// catalog NumRows and PreImport survivor counts are not physical row counts.
type rowDeleteMask struct {
	blocks []*[maskBlockWords]uint64
	rows   int64 // -1 until the first complete raw scan
}

type maskBudget struct{ used, limit int64 }

func (b *maskBudget) reserve(bytes int64) error {
	if bytes > b.limit-b.used {
		return merr.Wrapf(merr.ErrServiceResourceInsufficient, "snapshot row bitmaps exceed task reservation (%d bytes)", b.limit)
	}
	b.used += bytes
	return nil
}

func (m *rowDeleteMask) prepareRow(row int64, budget *maskBudget) error {
	if m.rows >= 0 && row >= m.rows {
		return merr.WrapErrDataIntegrityMsg("snapshot raw row count increased between scans")
	}
	if row/(maskBlockWords*64) >= int64(len(m.blocks)) {
		// Include conservative pointer-slice growth overhead per block. Like
		// delete-map accounting, this is not a cap on allocator/Arrow RSS.
		if err := budget.reserve(maskBlockWords*8 + 64); err != nil {
			return err
		}
		m.blocks = append(m.blocks, new([maskBlockWords]uint64))
	}
	return nil
}

func (m *rowDeleteMask) deleted(row int64) bool {
	return m.blocks[row/(maskBlockWords*64)][(row/64)%maskBlockWords]&(uint64(1)<<uint(row%64)) != 0
}

func (m *rowDeleteMask) finish(rows int64) error {
	if m.rows >= 0 && rows != m.rows {
		return merr.WrapErrDataIntegrityMsg("snapshot raw row count changed between scans: expected %d, got %d", m.rows, rows)
	}
	m.rows = rows
	return nil
}

// validateSnapshotRowTimestamp requires raw_row_ts <= a nonzero commit_ts:
// only then does EffectiveTimestamp's max match segcore's commit-time override.
// Check the actual stored row before filtering, never clamp or discard it.
// A nil source or zero commit timestamp imposes no upper bound.
func validateSnapshotRowTimestamp(source *internalpb.SnapshotImportSource, raw uint64) error {
	if commit := source.GetSourceCommitTimestamp(); commit != 0 && raw > commit {
		return merr.WrapErrDataIntegrityMsg("snapshot source manifest %s has raw row timestamp %d above segment commit timestamp %d", source.GetManifestPath(), raw, commit)
	}
	return nil
}

type snapshotMaskScan struct {
	source  *internalpb.SnapshotImportSource
	schema  *schemapb.CollectionSchema
	options []storage.RwOption
	mask    *rowDeleteMask
}

func (s *snapshotMaskScan) apply(ctx context.Context, deletes map[any]typeutil.Timestamp, budget *maskBudget) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	rr, err := storage.NewManifestRecordReader(ctx, s.source.GetManifestPath(), s.schema, s.options...)
	if err != nil {
		return err
	}
	defer rr.Close()
	pkField := s.schema.Fields[0]
	var row int64
	for {
		record, err := rr.Next()
		if err == io.EOF {
			return s.mask.finish(row)
		}
		if err != nil {
			return err
		}
		timestamps, ok := record.Column(common.TimeStampField).(*array.Int64)
		if !ok || timestamps.Len() != record.Len() || timestamps.NullN() != 0 {
			return merr.WrapErrDataIntegrityMsg("invalid snapshot row timestamp column")
		}
		ints, intPK := record.Column(pkField.GetFieldID()).(*array.Int64)
		strs, strPK := record.Column(pkField.GetFieldID()).(*array.String)
		if (pkField.GetDataType() != schemapb.DataType_Int64 || !intPK || ints.Len() != record.Len() || ints.NullN() != 0) &&
			(pkField.GetDataType() != schemapb.DataType_VarChar || !strPK || strs.Len() != record.Len() || strs.NullN() != 0) {
			return merr.WrapErrDataIntegrityMsg("invalid snapshot row primary key column")
		}
		for i := 0; i < record.Len(); i++ {
			if err := ctx.Err(); err != nil {
				return err
			}
			rawTs := uint64(timestamps.Value(i))
			if err := validateSnapshotRowTimestamp(s.source, rawTs); err != nil {
				return err
			}
			if err := s.mask.prepareRow(row, budget); err != nil {
				return err
			}
			var pk any
			if intPK {
				pk = ints.Value(i)
			} else {
				pk = strs.Value(i)
			}
			if ts, found := deletes[pk]; found && ts > tsoutil.EffectiveTimestamp(rawTs, s.source.GetSourceCommitTimestamp()) {
				s.mask.blocks[row/(maskBlockWords*64)][(row/64)%maskBlockWords] |= uint64(1) << uint(row%64)
			}
			row++
		}
	}
}

// BuildSnapshotDeleteMasks consumes each L0 stream once per task attempt. A
// bounded batch is applied to ALL source segments before the decoder advances.
// No reader may emit rows until shared and segment-local deletes both finish.
// Both phases rebuild masks from their immutable input; nothing is persisted.
func BuildSnapshotDeleteMasks(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema,
	cfg *indexpb.StorageConfig, sources []*internalpb.SnapshotImportSource, l0 *internalpb.SnapshotImportL0Source,
	start, end uint64, deleteBudget, bitmapBudget int64, encryption SourceEncryption, validate func(string) error,
) (*SnapshotL0Deletes, error) {
	pk, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	budget := &maskBudget{limit: bitmapBudget}
	masks := make(map[snapshotMaskKey]*rowDeleteMask)
	scans := make([]snapshotMaskScan, 0, len(sources))
	for _, source := range sources {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := maskKey(source)
		if err := validateStorageV3ManifestPath(key.manifest, validate); err != nil {
			return nil, err
		}
		if _, exists := masks[key]; exists {
			continue
		}
		if err := budget.reserve(256); err != nil {
			return nil, err
		}
		r := newReader(ctx, cm, schema, cfg, storage.StorageV3, 0, "")
		r.sourceEncryption, r.validatePath = encryption, validate
		options, err := r.prepareStorageV3Manifest(source.GetManifestPath())
		if err != nil {
			return nil, err
		}
		mask := &rowDeleteMask{rows: -1}
		masks[key] = mask
		// Both projected PackedReader and TEXT SegmentReader use the same
		// sequential column-group reader. LOB resolution replaces columns only;
		// it never reorders/filters rows. Do not push predicates into this scan.
		scans = append(scans, snapshotMaskScan{
			source: source, mask: mask, options: options,
			schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
				pk,
				{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			}},
		})
	}
	merger, err := loadSnapshotL0Deletes(ctx, cm, schema, cfg, l0, start, end, deleteBudget,
		func(batch map[any]typeutil.Timestamp) error {
			for i := range scans {
				if err := scans[i].apply(ctx, batch, budget); err != nil {
					return err
				}
			}
			return nil
		}, validate)
	if err != nil {
		return nil, err
	}
	for i := range scans {
		scan := &scans[i]
		paths, err := packed.GetDeltaLogPathsFromManifest(scan.source.GetManifestPath(), cfg)
		if err != nil {
			return nil, err
		}
		for i, path := range paths {
			if validate != nil {
				if err := validate(path); err != nil {
					return nil, err
				}
				paths[i] = snapshotstorage.NormalizeSnapshotObjectPath(path)
			}
		}
		// Reuse the emptied batch map and its charged path inventory. Shared
		// paths are skipped; local batches affect only this segment's bitmap.
		merger.consume = func(batch map[any]typeutil.Timestamp) error { return scan.apply(ctx, batch, budget) }
		if _, err := merger.Merge(ctx, paths, start, end, false); err != nil {
			return nil, err
		}
		if err := merger.flush(); err != nil {
			return nil, err
		}
		if scan.mask.rows < 0 {
			// Empty/all-out-of-range L0 still needs a physical row count and the
			// source timestamp invariant checked before emitting any data.
			if err := scan.apply(ctx, nil, budget); err != nil {
				return nil, err
			}
		}
	}
	return &SnapshotL0Deletes{masks: masks}, nil
}
