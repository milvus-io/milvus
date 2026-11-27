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
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// DeleteMerger incrementally decodes deletes into an owned PK -> max timestamp
// map, without depending on an insert reader or a collection schema. It is not
// concurrent. A batch consumer can drain the map when the budget is reached;
// without a consumer, all deletes must fit within the budget.
type DeleteMerger struct {
	cm            storage.ChunkManager
	cfg           *indexpb.StorageConfig
	pkType        schemapb.DataType
	retryAttempts uint
	budget        int64
	used          int64
	data          map[any]typeutil.Timestamp
	loadedPaths   map[string]bool // shared paths already folded into every bitmap
	// A batch consumer runs synchronously while the decoder still owns its
	// current record. It must finish before decoding resumes and may not retain
	// the map. fixed accounts for path inventory retained across batches.
	consume func(map[any]typeutil.Timestamp) error
	fixed   int64
}

func NewDeleteMerger(cm storage.ChunkManager, cfg *indexpb.StorageConfig, pkType schemapb.DataType,
	retryAttempts uint, budget int64,
) (*DeleteMerger, error) {
	if budget <= 0 {
		return nil, merr.Wrapf(merr.ErrServiceResourceInsufficient, "snapshot delete-map budget must be positive")
	}
	if pkType != schemapb.DataType_Int64 && pkType != schemapb.DataType_VarChar {
		return nil, merr.WrapErrDataIntegrityMsg("unsupported primary key type %s in deltalog", pkType.String())
	}
	m := &DeleteMerger{
		cm: cm, cfg: cfg, pkType: pkType, retryAttempts: retryAttempts,
		budget: budget, data: make(map[any]typeutil.Timestamp),
	}
	return m, nil
}

// loadSnapshotL0Deletes folds each bounded batch through consume and returns
// the emptied scratch merger for segment-local deletes. The retained shared
// path inventory prevents reopening objects already folded into every bitmap.
func loadSnapshotL0Deletes(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema,
	cfg *indexpb.StorageConfig, source *internalpb.SnapshotImportL0Source, start, end uint64, budget int64,
	consume func(map[any]typeutil.Timestamp) error, validate func(string) error,
) (*DeleteMerger, error) {
	if consume == nil {
		return nil, merr.WrapErrServiceInternalMsg("snapshot L0 loading requires a batch consumer")
	}
	if budget <= 0 {
		return nil, merr.Wrapf(merr.ErrServiceResourceInsufficient, "snapshot delete-map budget must be positive")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	pkField, err := typeutil.GetPrimaryFieldSchema(schema)
	if err != nil {
		return nil, err
	}
	merger, err := NewDeleteMerger(cm, cfg, pkField.GetDataType(),
		paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint(), budget)
	if err != nil {
		return nil, err
	}
	merger.consume = consume
	paths := make(map[string]bool)
	load := func(inventory []string, legacy bool) error {
		var unique []string
		for _, path := range inventory {
			if validate != nil {
				if err := validate(path); err != nil {
					return err
				}
				// Check storage identity before stripping the URI. URI/key aliases
				// must share one decoder and one download.
				path = snapshotstorage.NormalizeSnapshotObjectPath(path)
			}
			if previous, ok := paths[path]; ok {
				if previous != legacy {
					return merr.WrapErrDataIntegrityMsg("snapshot delete path has conflicting decoder contracts")
				}
				continue
			}
			// Retained path strings/map entries share the SAME reservation as PKs.
			// A second independent path budget would undercount task memory.
			charge := int64(len(path)) + 64
			if charge > budget-merger.used {
				if err := merger.flush(); err != nil {
					return err
				}
			}
			if charge > budget-merger.used {
				return merr.Wrapf(merr.ErrServiceResourceInsufficient, "snapshot L0 path inventory exceeds shared delete budget")
			}
			merger.used += charge
			merger.fixed += charge
			paths[path] = legacy
			unique = append(unique, path)
		}
		_, err := merger.Merge(ctx, unique, start, end, legacy)
		return err
	}
	if err := load(source.GetLegacyL0Deltalogs(), true); err != nil {
		return nil, err
	}
	// Resolve each exact manifest only once in this task execution. Do not
	// rewrite the persisted inventory or copy decoded deletes into each reader.
	// Consume one manifest at a time instead of retaining all expanded lists.
	manifests := make(map[string]struct{})
	for _, manifest := range source.GetManifestL0Paths() {
		base, version, err := packed.UnmarshalManifestPath(manifest)
		if err != nil || base == "" || version == packed.ManifestLatest {
			return nil, merr.WrapErrServiceInternalMsg("snapshot L0 requires an exact manifest")
		}
		if validate != nil {
			if err := validate(base); err != nil {
				return nil, err
			}
			manifest = packed.MarshalManifestPath(snapshotstorage.NormalizeSnapshotObjectPath(base), version)
		}
		if _, seen := manifests[manifest]; seen {
			continue
		}
		manifests[manifest] = struct{}{}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		inventory, err := packed.GetDeltaLogPathsFromManifest(manifest, cfg)
		if err != nil {
			return nil, merr.Wrap(err, "failed to resolve snapshot L0 manifest")
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := load(inventory, false); err != nil {
			return nil, err
		}
	}
	if err := merger.flush(); err != nil {
		return nil, err
	}
	merger.loadedPaths = paths
	return merger, nil
}

func (m *DeleteMerger) flush() error {
	if len(m.data) == 0 {
		return nil
	}
	if err := m.consume(m.data); err != nil {
		return err
	}
	// Reuse the allocation: old batches must not accumulate awaiting a GC.
	clear(m.data)
	m.used = m.fixed
	return nil
}

// Merge applies the inclusive timestamp window before max aggregation. The
// returned map is borrowed and must not be mutated by callers. Further merges
// update that same map; discard the merger on error, never publish a partial
// result. Paths already folded into all bitmaps are skipped during local merges.
func (m *DeleteMerger) Merge(ctx context.Context, paths []string, start, end uint64, legacy bool) (map[any]typeutil.Timestamp, error) {
	for _, path := range paths {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if contract, loaded := m.loadedPaths[path]; loaded {
			if contract != legacy {
				return nil, merr.WrapErrDataIntegrityMsg("snapshot delete path has conflicting decoder contracts")
			}
			// A manifest may also reference an already loaded L0 object.
			continue
		}
		version := storage.StorageV3
		if legacy {
			version = storage.StorageV1
		}
		consumed, err := m.mergeFile(ctx, path, start, end, version)
		if err != nil && legacy && !consumed && ctx.Err() == nil {
			// Preserve legacy V1/V2 probing only before a record was accepted.
			// Once decoding began, a terminal read/merge error must fail the job;
			// retrying another decoder could retain deletes from a corrupt prefix.
			_, err = m.mergeFile(ctx, path, start, end, storage.StorageV2)
		}
		if err != nil {
			return nil, err
		}
	}
	return m.data, nil
}

func (m *DeleteMerger) mergeFile(ctx context.Context, path string, start, end uint64, version int64) (bool, error) {
	options := []storage.RwOption{
		storage.WithVersion(version), storage.WithStorageConfig(m.cfg),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return multiReadWithRetry(ctx, m.cm, m.retryAttempts, paths)
		}),
	}
	delta, err := storage.NewDeltalogReader(ctx, m.pkType, []string{path}, options...)
	if err != nil {
		return false, err
	}
	defer delta.Close()
	consumed := false
	for {
		if err := ctx.Err(); err != nil {
			return consumed, err
		}
		record, err := delta.Next()
		if err == io.EOF {
			return consumed, nil
		}
		if err != nil {
			return consumed, err
		}
		consumed = true
		// Arrow buffers belong to delta until Next/Close. Every retained
		// string key must be owned; timestamps and integer keys are values.
		timestamps, ok := record.Column(common.TimeStampField).(*array.Int64)
		if !ok || timestamps.Len() != record.Len() || timestamps.NullN() != 0 {
			return true, merr.WrapErrDataIntegrityMsg("invalid snapshot deltalog timestamp column")
		}
		ints, intPK := record.Column(0).(*array.Int64)
		strs, strPK := record.Column(0).(*array.String)
		if (m.pkType != schemapb.DataType_Int64 || !intPK || ints.Len() != record.Len() || ints.NullN() != 0) &&
			(m.pkType != schemapb.DataType_VarChar || !strPK || strs.Len() != record.Len() || strs.NullN() != 0) {
			return true, merr.WrapErrDataIntegrityMsg("invalid snapshot deltalog primary key column")
		}
		for i := 0; i < record.Len(); i++ {
			if err := ctx.Err(); err != nil {
				return true, err
			}
			ts := uint64(timestamps.Value(i))
			if ts < start || ts > end {
				continue
			}
			var pk any
			if m.pkType == schemapb.DataType_Int64 {
				pk = ints.Value(i)
			} else {
				pk = strs.Value(i)
			}
			if err := m.merge(pk, ts); err != nil {
				return true, err
			}
		}
	}
}

func (m *DeleteMerger) merge(pk any, ts uint64) error {
	if previous, ok := m.data[pk]; ok {
		if ts > previous {
			// Go map assignment can replace an equal string key as well as
			// its value. Do not replace our owned key with an Arrow view.
			if str, ok := pk.(string); ok {
				pk = strings.Clone(str)
			}
			m.data[pk] = ts
		}
		return nil
	}
	// Conservative per-entry accounting includes key/interface, timestamp,
	// bucket slack and map growth. This is an accounting bound, not an RSS cap
	// for the Arrow reader or the Go allocator.
	charge := int64(128)
	if str, ok := pk.(string); ok {
		charge += int64(len(str))
	}
	if charge > m.budget-m.used && m.consume != nil {
		if err := m.flush(); err != nil {
			return err
		}
	}
	if charge > m.budget-m.used {
		return merr.Wrapf(merr.ErrServiceResourceInsufficient,
			"snapshot delete map exceeds its %d byte budget (accounted=%d, next key=%d); increase dataNode.import.readDeleteBufferSizeInMB and ensure sufficient dataNode.import.memoryLimitPercentage allowance",
			m.budget, m.used, charge)
	}
	if str, ok := pk.(string); ok {
		pk = strings.Clone(str)
	}
	m.used += charge
	m.data[pk] = ts
	return nil
}
