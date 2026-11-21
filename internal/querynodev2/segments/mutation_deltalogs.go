package segments

import (
	"cmp"
	"context"
	"slices"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storage/deltacodec"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// loadMutationDeltalogs reads the exact parent and overlay manifests selected
// by the load snapshot. File timestamps never establish a replay frontier.
func (loader *segmentLoader) loadMutationDeltalogs(ctx context.Context, segment *LocalSegment, info *querypb.SegmentLoadInfo, meta deltaStorageMetadata) error {
	cfg := createStorageConfig()
	plugin, err := hookutil.GetCollectionPluginContext(meta.properties, segment.Collection())
	if err != nil {
		return err
	}
	legacy, err := storage.NewDeltaDataWithPkType(0, meta.primaryKeyType)
	if err != nil {
		return err
	}
	var selected []int64
	if segment.updateFields != nil {
		selected = make([]int64, 0, len(segment.updateFields))
		for id := range segment.updateFields {
			selected = append(selected, id)
		}
		slices.Sort(selected)
	}
	var rows []deltacodec.Row
	visit := func(layout deltacodec.Layout, batch []deltacodec.Row) error {
		if layout.CollectionID != 0 && (layout.CollectionID != segment.Collection() || layout.VChannel != info.GetInsertChannel() || layout.PartitionID != segment.Partition()) {
			return merr.WrapErrDataIntegrityMsg("deltalog identity does not match segment %d", segment.ID())
		}
		if layout.CollectionID == 0 {
			for _, r := range batch {
				if err := appendDeleteRow(legacy, r); err != nil {
					return err
				}
			}
			return nil
		}
		for _, f := range layout.Fields {
			if tracked, ok := segment.updateFields[f.ID]; ok && (tracked.Type != f.Type || f.Type == schemapb.DataType_Array && tracked.Element != f.Element) {
				return merr.WrapErrDataIntegrityMsg("deltalog field %d disagrees with segment %d schema", f.ID, segment.ID())
			}
		}
		rows = append(rows, batch...)
		return nil
	}
	readManifest := func(path string) error {
		paths, err := packed.GetDeltaLogPathsFromManifest(path, cfg, common.TalonStageManifest)
		if err != nil {
			return err
		}
		for _, path := range paths {
			if err := storage.ReadPackedMutationDelta(ctx, &datapb.Binlog{LogPath: path}, meta.primaryKeyType, meta.nullableArrays, selected, cfg, plugin, visit); err != nil {
				return merr.Wrapf(err, "read manifest delta %q", path)
			}
		}
		return nil
	}
	if path := info.GetManifestPath(); path != "" {
		if err := readManifest(path); err != nil {
			return err
		}
	} else {
		for _, group := range info.GetDeltalogs() {
			for _, file := range group.GetBinlogs() {
				if file.GetLogPath() == "" {
					if file.GetEntriesNum() != 0 || file.GetMutationCounts() != nil {
						return merr.WrapErrDataIntegrityMsg("explicit deltalog %d has no path", file.GetLogID())
					}
					continue
				}
				if err := storage.ReadMutationDelta(ctx, loader.cm, file, info.GetStorageVersion(), meta.primaryKeyType, meta.nullableArrays, selected, cfg, plugin, visit); err != nil {
					return merr.Wrapf(err, "read delta %q", file.GetLogPath())
				}
			}
		}
	}
	for _, path := range info.GetChildManifestPaths() {
		if err := readManifest(path); err != nil {
			return err
		}
	}
	// Ordinary delete-only files retain their compact PK/timestamp vectors.
	if len(rows) == 0 {
		return segment.LoadDeltaData(ctx, legacy)
	}
	for i, ts := range legacy.DeleteTimestamps() {
		ids := storage.ParsePrimaryKeys2IDs([]storage.PrimaryKey{legacy.DeletePks().Get(i)})
		rows = append(rows, deltacodec.Row{TimeTick: ts, Operation: deltacodec.Delete, PrimaryKeys: ids})
	}
	return applyPersistedMutations(ctx, segment, meta, rows)
}

type mutationDeltaTarget interface {
	LoadDeltaData(context.Context, *storage.DeltaData) error
	ApplyUpdates(context.Context, uint64, uint32, *messagespb.UpdateMessageBody) error
}

func applyPersistedMutations(ctx context.Context, segment mutationDeltaTarget, meta deltaStorageMetadata, rows []deltacodec.Row) error {
	slices.SortStableFunc(rows, func(a, b deltacodec.Row) int {
		if c := cmp.Compare(a.TimeTick, b.TimeTick); c != 0 {
			return c
		}
		return cmp.Compare(a.SourceOrdinal, b.SourceOrdinal)
	})
	var deletes *storage.DeltaData
	flush := func() error {
		if deletes == nil {
			return nil
		}
		err := segment.LoadDeltaData(ctx, deletes)
		deletes = nil
		return err
	}
	for _, r := range rows {
		if err := ctx.Err(); err != nil {
			return err
		}
		switch r.Operation {
		case deltacodec.Delete:
			if deletes == nil {
				var err error
				deletes, err = storage.NewDeltaDataWithPkType(0, meta.primaryKeyType)
				if err != nil {
					return err
				}
			}
			if err := appendDeleteRow(deletes, r); err != nil {
				return err
			}
		case deltacodec.Update:
			if err := flush(); err != nil {
				return err
			}
			if err := segment.ApplyUpdates(ctx, r.TimeTick, r.SourceOrdinal, &messagespb.UpdateMessageBody{PrimaryKeys: r.PrimaryKeys, Assignments: r.Assignments}); err != nil {
				return err
			}
		default:
			return merr.WrapErrDataIntegrityMsg("unsupported persisted mutation %d", r.Operation)
		}
	}
	return flush()
}

func appendDeleteRow(deletes *storage.DeltaData, r deltacodec.Row) error {
	pks := storage.ParseIDs2PrimaryKeysBatch(r.PrimaryKeys)
	for i := 0; i < pks.Len(); i++ {
		if err := deletes.Append(pks.Get(i), r.TimeTick); err != nil {
			return err
		}
	}
	return nil
}
