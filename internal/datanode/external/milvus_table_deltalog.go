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

package external

import (
	"context"
	"io"
	"path"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Batch virtual-PK delete translation to cap delete-event memory while avoiding
// one full source-fragment PK scan per source deltalog.
const milvusTableVirtualPKDeltalogBatchSize = 32

// postProcessMilvusTableDeltalogs finalizes deltalog handling after the target
// segment manifest is built. Real-PK milvus-table segments keep source deltas
// in the manifest; virtual-PK segments rewrite source-PK deletes into target
// virtual-PK deltalogs.
func (t *RefreshExternalCollectionTask) postProcessMilvusTableDeltalogs(
	ctx context.Context,
	basePath string,
	manifestPath string,
	segmentID int64,
	fragments []packed.Fragment,
) (string, error) {
	if !packed.HasExternalPrimaryKey(t.req.GetSchema()) {
		return t.translateMilvusTableDeltalogsToVirtualPKManifest(ctx, basePath, manifestPath, segmentID, fragments)
	}
	// Source segment manifest deltas are imported by the milvus-table manifest
	// builder. Fragment deltas left here are snapshot L0 overlays, so keep their
	// source paths in the target manifest instead of copying the files.
	return t.addMilvusTableL0DeltalogsToManifest(ctx, manifestPath, fragments)
}

// addMilvusTableL0DeltalogsToManifest appends snapshot L0 deltalogs to a
// real-PK target manifest without copying files. Segment-manifest deltas have
// already been imported by the C++ manifest builder.
func (t *RefreshExternalCollectionTask) addMilvusTableL0DeltalogsToManifest(
	ctx context.Context,
	manifestPath string,
	fragments []packed.Fragment,
) (string, error) {
	var entries []packed.DeltaLogEntry
	seen := make(map[string]struct{})
	for _, fragment := range fragments {
		for _, fieldBinlog := range fragment.Deltalogs {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				sourcePath := binlog.GetLogPath()
				if sourcePath == "" {
					continue
				}
				if _, ok := seen[sourcePath]; ok {
					continue
				}
				seen[sourcePath] = struct{}{}
				if err := packed.ValidateMilvusTableSourceDeltalogPath(sourcePath); err != nil {
					return "", err
				}
				logID := binlog.GetLogID()
				if logID == 0 {
					return "", merr.WrapErrServiceInternalMsg("milvus-table source deltalog %s has no allocated log ID", sourcePath)
				}
				if err := ensureContext(ctx); err != nil {
					return "", err
				}
				entries = append(entries, packed.DeltaLogEntry{
					Path:       sourcePath,
					NumEntries: binlog.GetEntriesNum(),
				})
			}
		}
	}
	if len(entries) == 0 {
		return manifestPath, nil
	}
	if err := ensureContext(ctx); err != nil {
		return "", err
	}
	updatedManifestPath, err := packed.AddDeltaLogsToManifestOverwrite(manifestPath, t.req.GetStorageConfig(), entries)
	if err != nil {
		return "", err
	}
	if err := ensureContext(ctx); err != nil {
		return "", err
	}
	return updatedManifestPath, nil
}

// milvusTableDeltalogRef identifies one source StorageV3 deltalog that may
// need to be translated into a target virtual-PK deltalog.
type milvusTableDeltalogRef struct {
	sourcePath string
	logID      int64
	numEntries int64
}

// milvusTableSourcePKOffset records where a source PK lands in the target
// segment and the insert timestamp used by delete visibility rules.
type milvusTableSourcePKOffset struct {
	targetOffset    int64
	insertTimestamp storage.Timestamp
}

// milvusTableSourceDeleteEvent is one delete event read from a source deltalog.
type milvusTableSourceDeleteEvent struct {
	sourcePKKey     string
	deleteTimestamp storage.Timestamp
}

// milvusTableSourceDeltalogDeletes keeps delete events grouped by source log so
// the translated virtual-PK log can preserve the source log identity.
type milvusTableSourceDeltalogDeletes struct {
	ref    milvusTableDeltalogRef
	events []milvusTableSourceDeleteEvent
}

// getMilvusTableSourceManifestDeltalogs reads source segment deltas from the
// StorageV3 manifest and caches a deep copy by manifest path for the refresh
// task lifetime.
func (t *RefreshExternalCollectionTask) getMilvusTableSourceManifestDeltalogs(
	manifestPath string,
) ([]*datapb.FieldBinlog, error) {
	if manifestPath == "" {
		return nil, nil
	}

	t.milvusTableSourceDeltalogsMu.Lock()
	if cached, ok := t.milvusTableSourceDeltalogs[manifestPath]; ok {
		t.milvusTableSourceDeltalogsMu.Unlock()
		return cloneFieldBinlogs(cached), nil
	}
	t.milvusTableSourceDeltalogsMu.Unlock()

	extfs := t.milvusTableExternalSpecContext()
	deltalogs, err := packed.GetDeltaLogsFromManifestWithExtfs(manifestPath, t.req.GetStorageConfig(), extfs)
	if err != nil {
		return nil, merr.Wrapf(err, "read milvus-table source manifest deltalogs %s", manifestPath)
	}
	if err := validateMilvusTableStorageV3Deltalogs(deltalogs); err != nil {
		return nil, err
	}
	if err := populateDeltalogIDsFromPath(deltalogs); err != nil {
		return nil, err
	}

	t.milvusTableSourceDeltalogsMu.Lock()
	if t.milvusTableSourceDeltalogs == nil {
		t.milvusTableSourceDeltalogs = make(map[string][]*datapb.FieldBinlog)
	}
	t.milvusTableSourceDeltalogs[manifestPath] = cloneFieldBinlogs(deltalogs)
	t.milvusTableSourceDeltalogsMu.Unlock()

	return deltalogs, nil
}

func validateMilvusTableStorageV3Deltalogs(binlogs []*datapb.FieldBinlog) error {
	for _, fieldBinlog := range binlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogPath() == "" {
				continue
			}
			if err := packed.ValidateMilvusTableStorageV3DeltalogPath(binlog.GetLogPath()); err != nil {
				return err
			}
		}
	}
	return nil
}

// populateDeltalogIDsFromPath fills missing source LogID values from stable
// deltalog path basenames. StorageV3 segment deltas use _delta/<logID>; snapshot
// L0 delete overlays may still use legacy delta_log/.../<logID>.
func populateDeltalogIDsFromPath(binlogs []*datapb.FieldBinlog) error {
	for _, fieldBinlog := range binlogs {
		for _, binlog := range fieldBinlog.GetBinlogs() {
			if binlog.GetLogID() != 0 {
				continue
			}
			if binlog.GetLogPath() == "" {
				continue
			}
			logID, err := parseMilvusTableDeltalogIDFromPath(binlog.GetLogPath())
			if err != nil {
				return err
			}
			binlog.LogID = logID
		}
	}
	return nil
}

// parseMilvusTableDeltalogIDFromPath extracts the numeric log ID from a
// milvus-table source deltalog path.
func parseMilvusTableDeltalogIDFromPath(logPath string) (int64, error) {
	if err := packed.ValidateMilvusTableSourceDeltalogPath(logPath); err != nil {
		return 0, err
	}
	logName := path.Base(strings.TrimRight(logPath, "/"))
	logID, err := strconv.ParseInt(logName, 10, 64)
	if err != nil || logID <= 0 {
		return 0, merr.WrapErrServiceInternalMsg("milvus-table deltalog path %s must end with a positive numeric log ID", logPath)
	}
	return logID, nil
}

// translateMilvusTableDeltalogsToVirtualPKManifest rewrites source-PK deletes
// into target-owned virtual-PK deltalogs and returns the manifest that points to
// those target deltas.
func (t *RefreshExternalCollectionTask) translateMilvusTableDeltalogsToVirtualPKManifest(
	ctx context.Context,
	basePath string,
	manifestPath string,
	segmentID int64,
	fragments []packed.Fragment,
) (string, error) {
	refs, err := collectMilvusTableDeltalogRefs(fragments)
	if err != nil {
		return "", err
	}
	if len(refs) == 0 {
		return manifestPath, nil
	}

	sourcePKField, err := t.getMilvusTableSourcePKField()
	if err != nil {
		return "", err
	}

	var entries []packed.DeltaLogEntry
	for batchStart := 0; batchStart < len(refs); batchStart += milvusTableVirtualPKDeltalogBatchSize {
		batchEnd := batchStart + milvusTableVirtualPKDeltalogBatchSize
		if batchEnd > len(refs) {
			batchEnd = len(refs)
		}
		batchRefs := refs[batchStart:batchEnd]
		batchDeletes := make([]milvusTableSourceDeltalogDeletes, 0, len(batchRefs))
		deletedSourcePKKeys := make(map[string]struct{})
		for _, ref := range batchRefs {
			if err := ensureContext(ctx); err != nil {
				return "", err
			}
			deletes, refDeletedSourcePKKeys, err := t.loadMilvusTableSourceDeltalogDeletes(
				ctx,
				ref,
				sourcePKField.GetDataType(),
			)
			if err != nil {
				return "", err
			}
			batchDeletes = append(batchDeletes, deletes)
			for key := range refDeletedSourcePKKeys {
				deletedSourcePKKeys[key] = struct{}{}
			}
		}

		sourcePKOffsets, err := t.loadMilvusTableSourcePKOffsets(ctx, fragments, sourcePKField, deletedSourcePKKeys)
		if err != nil {
			return "", err
		}
		for _, deletes := range batchDeletes {
			if err := ensureContext(ctx); err != nil {
				return "", err
			}
			entry, err := t.writeMilvusTableVirtualPKDeltalog(
				ctx,
				basePath,
				segmentID,
				sourcePKOffsets,
				deletes,
			)
			if err != nil {
				return "", err
			}
			entries = append(entries, entry)
		}
	}
	if len(entries) == 0 {
		return manifestPath, nil
	}
	return packed.AddDeltaLogsToManifestOverwrite(manifestPath, t.req.GetStorageConfig(), entries)
}

// collectMilvusTableDeltalogRefs collects unique source deltalog paths from the
// fragments selected for a target segment.
func collectMilvusTableDeltalogRefs(fragments []packed.Fragment) ([]milvusTableDeltalogRef, error) {
	seen := make(map[string]struct{})
	var refs []milvusTableDeltalogRef
	for _, fragment := range fragments {
		for _, fieldBinlog := range fragment.Deltalogs {
			for _, binlog := range fieldBinlog.GetBinlogs() {
				sourcePath := binlog.GetLogPath()
				if sourcePath == "" {
					continue
				}
				if _, ok := seen[sourcePath]; ok {
					continue
				}
				if err := packed.ValidateMilvusTableSourceDeltalogPath(sourcePath); err != nil {
					return nil, err
				}
				seen[sourcePath] = struct{}{}
				refs = append(refs, milvusTableDeltalogRef{
					sourcePath: sourcePath,
					logID:      binlog.GetLogID(),
					numEntries: binlog.GetEntriesNum(),
				})
			}
		}
	}
	return refs, nil
}

// getMilvusTableSourcePKField loads and caches the source snapshot schema's
// supported primary-key field for virtual-PK delete translation.
func (t *RefreshExternalCollectionTask) getMilvusTableSourcePKField() (*schemapb.FieldSchema, error) {
	t.milvusTableSourcePKFieldMu.Lock()
	if t.milvusTableSourcePKField != nil {
		sourcePKField := t.milvusTableSourcePKField
		t.milvusTableSourcePKFieldMu.Unlock()
		return sourcePKField, nil
	}
	t.milvusTableSourcePKFieldMu.Unlock()

	extfs := t.milvusTableExternalSpecContext()
	metadata, err := packed.ReadMilvusTableSnapshotMetadata(
		t.req.GetExternalSource(),
		t.req.GetExternalSpec(),
		t.req.GetStorageConfig(),
		extfs,
	)
	if err != nil {
		return nil, err
	}
	sourceSchema := metadata.GetCollection().GetSchema()
	sourcePKField, err := typeutil.GetPrimaryFieldSchema(sourceSchema)
	if err != nil {
		return nil, merr.Wrap(err, "milvus-table source schema primary key")
	}
	switch sourcePKField.GetDataType() {
	case schemapb.DataType_Int64, schemapb.DataType_VarChar:
		t.milvusTableSourcePKFieldMu.Lock()
		if t.milvusTableSourcePKField != nil {
			sourcePKField = t.milvusTableSourcePKField
		} else {
			t.milvusTableSourcePKField = sourcePKField
		}
		t.milvusTableSourcePKFieldMu.Unlock()
		return sourcePKField, nil
	default:
		return nil, merr.WrapErrServiceInternalMsg("milvus-table source primary key type %s is unsupported", sourcePKField.GetDataType())
	}
}

// milvusTableExternalSpecContext builds the extfs context used to read source
// manifests, stats, and deltalogs. Bare local paths intentionally clear Source.
func (t *RefreshExternalCollectionTask) milvusTableExternalSpecContext() packed.ExternalSpecContext {
	source := t.req.GetExternalSource()
	if !packed.IsRemoteObjectURL(source) {
		source = ""
	}
	return packed.ExternalSpecContext{
		CollectionID: t.req.GetCollectionID(),
		Source:       source,
		Spec:         t.req.GetExternalSpec(),
	}
}

// loadMilvusTableSourcePKOffsets maps deleted source primary keys to target
// row offsets, which are later encoded into virtual PKs.
//
// INVARIANT: the target segment's row order MUST equal the source row order
// produced by iterating `fragments` in this exact slice order and, within each
// fragment, by ascending source row offset. The target offset is computed as
// `targetStartOffset + sourceOffset - fragment.StartRow`, where targetStartOffset
// accumulates fragment.RowCount in the same iteration order. If the target
// manifest builder ever reorders fragments or rows, the same `fragments` slice
// (same order) must be handed to both the builder and this function, otherwise
// virtual-PK deletes will be applied to the wrong rows.
func (t *RefreshExternalCollectionTask) loadMilvusTableSourcePKOffsets(
	ctx context.Context,
	fragments []packed.Fragment,
	sourcePKField *schemapb.FieldSchema,
	deletedSourcePKKeys map[string]struct{},
) (map[string][]milvusTableSourcePKOffset, error) {
	sourcePKOffsets := make(map[string][]milvusTableSourcePKOffset)
	if len(deletedSourcePKKeys) == 0 {
		return sourcePKOffsets, nil
	}
	var targetStartOffset int64
	for _, fragment := range fragments {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		if err := t.loadMilvusTableFragmentSourcePKOffsets(
			ctx,
			fragment,
			targetStartOffset,
			sourcePKField,
			deletedSourcePKKeys,
			sourcePKOffsets,
		); err != nil {
			return nil, err
		}
		targetStartOffset += fragment.RowCount
	}
	return sourcePKOffsets, nil
}

// loadMilvusTableFragmentSourcePKOffsets scans one source manifest fragment and
// records target offsets for only the source PKs that have delete events. This
// is an O(rows) PK/timestamp column scan and should stay limited to virtual-PK
// delete translation, where source-PK deletes must be mapped back to target row
// offsets before the target deltalog can be written.
func (t *RefreshExternalCollectionTask) loadMilvusTableFragmentSourcePKOffsets(
	ctx context.Context,
	fragment packed.Fragment,
	targetStartOffset int64,
	sourcePKField *schemapb.FieldSchema,
	deletedSourcePKKeys map[string]struct{},
	sourcePKOffsets map[string][]milvusTableSourcePKOffset,
) error {
	if fragment.FilePath == "" {
		return merr.WrapErrServiceInternalMsg("milvus-table fragment has empty source manifest path")
	}
	sourceSchema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			sourcePKField,
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
		},
	}
	reader, err := storage.NewManifestReaderWithExtfs(
		fragment.FilePath,
		sourceSchema,
		packed.DefaultReadBufferSize,
		t.req.GetStorageConfig(),
		nil,
		t.milvusTableExternalSpecContext(),
	)
	if err != nil {
		return merr.Wrapf(err, "read milvus-table source primary key column %s", fragment.FilePath)
	}
	defer reader.Close()

	var sourceOffset int64
	for {
		if err := ensureContext(ctx); err != nil {
			return err
		}
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return merr.Wrapf(err, "read milvus-table source primary key batch %s", fragment.FilePath)
		}
		if err := appendMilvusTableSourcePKOffsetsFromRecord(
			record,
			sourcePKField,
			fragment,
			targetStartOffset,
			sourceOffset,
			deletedSourcePKKeys,
			sourcePKOffsets,
		); err != nil {
			record.Release()
			return err
		}
		sourceOffset += int64(record.Len())
		record.Release()
	}
	return nil
}

// appendMilvusTableSourcePKOffsetsFromRecord appends target offsets for matching
// deleted PKs from one source data batch.
func appendMilvusTableSourcePKOffsetsFromRecord(
	record storage.Record,
	sourcePKField *schemapb.FieldSchema,
	fragment packed.Fragment,
	targetStartOffset int64,
	batchSourceOffset int64,
	deletedSourcePKKeys map[string]struct{},
	sourcePKOffsets map[string][]milvusTableSourcePKOffset,
) error {
	pkColumn := record.Column(sourcePKField.GetFieldID())
	tsColumn := record.Column(common.TimeStampField)
	tsArray, ok := tsColumn.(*array.Int64)
	if !ok {
		return merr.WrapErrServiceInternalMsg("milvus-table source timestamp column has unexpected type %T", tsColumn)
	}
	for i := 0; i < record.Len(); i++ {
		sourceOffset := batchSourceOffset + int64(i)
		if sourceOffset < fragment.StartRow || sourceOffset >= fragment.EndRow {
			continue
		}
		key, err := milvusTablePrimaryKeyMapKey(sourcePKField.GetDataType(), pkColumn, i)
		if err != nil {
			return err
		}
		if _, ok := deletedSourcePKKeys[key]; !ok {
			continue
		}
		sourcePKOffsets[key] = append(sourcePKOffsets[key], milvusTableSourcePKOffset{
			targetOffset:    targetStartOffset + sourceOffset - fragment.StartRow,
			insertTimestamp: storage.Timestamp(tsArray.Value(i)),
		})
	}
	return nil
}

// loadMilvusTableSourceDeltalogDeletes reads one source deltalog and returns
// its delete events plus the source PKs to look up in source data fragments.
func (t *RefreshExternalCollectionTask) loadMilvusTableSourceDeltalogDeletes(
	ctx context.Context,
	ref milvusTableDeltalogRef,
	sourcePKType schemapb.DataType,
) (milvusTableSourceDeltalogDeletes, map[string]struct{}, error) {
	deletedSourcePKKeys := make(map[string]struct{})
	if ref.logID == 0 {
		return milvusTableSourceDeltalogDeletes{}, nil, merr.WrapErrServiceInternalMsg("milvus-table source deltalog %s has no allocated log ID", ref.sourcePath)
	}
	reader, err := t.newMilvusTableSourceDeltalogReader(ctx, ref, sourcePKType)
	if err != nil {
		return milvusTableSourceDeltalogDeletes{}, nil, merr.Wrapf(err, "read milvus-table source deltalog %s", ref.sourcePath)
	}

	deletes := milvusTableSourceDeltalogDeletes{ref: ref}
	for {
		if err := ensureContext(ctx); err != nil {
			_ = reader.Close()
			return milvusTableSourceDeltalogDeletes{}, nil, err
		}
		record, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			_ = reader.Close()
			return milvusTableSourceDeltalogDeletes{}, nil, merr.Wrapf(err, "read milvus-table source deltalog %s", ref.sourcePath)
		}
		if err := appendMilvusTableSourceDeleteEventsFromRecord(
			record,
			sourcePKType,
			deletedSourcePKKeys,
			&deletes.events,
		); err != nil {
			record.Release()
			_ = reader.Close()
			return milvusTableSourceDeltalogDeletes{}, nil, err
		}
		record.Release()
	}
	if err := reader.Close(); err != nil {
		return milvusTableSourceDeltalogDeletes{}, nil, err
	}
	return deletes, deletedSourcePKKeys, nil
}

func (t *RefreshExternalCollectionTask) newMilvusTableSourceDeltalogReader(
	ctx context.Context,
	ref milvusTableDeltalogRef,
	sourcePKType schemapb.DataType,
) (storage.RecordReader, error) {
	if packed.IsMilvusTableStorageV3DeltalogPath(ref.sourcePath) {
		return storage.NewDeltalogReader(
			sourcePKType,
			[]string{ref.sourcePath},
			storage.WithVersion(storage.StorageV3),
			storage.WithStorageConfig(t.req.GetStorageConfig()),
			storage.WithExternalReaderContext(t.milvusTableExternalSpecContext()),
		)
	}
	if err := packed.ValidateMilvusTableSourceDeltalogPath(ref.sourcePath); err != nil {
		return nil, err
	}
	return storage.NewDeltalogReader(
		sourcePKType,
		[]string{ref.sourcePath},
		storage.WithVersion(storage.StorageV1),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return t.readMilvusTableSourceFiles(ctx, paths)
		}),
	)
}

func (t *RefreshExternalCollectionTask) readMilvusTableSourceFiles(ctx context.Context, paths []string) ([][]byte, error) {
	extfs := t.milvusTableExternalSpecContext()
	files := make([][]byte, 0, len(paths))
	for _, filePath := range paths {
		if err := ensureContext(ctx); err != nil {
			return nil, err
		}
		data, err := packed.ReadFileWithExternalSpecContext(ctx, t.req.GetStorageConfig(), filePath, extfs)
		if err != nil {
			return nil, merr.Wrapf(err, "read milvus-table source file %s", filePath)
		}
		files = append(files, data)
	}
	return files, nil
}

// appendMilvusTableSourceDeleteEventsFromRecord decodes one deltalog batch into
// source-PK delete events.
func appendMilvusTableSourceDeleteEventsFromRecord(
	record storage.Record,
	sourcePKType schemapb.DataType,
	deletedSourcePKKeys map[string]struct{},
	events *[]milvusTableSourceDeleteEvent,
) error {
	pkColumn := record.Column(0)
	tsColumn := record.Column(common.TimeStampField)
	tsArray, ok := tsColumn.(*array.Int64)
	if !ok {
		return merr.WrapErrServiceInternalMsg("milvus-table deltalog timestamp column has unexpected type %T", tsColumn)
	}
	for i := 0; i < record.Len(); i++ {
		key, err := milvusTablePrimaryKeyMapKey(sourcePKType, pkColumn, i)
		if err != nil {
			return err
		}
		deletedSourcePKKeys[key] = struct{}{}
		*events = append(*events, milvusTableSourceDeleteEvent{
			sourcePKKey:     key,
			deleteTimestamp: storage.Timestamp(tsArray.Value(i)),
		})
	}
	return nil
}

// writeMilvusTableVirtualPKDeltalog writes one target-owned packed deltalog for
// translated virtual-PK deletes that originated from a source deltalog.
func (t *RefreshExternalCollectionTask) writeMilvusTableVirtualPKDeltalog(
	ctx context.Context,
	basePath string,
	segmentID int64,
	sourcePKOffsets map[string][]milvusTableSourcePKOffset,
	deletes milvusTableSourceDeltalogDeletes,
) (packed.DeltaLogEntry, error) {
	targetPath := metautil.BuildDeltaLogPathV3(basePath, deletes.ref.logID)
	virtualPKs, timestamps := buildMilvusTableVirtualPKDeletes(segmentID, sourcePKOffsets, deletes.events)
	if len(virtualPKs) == 0 {
		return packed.DeltaLogEntry{Path: targetPath}, nil
	}

	record, _, _, err := storage.BuildDeleteRecord(virtualPKs, timestamps)
	if err != nil {
		return packed.DeltaLogEntry{}, err
	}
	defer record.Release()
	// NewDeltalogWriter only implements StorageV1/StorageV2 (StorageV3 returns
	// an error). StorageV2 here is the packed deltalog format, which the load
	// path reads via NewDeltalogReader where StorageV2 and StorageV3 share the
	// same packed reader. So a V2-written deltalog is read back identically by
	// the StorageV3 reader used in segment load.
	writer, err := storage.NewDeltalogWriter(
		ctx,
		t.req.GetCollectionID(),
		t.req.GetPartitionID(),
		segmentID,
		deletes.ref.logID,
		schemapb.DataType_Int64,
		targetPath,
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(t.req.GetStorageConfig()),
	)
	if err != nil {
		return packed.DeltaLogEntry{}, merr.Wrapf(err, "create milvus-table target deltalog %s", targetPath)
	}
	writerClosed := false
	defer func() {
		if !writerClosed {
			_ = writer.Close()
		}
	}()
	if err := writer.Write(record); err != nil {
		return packed.DeltaLogEntry{}, merr.Wrapf(err, "write milvus-table target deltalog %s", targetPath)
	}
	if err := writer.Close(); err != nil {
		writerClosed = true
		return packed.DeltaLogEntry{}, merr.Wrapf(err, "write milvus-table target deltalog %s", targetPath)
	}
	writerClosed = true
	return packed.DeltaLogEntry{
		Path:       targetPath,
		NumEntries: int64(len(virtualPKs)),
	}, nil
}

// buildMilvusTableVirtualPKDeletes converts source-PK delete events into target
// virtual-PK delete rows after applying insert/delete timestamp visibility.
func buildMilvusTableVirtualPKDeletes(
	segmentID int64,
	sourcePKOffsets map[string][]milvusTableSourcePKOffset,
	deleteEvents []milvusTableSourceDeleteEvent,
) ([]storage.PrimaryKey, []storage.Timestamp) {
	var virtualPKs []storage.PrimaryKey
	var timestamps []storage.Timestamp
	for _, event := range deleteEvents {
		offsets, ok := sourcePKOffsets[event.sourcePKKey]
		if !ok {
			continue
		}
		for _, offset := range offsets {
			// Match segcore delete semantics: same-timestamp deletes do not
			// remove the inserted row.
			if offset.insertTimestamp >= event.deleteTimestamp {
				continue
			}
			virtualPKs = append(virtualPKs, storage.NewInt64PrimaryKey(typeutil.GetVirtualPK(segmentID, offset.targetOffset)))
			timestamps = append(timestamps, event.deleteTimestamp)
		}
	}
	return virtualPKs, timestamps
}

// milvusTablePrimaryKeyMapKey normalizes supported source PK values into map
// keys used while joining source deltalog deletes with source data rows.
func milvusTablePrimaryKeyMapKey(pkType schemapb.DataType, pkColumn arrow.Array, index int) (string, error) {
	switch pkType {
	case schemapb.DataType_Int64:
		int64Array, ok := pkColumn.(*array.Int64)
		if !ok {
			return "", merr.WrapErrServiceInternalMsg("milvus-table int64 primary key column has unexpected type %T", pkColumn)
		}
		return "i:" + strconv.FormatInt(int64Array.Value(index), 10), nil
	case schemapb.DataType_VarChar:
		stringArray, ok := pkColumn.(*array.String)
		if !ok {
			return "", merr.WrapErrServiceInternalMsg("milvus-table varchar primary key column has unexpected type %T", pkColumn)
		}
		return "s:" + stringArray.Value(index), nil
	default:
		return "", merr.WrapErrServiceInternalMsg("milvus-table source primary key type %s is unsupported", pkType)
	}
}
