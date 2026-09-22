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

package binlog

import (
	"context"
	"io"
	"math"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type reader struct {
	ctx              context.Context
	cm               storage.ChunkManager
	storageConfig    *indexpb.StorageConfig
	schema           *schemapb.CollectionSchema
	storageVersion   int64
	importEz         string
	sourceEncryption SourceEncryption

	fileSize         *atomic.Int64
	bufferSize       int
	retryAttempts    uint
	deleteData       map[any]typeutil.Timestamp // pk2ts
	insertLogs       map[int64][]string         // fieldID (or fieldGroupID if storage v2) -> binlogs
	storageV3Files   []string                   // exact manifest data files plus LOB files without reported sizes
	storageV3LobSize int64                      // manifest-reported LOB size when available
	validatePath     func(string) error

	filters []Filter
	dr      storage.DeserializeReader[*storage.Value]

	// Coordinator-expanded jobs with applicable L0, nonzero source commit
	// timestamps, or external storage carry this context. Segment-local deletes
	// need the same commit-time override as standalone L0 deletes.
	snapshotSource *internalpb.SnapshotImportSource
	readErr        error
	deleteBudget   int64
	deleteMask     *rowDeleteMask
	rawRow         int64
}

// SourceEncryption is resolved by the task's factory, not by each record reader.
// Encrypted is independent of PluginContext: an encrypted source may resolve to
// a nil context when the cluster encryption plugin is disabled.
type SourceEncryption struct {
	Encrypted     bool
	PluginContext *indexcgopb.StoragePluginContext
}

func NewReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	storageVersion int64,
	paths []string,
	tsStart,
	tsEnd uint64,
	bufferSize int,
	importEz string,
) (*reader, error) {
	r := newReader(ctx, cm, schema, storageConfig, storageVersion, bufferSize, importEz)
	err := r.init(paths, tsStart, tsEnd)
	if err != nil {
		return nil, err
	}
	return r, nil
}

// NewStorageV3ManifestReader opens one exact manifest captured by snapshot
// metadata. Keeping this constructor distinct from legacy path-based backup
// import prevents an object key from being interpreted through content-shaped
// path heuristics and prevents fallback to ManifestLatest.
// source is nil only when neither L0, commit-time overrides nor external storage
// require context. Any descriptor activates bounded reads and preserves source
// commit timestamps. L0 deletes are supplied only through task-prepared bitmaps.
func NewStorageV3ManifestReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	manifestPath string,
	tsStart,
	tsEnd uint64,
	bufferSize int,
	encryption SourceEncryption,
	source *internalpb.SnapshotImportSource,
	snapshotDeleteBudget int64,
	validate func(string) error,
) (*reader, error) {
	r := newReader(ctx, cm, schema, storageConfig, storage.StorageV3, bufferSize, "")
	r.sourceEncryption = encryption
	r.validatePath = validate
	return initStorageV3ManifestReader(r, manifestPath, tsStart, tsEnd, source, snapshotDeleteBudget, nil)
}

// NewStorageV3ManifestReaderWithSharedL0 borrows a prepared bitmap containing
// shared L0 and segment-local deletes, evaluated at the source's timestamp.
func NewStorageV3ManifestReaderWithSharedL0(ctx context.Context, cm storage.ChunkManager,
	schema *schemapb.CollectionSchema, storageConfig *indexpb.StorageConfig, manifestPath string,
	tsStart, tsEnd uint64, bufferSize int, encryption SourceEncryption, source *internalpb.SnapshotImportSource,
	snapshotDeleteBudget int64, shared *SnapshotL0Deletes,
	validate func(string) error,
) (*reader, error) {
	if shared == nil {
		return nil, merr.WrapErrServiceInternalMsg("shared L0 reader requires prepared bitmaps")
	}
	r := newReader(ctx, cm, schema, storageConfig, storage.StorageV3, bufferSize, "")
	r.sourceEncryption = encryption
	r.validatePath = validate
	return initStorageV3ManifestReader(r, manifestPath, tsStart, tsEnd, source, snapshotDeleteBudget, shared)
}

func initStorageV3ManifestReader(r *reader, manifestPath string, tsStart, tsEnd uint64,
	source *internalpb.SnapshotImportSource, snapshotDeleteBudget int64, shared *SnapshotL0Deletes,
) (*reader, error) {
	if source != nil {
		// Versions 3/4 add coordinator-owned partition routing; row/delete
		// decoding remains identical to versions 1/2.
		if source.GetVersion() < 1 || source.GetVersion() > 4 {
			return nil, merr.Wrapf(merr.ErrServiceUnimplemented, "unsupported snapshot import source version %d", source.GetVersion())
		}
		// Reject development-version inline inventories before any storage IO.
		// Ignoring them would resurrect deleted rows; reopening them here would
		// restore the per-segment L0 loading that task sharing replaces.
		if len(source.GetLegacyL0Deltalogs())+len(source.GetManifestL0Deltalogs()) != 0 {
			return nil, merr.Wrapf(merr.ErrServiceUnimplemented, "snapshot import tasks with inline L0 are no longer supported; resubmit the snapshot import")
		}
		if source.GetManifestPath() != manifestPath {
			return nil, merr.WrapErrServiceInternalMsg("snapshot import source does not match the selected manifest")
		}
		r.snapshotSource = source
		// Both phases pass the exact budget reserved before opening us. Do not
		// reread configuration here: a refresh must not change that reservation.
		r.deleteBudget = snapshotDeleteBudget
		if r.deleteBudget <= 0 {
			return nil, merr.Wrapf(merr.ErrServiceResourceInsufficient, "snapshot delete-map budget must be positive")
		}
	}
	if shared != nil {
		if source == nil {
			return nil, merr.WrapErrServiceInternalMsg("shared L0 reader requires a typed source")
		}
		r.deleteMask = shared.masks[maskKey(source)]
		if r.deleteMask == nil {
			return nil, merr.WrapErrServiceInternalMsg("snapshot task has no prepared bitmap for source manifest")
		}
	}
	if tsStart != 0 || tsEnd != math.MaxUint64 {
		r.filters = append(r.filters, FilterWithTimeRange(tsStart, tsEnd))
	}
	if err := validateStorageV3ManifestPath(manifestPath, r.validatePath); err != nil {
		return nil, err
	}
	if err := r.initStorageV3Manifest(manifestPath, tsStart, tsEnd); err != nil {
		return nil, err
	}
	return r, nil
}

func validateStorageV3ManifestPath(manifestPath string, validate func(string) error) error {
	if strings.TrimSpace(manifestPath) == "" {
		return merr.WrapErrImportFailed("no StorageV3 manifest to import")
	}
	base, version, err := packed.UnmarshalManifestPath(manifestPath)
	if err != nil {
		return merr.WrapErrImportFailedMsg("invalid StorageV3 manifest path: %s", err)
	}
	if version == packed.ManifestLatest {
		return merr.WrapErrImportFailedMsg(
			"snapshot-source import requires an exact StorageV3 manifest version",
		)
	}
	if validate != nil {
		return validate(base)
	}
	return nil
}

func newReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	storageConfig *indexpb.StorageConfig,
	storageVersion int64,
	bufferSize int,
	importEz string,
) *reader {
	systemFieldsAbsent := true
	for _, field := range schema.Fields {
		if field.GetFieldID() < 100 {
			systemFieldsAbsent = false
			break
		}
	}
	if systemFieldsAbsent {
		schema = typeutil.AppendSystemFields(schema)
	}
	r := &reader{
		ctx:            ctx,
		cm:             cm,
		schema:         schema,
		storageVersion: storageVersion,
		fileSize:       atomic.NewInt64(0),
		bufferSize:     bufferSize,
		storageConfig:  storageConfig,
		importEz:       importEz,
		retryAttempts:  paramtable.Get().CommonCfg.StorageReadRetryAttempts.GetAsUint(),
	}
	return r
}

func (r *reader) init(paths []string, tsStart, tsEnd uint64) error {
	if tsStart != 0 || tsEnd != math.MaxUint64 {
		r.filters = append(r.filters, FilterWithTimeRange(tsStart, tsEnd))
	}
	if len(paths) == 0 {
		return merr.WrapErrImportFailed("no insert binlogs to import")
	}
	// the "paths" has one or two paths, the first is the binlog path of a segment
	// the other is optional, is the delta path of a segment
	if len(paths) > 2 {
		return merr.WrapErrImportFailedMsg("too many input paths for binlog import. "+
			"Valid paths length should be one or two, but got paths:%s", paths)
	}
	if r.storageVersion == storage.StorageV3 {
		return merr.WrapErrImportFailedMsg(
			"StorageV3 backup import requires an exact manifest from a snapshot source",
		)
	}

	insertLogs, err := listInsertLogs(r.ctx, r.cm, paths[0], r.retryAttempts)
	if err != nil {
		return err
	}

	validInsertLogs, cloneschema, err := verify(r.schema, r.storageVersion, insertLogs)
	if err != nil {
		return err
	}
	binlogs := createFieldBinlogList(validInsertLogs)
	r.insertLogs = validInsertLogs
	r.schema = cloneschema

	validIDs := lo.Keys(r.insertLogs)
	mlog.Info(r.ctx, "create binlog reader for these fields", mlog.Any("validIDs", validIDs))

	rwOptions := []storage.RwOption{
		storage.WithVersion(r.storageVersion),
		storage.WithBufferSize(32 * 1024 * 1024),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return r.multiReadWithRetry(ctx, paths)
		}),
		storage.WithStorageConfig(r.storageConfig),
	}

	if len(r.importEz) > 0 {
		ezID, err := hookutil.GetEzIDByImportEzk(r.importEz)
		if err != nil {
			return err
		}
		pluginContext, err := hookutil.GetCPluginContextByEzID(ezID)
		if err != nil {
			return err
		}
		rwOptions = append(rwOptions, storage.WithPluginContext(pluginContext))
	}

	rr, err := storage.NewBinlogRecordReader(r.ctx, binlogs, r.schema, rwOptions...)
	if err != nil {
		return err
	}

	r.dr = storage.NewDeserializeReader(rr, func(record storage.Record, v []*storage.Value) error {
		return storage.ValueDeserializerWithSchema(record, v, r.schema, true)
	})

	if len(paths) < 2 {
		return nil
	}
	var deltaLogs []string
	err = importcommon.WalkWithPrefixRetry(r.ctx, r.cm, paths[1], true, r.retryAttempts,
		func() {
			deltaLogs = nil
		},
		func(chunkInfo *storage.ChunkObjectInfo) bool {
			deltaLogs = append(deltaLogs, chunkInfo.FilePath)
			return true
		})
	if err != nil {
		return err
	}
	if len(deltaLogs) == 0 {
		return nil
	}
	r.deleteData, err = r.readDelete(deltaLogs, tsStart, tsEnd)
	if err != nil {
		return err
	}
	mlog.Info(context.TODO(), "read delete done",
		mlog.String("collection", r.schema.GetName()),
		mlog.Int("deleteRows", len(r.deleteData)),
	)

	deleteFilter, err := FilterWithDelete(r)
	if err != nil {
		return err
	}
	r.filters = append(r.filters, deleteFilter)
	return nil
}

func (r *reader) prepareStorageV3Manifest(manifestPath string) ([]storage.RwOption, error) {
	// Use the same physical-ID selection as V1 backup import. Select before
	// checking CMEK/TEXT or enabling LOB resolution: a target-only nullable TEXT
	// field needs no source LOB reader and is filled with NULL by Import later.
	// Keep the task's full target schema unchanged; both phases project only
	// this reader's schema using the same exact manifest.
	present, err := packed.GetManifestFieldIDs(manifestPath, r.storageConfig)
	if err != nil {
		return nil, err
	}
	readSchema, err := selectImportFields(r.schema, func(id int64) bool {
		_, ok := present[id]
		return ok
	})
	if err != nil {
		return nil, err
	}
	r.schema = readSchema

	rwOptions := []storage.RwOption{
		storage.WithVersion(storage.StorageV3),
		storage.WithBufferSize(32 * 1024 * 1024),
		storage.WithStorageConfig(r.storageConfig),
		storage.WithPresentFields(present),
	}

	if r.sourceEncryption.Encrypted {
		// TEXT is physically stored as LOB references and therefore needs
		// SegmentReader. Its current C API cannot accept the source key-retriever
		// context. Keep CMEK support on the existing PackedReader path until that
		// shared API grows the required context; do not add an Import-only reader.
		if typeutil.HasTextField(r.schema) {
			return nil, merr.WrapErrOperationNotSupportedMsg(
				"CMEK-protected snapshot-source import does not support TEXT/LOB fields",
			)
		}
	}

	// Always mark the source context as resolved, including an explicit nil.
	// Never fall back to destination CMEK properties. A nil context alone does
	// not prove the source is plaintext; the factory also supplies Encrypted.
	// The target writer resolves its own key independently.
	rwOptions = append(rwOptions, storage.WithPluginContext(r.sourceEncryption.PluginContext))
	if typeutil.HasTextField(r.schema) {
		rwOptions = append(rwOptions, storage.WithResolveTextLob())
	}

	// Validate all discovered references before constructing a data reader.
	// In particular SegmentReader may resolve LOB references during reads.
	if err := r.collectStorageV3Files(manifestPath); err != nil {
		return nil, err
	}
	return rwOptions, nil
}

func (r *reader) initStorageV3Manifest(manifestPath string, tsStart, tsEnd uint64) error {
	rwOptions, err := r.prepareStorageV3Manifest(manifestPath)
	if err != nil {
		return err
	}
	var deltaPaths []string
	if r.deleteMask == nil {
		deltaPaths, err = packed.GetDeltaLogPathsFromManifest(manifestPath, r.storageConfig)
	}
	if err != nil {
		return merr.Wrap(err, "failed to read StorageV3 deltalogs from manifest")
	}
	for i, path := range deltaPaths {
		if r.validatePath != nil {
			if err := r.validatePath(path); err != nil {
				return err
			}
			deltaPaths[i] = snapshotstorage.NormalizeSnapshotObjectPath(path)
		}
	}
	rr, err := storage.NewManifestRecordReader(r.ctx, manifestPath, r.schema, rwOptions...)
	if err != nil {
		return err
	}
	r.dr = storage.NewDeserializeReader(rr, func(record storage.Record, v []*storage.Value) error {
		return storage.ValueDeserializerWithSchema(record, v, r.schema, true)
	})
	if r.deleteMask != nil {
		return nil // Both shared and segment-local deletes are already folded.
	}

	if len(deltaPaths) == 0 && r.snapshotSource == nil {
		return nil
	}

	r.deleteData, err = r.readDeleteV3(deltaPaths, tsStart, tsEnd)
	if err != nil {
		r.dr.Close()
		return err
	}
	deleteFilter, err := FilterWithDelete(r)
	if err != nil {
		r.dr.Close()
		return err
	}
	r.filters = append(r.filters, deleteFilter)
	return nil
}

func (r *reader) collectStorageV3Files(manifestPath string) error {
	r.storageV3LobSize = 0
	files := make(map[string]struct{})
	fragments, err := packed.ReadFragmentsFromManifest(manifestPath, r.storageConfig, nil)
	if err != nil {
		return merr.Wrap(err, "failed to read StorageV3 data files from manifest")
	}
	for _, fragment := range fragments {
		if strings.TrimSpace(fragment.FilePath) == "" {
			return merr.WrapErrDataIntegrityMsg(
				"StorageV3 manifest %s contains a data fragment without a path",
				manifestPath,
			)
		}
		if r.validatePath != nil {
			if err := r.validatePath(fragment.FilePath); err != nil {
				return err
			}
		}
		files[snapshotstorage.NormalizeSnapshotObjectPath(fragment.FilePath)] = struct{}{}
	}
	if !typeutil.HasTextField(r.schema) {
		r.storageV3Files = lo.Keys(files)
		return nil
	}

	lobFiles, err := packed.GetManifestLobFiles(manifestPath, r.storageConfig)
	if err != nil {
		return merr.Wrap(err, "failed to read StorageV3 LOB files from manifest")
	}
	for _, lobFile := range lobFiles {
		if r.validatePath != nil {
			if err := r.validatePath(lobFile.Path); err != nil {
				return err
			}
			// Positive manifest-reported sizes bypass Size() object lookups.
			// Check these too, even if all rows referencing the LOB are deleted.
			exists, err := r.cm.Exist(r.ctx, snapshotstorage.NormalizeSnapshotObjectPath(lobFile.Path))
			if err != nil {
				return merr.Wrap(err, "failed to check snapshot LOB")
			}
			if !exists {
				return merr.WrapErrDataIntegrityMsg("snapshot LOB does not exist: %s", lobFile.Path)
			}
		}
		if strings.TrimSpace(lobFile.Path) == "" {
			return merr.WrapErrDataIntegrityMsg(
				"StorageV3 manifest %s contains a LOB file without a path",
				manifestPath,
			)
		}
		if lobFile.FileSizeBytes < 0 {
			return merr.WrapErrDataIntegrityMsg(
				"StorageV3 manifest %s contains LOB file %s with negative size %d",
				manifestPath,
				lobFile.Path,
				lobFile.FileSizeBytes,
			)
		}
		if lobFile.FileSizeBytes > 0 {
			r.storageV3LobSize += lobFile.FileSizeBytes
			continue
		}
		files[snapshotstorage.NormalizeSnapshotObjectPath(lobFile.Path)] = struct{}{}
	}
	r.storageV3Files = lo.Keys(files)
	return nil
}

func (r *reader) readDelete(deltaLogs []string, tsStart, tsEnd uint64) (map[any]typeutil.Timestamp, error) {
	v1opts := []storage.RwOption{
		storage.WithVersion(storage.StorageV1),
		storage.WithDownloader(func(ctx context.Context, paths []string) ([][]byte, error) {
			return r.multiReadWithRetry(ctx, paths)
		}),
	}
	v2opts := []storage.RwOption{
		storage.WithVersion(storage.StorageV2),
		storage.WithStorageConfig(r.storageConfig),
	}

	deleteData := make(map[any]typeutil.Timestamp)

	readInternal := func(path string, opts []storage.RwOption) (map[any]typeutil.Timestamp, error) {
		tempData := make(map[any]typeutil.Timestamp)
		pkField, err := typeutil.GetPrimaryFieldSchema(r.schema)
		if err != nil {
			return nil, err
		}
		reader, err := storage.NewDeltalogReader(r.ctx, pkField.DataType, []string{path}, opts...)
		if err != nil {
			return nil, err
		}
		defer reader.Close()

		for {
			rec, err := reader.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				mlog.Error(r.ctx, "compose delete wrong, failed to read deltalogs", mlog.Err(err))
				return nil, err
			}

			for i := 0; i < rec.Len(); i++ {
				ts := typeutil.Timestamp(rec.Column(1).(*array.Int64).Value(i))
				if ts < tsStart || ts > tsEnd {
					continue
				}
				var pk any
				switch pkField.DataType {
				case schemapb.DataType_Int64:
					pk = rec.Column(0).(*array.Int64).Value(i)
				case schemapb.DataType_VarChar:
					pk = strings.Clone(rec.Column(0).(*array.String).Value(i))
				}
				if tsExisting, ok := tempData[pk]; ok && tsExisting > ts {
					// skip if existing entry is newer
					continue
				}
				tempData[pk] = ts
			}
		}
		return tempData, nil
	}

	for _, path := range deltaLogs {
		// try v1 first
		tempData, errv1 := readInternal(path, v1opts)
		if errv1 != nil {
			// try v2 if v1 failed
			tempData, errv2 := readInternal(path, v2opts)
			if errv2 != nil {
				return nil, errv2
			}
			// Merge v2 results into deleteData
			for pk, ts := range tempData {
				if tsExisting, ok := deleteData[pk]; ok && tsExisting > ts {
					continue
				}
				deleteData[pk] = ts
			}
		} else {
			// Merge v1 results into deleteData
			for pk, ts := range tempData {
				if tsExisting, ok := deleteData[pk]; ok && tsExisting > ts {
					continue
				}
				deleteData[pk] = ts
			}
		}
	}
	return deleteData, nil
}

func (r *reader) readDeleteV3(
	deltaPaths []string,
	tsStart, tsEnd uint64,
) (map[any]typeutil.Timestamp, error) {
	pkField, err := typeutil.GetPrimaryFieldSchema(r.schema)
	if err != nil {
		return nil, err
	}
	if r.snapshotSource != nil {
		merger, err := NewDeleteMerger(r.cm, r.storageConfig, pkField.GetDataType(), r.retryAttempts, r.deleteBudget)
		if err != nil {
			return nil, err
		}
		return merger.Merge(r.ctx, deltaPaths, tsStart, tsEnd, false)
	}
	options := []storage.RwOption{
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(r.storageConfig),
	}
	// GetDeltaLogPathsFromManifest has already removed zero-entry manifest
	// markers. Read the remaining physical files through the shared path reader
	// so task cancellation is checked between files.
	reader, err := storage.NewDeltalogReader(r.ctx, pkField.DataType, deltaPaths, options...)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	deleteData := make(map[any]typeutil.Timestamp)
	for {
		record, err := reader.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		// RecordReader owns this borrowed record and releases it on the next
		// Next or Close. Releasing it here would double-release Arrow buffers.
		for i := 0; i < record.Len(); i++ {
			ts := typeutil.Timestamp(record.Column(common.TimeStampField).(*array.Int64).Value(i))
			if ts < tsStart || ts > tsEnd {
				continue
			}
			var pk any
			switch pkField.DataType {
			case schemapb.DataType_Int64:
				pk = record.Column(0).(*array.Int64).Value(i)
			case schemapb.DataType_VarChar:
				pk = strings.Clone(record.Column(0).(*array.String).Value(i))
			default:
				return nil, merr.WrapErrDataIntegrityMsg("unsupported primary key type %s in StorageV3 deltalog", pkField.DataType.String())
			}
			if existing, ok := deleteData[pk]; ok && existing > ts {
				continue
			}
			deleteData[pk] = ts
		}
	}
	return deleteData, nil
}

// multiReadWithRetry wraps MultiRead with denylist retry: retries all errors
// except permanent/validation ones (permission denied, bucket not found, etc.),
// matching the strategy used by parquet/json/csv imports via RetryableReader.
func (r *reader) multiReadWithRetry(ctx context.Context, paths []string) ([][]byte, error) {
	return multiReadWithRetry(ctx, r.cm, r.retryAttempts, paths)
}

func multiReadWithRetry(ctx context.Context, cm storage.ChunkManager, attempts uint, paths []string) ([][]byte, error) {
	var result [][]byte
	representative := ""
	if len(paths) > 0 {
		representative = paths[0]
	}
	err := retry.Handle(ctx, func() (bool, error) {
		var e error
		result, e = cm.MultiRead(ctx, paths)
		if e == nil {
			return false, nil
		}
		e = storage.ToMilvusIoError(representative, e)
		if merr.IsNonRetryableErr(e) {
			return false, e
		}
		return true, e
	}, retry.Attempts(attempts))
	return result, err
}

func (r *reader) Read() (*storage.InsertData, error) {
	if r.readErr != nil {
		return nil, r.readErr
	}
	insertData, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
	if err != nil {
		return nil, err
	}
	rowNum := 0
	for {
		if r.snapshotSource != nil {
			if err := r.ctx.Err(); err != nil {
				r.readErr = err
				r.Close()
				return nil, err
			}
		}
		v, err := r.dr.NextValue()
		if err == io.EOF {
			if r.deleteMask != nil && r.rawRow != r.deleteMask.rows {
				r.readErr = merr.WrapErrDataIntegrityMsg("snapshot raw row count changed after bitmap preparation")
				r.Close()
				return nil, r.readErr
			}
			if insertData.GetRowNum() == 0 {
				return nil, io.EOF
			}
			break
		}
		if err != nil {
			return nil, err
		}
		// Validate before any range/delete filter can hide an invalid row.
		if err := validateSnapshotRowTimestamp(r.snapshotSource, uint64((*v).Timestamp)); err != nil {
			r.readErr = err
			// A caller must not retry Read and advance past the corrupt row.
			// Close the borrowed-record owner and keep the failure sticky.
			r.Close()
			return nil, r.readErr
		}
		if r.deleteMask != nil {
			if r.rawRow >= r.deleteMask.rows {
				r.readErr = merr.WrapErrDataIntegrityMsg("snapshot raw row count increased after bitmap preparation")
				r.Close()
				return nil, r.readErr
			}
			deleted := r.deleteMask.deleted(r.rawRow)
			r.rawRow++ // Count even deleted or out-of-range rows across Read calls.
			if deleted {
				continue
			}
		}
		row := (*v).Value.(map[int64]any)
		allFields := typeutil.GetAllFieldSchemas(r.schema)
		// convert record to fieldData
		for _, field := range allFields {
			fieldData := insertData.Data[field.GetFieldID()]
			if fieldData == nil {
				fieldData, err = storage.NewFieldData(field.GetDataType(), field, 1024)
				if err != nil {
					return nil, err
				}
				insertData.Data[field.GetFieldID()] = fieldData
			}

			err := fieldData.AppendRow(row[field.GetFieldID()])
			if err != nil {
				return nil, err
			}
			rowNum++
		}
		if rowNum%100 == 0 && // Prevent frequent memory check
			insertData.GetMemorySize() >= r.bufferSize {
			break
		}
	}
	insertData, err = r.filter(insertData)
	if err != nil {
		return nil, err
	}
	return insertData, nil
}

func (r *reader) filter(insertData *storage.InsertData) (*storage.InsertData, error) {
	if len(r.filters) == 0 {
		return insertData, nil
	}
	masks := make(map[int]struct{}, 0)
OUTER:
	for i := 0; i < insertData.GetRowNum(); i++ {
		row := insertData.GetRow(i)
		for _, f := range r.filters {
			if !f(row) {
				masks[i] = struct{}{}
				continue OUTER
			}
		}
	}
	if len(masks) == 0 { // no data will undergo filtration, return directly
		return insertData, nil
	}
	result, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
	if err != nil {
		return nil, err
	}
	for i := 0; i < insertData.GetRowNum(); i++ {
		if _, ok := masks[i]; ok {
			continue
		}
		row := insertData.GetRow(i)
		err = result.Append(row)
		if err != nil {
			return nil, merr.WrapErrImportFailedMsg("failed to append row, err=%s", err.Error())
		}
	}
	return result, nil
}

func (r *reader) Size() (int64, error) {
	if size := r.fileSize.Load(); size != 0 {
		return size, nil
	}
	paths := lo.Flatten(lo.Values(r.insertLogs))
	baseSize := int64(0)
	if r.storageVersion == storage.StorageV3 {
		paths = r.storageV3Files
		baseSize = r.storageV3LobSize
	}
	size, err := storage.GetFilesSize(r.ctx, paths, r.cm)
	if err != nil {
		return 0, err
	}
	size += baseSize
	r.fileSize.Store(size)
	return size, nil
}

func (r *reader) Close() {
	if r.dr != nil {
		_ = r.dr.Close()
		r.dr = nil
	}
	if r.snapshotSource != nil {
		r.deleteData = nil
		r.deleteMask = nil
	}
}
