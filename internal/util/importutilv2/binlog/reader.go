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
	"path"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/samber/lo"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type reader struct {
	ctx            context.Context
	cm             storage.ChunkManager
	storageConfig  *indexpb.StorageConfig
	schema         *schemapb.CollectionSchema
	storageVersion int64
	importEz       string

	fileSize         *atomic.Int64
	bufferSize       int
	retryAttempts    uint
	deleteData       map[any]typeutil.Timestamp // pk2ts
	insertLogs       map[int64][]string         // fieldID (or fieldGroupID if storage v2) -> binlogs
	storageV3Files   []string                   // segment-local files plus manifest-owned LOB files
	storageV3LobSize int64                      // manifest-reported LOB size when available

	filters []Filter
	dr      storage.DeserializeReader[*storage.Value]
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
	err := r.init(paths, tsStart, tsEnd)
	if err != nil {
		return nil, err
	}
	return r, nil
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
		return r.initStorageV3(paths[0], tsStart, tsEnd)
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

func (r *reader) initStorageV3(segmentBasePath string, tsStart, tsEnd uint64) error {
	// StorageV3 TEXT import uses milvus-storage SegmentReader. Its current C
	// API cannot receive Milvus' key-retriever context, so encrypted backups
	// must fail before any manifest or LOB file is opened. CMEK support requires
	// extending that existing reader API and is intentionally outside this PR.
	if r.importEz != "" {
		return merr.WrapErrOperationNotSupportedMsg(
			"CMEK-protected StorageV3 backup import is not supported",
		)
	}

	manifestPath := packed.MarshalManifestPath(segmentBasePath, packed.ManifestLatest)
	rwOptions := []storage.RwOption{
		storage.WithVersion(storage.StorageV3),
		storage.WithBufferSize(32 * 1024 * 1024),
		storage.WithStorageConfig(r.storageConfig),
		storage.WithResolveTextLob(),
	}

	rr, err := storage.NewManifestRecordReader(r.ctx, manifestPath, r.schema, rwOptions...)
	if err != nil {
		return err
	}
	r.dr = storage.NewDeserializeReader(rr, func(record storage.Record, v []*storage.Value) error {
		return storage.ValueDeserializerWithSchema(record, v, r.schema, true)
	})

	if err := r.collectStorageV3Files(segmentBasePath, manifestPath); err != nil {
		r.dr.Close()
		return err
	}

	deltaPaths, err := packed.GetDeltaLogPathsFromManifest(manifestPath, r.storageConfig)
	if err != nil {
		r.dr.Close()
		return merr.Wrap(err, "failed to read StorageV3 deltalogs from manifest")
	}
	if len(deltaPaths) == 0 {
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

func (r *reader) collectStorageV3Files(segmentBasePath, manifestPath string) error {
	r.storageV3LobSize = 0
	files := make(map[string]struct{})
	// WalkWithPrefix matches raw string prefixes. Keep the directory boundary
	// so segment "10" cannot include files owned by a sibling such as "100".
	segmentPrefix := strings.TrimSuffix(path.Clean(segmentBasePath), "/") + "/"
	err := importcommon.WalkWithPrefixRetry(r.ctx, r.cm, segmentPrefix, true, r.retryAttempts,
		func() {
			clear(files)
		},
		func(info *storage.ChunkObjectInfo) bool {
			files[info.FilePath] = struct{}{}
			return true
		})
	if err != nil {
		return err
	}

	lobFiles, err := packed.GetManifestLobFiles(manifestPath, r.storageConfig)
	if err != nil {
		return merr.Wrap(err, "failed to read StorageV3 LOB files from manifest")
	}
	for _, lobFile := range lobFiles {
		if lobFile.FileSizeBytes > 0 {
			r.storageV3LobSize += lobFile.FileSizeBytes
			continue
		}
		if lobFile.Path != "" {
			files[lobFile.Path] = struct{}{}
		}
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
	var result [][]byte
	representative := ""
	if len(paths) > 0 {
		representative = paths[0]
	}
	err := retry.Handle(ctx, func() (bool, error) {
		var e error
		result, e = r.cm.MultiRead(ctx, paths)
		if e == nil {
			return false, nil
		}
		e = storage.ToMilvusIoError(representative, e)
		if merr.IsNonRetryableErr(e) {
			return false, e
		}
		return true, e
	}, retry.Attempts(r.retryAttempts))
	return result, err
}

func (r *reader) Read() (*storage.InsertData, error) {
	insertData, err := storage.NewInsertDataWithFunctionOutputField(r.schema)
	if err != nil {
		return nil, err
	}
	rowNum := 0
	for {
		v, err := r.dr.NextValue()
		if err == io.EOF {
			if insertData.GetRowNum() == 0 {
				return nil, io.EOF
			}
			break
		}
		if err != nil {
			return nil, err
		}
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

			err := fieldData.AppendRow((*v).Value.(map[int64]any)[field.GetFieldID()])
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
}
