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

package importutilv2

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	snapshotstorage "github.com/milvus-io/milvus/internal/snapshotio/storage"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/csv"
	"github.com/milvus-io/milvus/internal/util/importutilv2/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

//go:generate mockery --name=Reader --structname=MockReader --output=./  --filename=mock_reader.go --with-expecter --inpackage
type Reader interface {
	// Size returns the size of the underlying file/files in bytes.
	// It returns an error if the size cannot be determined.
	Size() (int64, error)

	// Read reads data from the underlying file/files.
	// It returns the storage.InsertData and an error, if any.
	Read() (*storage.InsertData, error)

	// Close closes the underlying file reader.
	Close()
}

// ReaderFactory belongs to one task execution. Only resolved source dependencies
// are shared; each NewReader call owns its stream, decoder and request context.
// Options and storage configuration must remain immutable during execution.
type ReaderFactory struct {
	cm      storage.ChunkManager
	cfg     *indexpb.StorageConfig
	options Options
	storage func() (readerStorage, error)
}

type readerStorage struct {
	cm         storage.ChunkManager
	cfg        *indexpb.StorageConfig
	encryption binlog.SourceEncryption
}

func NewReaderFactory(ctx context.Context, cm storage.ChunkManager, cfg *indexpb.StorageConfig, options Options) *ReaderFactory {
	return &ReaderFactory{
		cm: cm, cfg: cfg, options: options,
		storage: sync.OnceValues(func() (readerStorage, error) {
			// Use the task context, never the first file's context. Cache a
			// resolution error for this attempt too; a retry creates a new factory.
			if err := ctx.Err(); err != nil {
				return readerStorage{}, err
			}
			uri, _ := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options)
			sourceCM, sourceCfg, err := ResolveSnapshotImportStorage(ctx, cm, cfg, uri, options)
			if err != nil {
				return readerStorage{}, err
			}
			resolved := readerStorage{cm: sourceCM, cfg: sourceCfg}
			if ezk, _ := GetEZK(options); ezk != "" {
				resolved.encryption.Encrypted = true
				ezID, err := hookutil.GetEzIDByImportEzk(ezk)
				if err != nil {
					return readerStorage{}, err
				}
				resolved.encryption.PluginContext, err = hookutil.GetCPluginContextByEzID(ezID)
				if err != nil {
					return readerStorage{}, err
				}
			}
			return resolved, nil
		}),
	}
}

func (f *ReaderFactory) NewReader(ctx context.Context, schema *schemapb.CollectionSchema, file *internalpb.ImportFile,
	bufferSize int, deleteBudget int64, shared *binlog.SnapshotL0Deletes,
) (Reader, error) {
	if shared == nil && SnapshotSourceUsesSharedL0(file.GetSnapshotSource()) {
		return nil, merr.Wrapf(merr.ErrServiceUnimplemented, "task-shared snapshot source requires prepared deletes")
	}
	if shared != nil && !SnapshotSourceUsesSharedL0(file.GetSnapshotSource()) {
		return nil, merr.WrapErrServiceInternalMsg("shared L0 reader requires a task-shared snapshot descriptor and prepared bitmaps")
	}
	if err := ValidateSnapshotSourceOptions(f.options); err != nil {
		return nil, err
	}
	if file.GetSnapshotSource() != nil || HasExternalSource(f.options) {
		if err := ValidateSnapshotImportFiles([]*internalpb.ImportFile{file}, f.options); err != nil {
			return nil, err
		}
	}
	resolved := readerStorage{cm: f.cm, cfg: f.cfg}
	if IsSnapshotSource(f.options) {
		// Validate the descriptor before accessing storage or the key retriever.
		// Same-bucket and no-L0 snapshots use this path too. Ordinary import
		// retains its existing storage and encryption initialization.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		var err error
		resolved, err = f.storage()
		if err != nil {
			return nil, err
		}
	}
	return newReader(ctx, resolved.cm, schema, file, f.options, bufferSize, resolved.cfg, deleteBudget, shared, resolved.encryption)
}

// PrepareSnapshotDeletes is task-local and shared by PreImport and Import. The
// existing private-map reservation becomes the bitmap pool; neither the wire
// task nor coordinator grouping/slot calculation needs a new field or knob.
func (f *ReaderFactory) PrepareSnapshotDeletes(ctx context.Context, schema *schemapb.CollectionSchema,
	files []*internalpb.ImportFile, source *internalpb.SnapshotImportL0Source, deleteBudget, bitmapBudget int64,
) (*binlog.SnapshotL0Deletes, error) {
	if err := ValidateSnapshotImportTask(files, f.options, source); err != nil {
		return nil, err
	}
	start, end, err := ParseTimeRange(f.options)
	if err != nil {
		return nil, err
	}
	resolved, err := f.storage()
	if err != nil {
		return nil, err
	}
	validate, err := SnapshotPathValidator(f.options, resolved.cm)
	if err != nil {
		return nil, err
	}
	sources := make([]*internalpb.SnapshotImportSource, len(files))
	hasV2 := false
	for i, file := range files {
		sources[i] = file.GetSnapshotSource()
		hasV2 = hasV2 || sources[i].GetV2Reference() != nil
	}
	if !hasV2 {
		return binlog.BuildSnapshotDeleteMasks(ctx, resolved.cm, schema, resolved.cfg, sources, source,
			start, end, deleteBudget, bitmapBudget, resolved.encryption, validate)
	}
	return binlog.BuildSnapshotDeleteMasksWithV2(ctx, resolved.cm, schema, resolved.cfg, sources, source,
		start, end, deleteBudget, bitmapBudget, resolved.encryption, validate,
		func(ctx context.Context, source *internalpb.SnapshotImportSource) (*datapb.SegmentDescription, error) {
			return resolveSnapshotV2Source(ctx, resolved.cm, source, f.options)
		})
}

// NewReader requires the caller's reserved positive delete budget for typed
// snapshot sources. Other inputs do not use that budget and pass zero.
func NewReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	importFile *internalpb.ImportFile,
	options Options,
	bufferSize int,
	storageConfig *indexpb.StorageConfig,
	snapshotDeleteBudget int64,
) (Reader, error) {
	return NewReaderFactory(ctx, cm, storageConfig, options).NewReader(ctx, schema, importFile, bufferSize, snapshotDeleteBudget, nil)
}

func newReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema,
	importFile *internalpb.ImportFile, options Options, bufferSize int, storageConfig *indexpb.StorageConfig,
	snapshotDeleteBudget int64, shared *binlog.SnapshotL0Deletes,
	encryption binlog.SourceEncryption,
) (Reader, error) {
	source := importFile.GetSnapshotSource()
	if source != nil && (shared != nil || source.GetVersion() >= 10) {
		// Adapt only the validated reader view. Never expand L0 paths back
		// into a segment or mutate the persisted descriptor: the reader must
		// borrow the task's prepared bitmap instead of opening L0 again.
		source = proto.Clone(source).(*internalpb.SnapshotImportSource)
		source.Version = SnapshotSourceContractVersion(source)
		if shared != nil {
			source.SourceChannel = ""
			source.SourcePartitionId = 0
		}
	}
	if IsBackup(options) {
		tsStart, tsEnd, err := ParseTimeRange(options)
		if err != nil {
			return nil, err
		}
		paths := importFile.GetPaths()
		if IsSnapshotSource(options) {
			validate, err := SnapshotPathValidator(options, cm)
			if err != nil {
				return nil, err
			}
			if source != nil {
				if source.GetV2Reference() != nil {
					var segment *datapb.SegmentDescription
					if shared == nil {
						segment, err = resolveSnapshotV2Source(ctx, cm, source, options)
						if err != nil {
							return nil, err
						}
					}
					return binlog.NewStorageV2SnapshotReader(ctx, cm, schema, storageConfig, segment,
						tsStart, tsEnd, bufferSize, encryption, source, snapshotDeleteBudget, shared, validate)
				}
				if shared != nil {
					return binlog.NewStorageV3ManifestReaderWithSharedL0(ctx, cm, schema, storageConfig,
						source.GetManifestPath(), tsStart, tsEnd, bufferSize, encryption, source, snapshotDeleteBudget, shared, validate)
				}
				return binlog.NewStorageV3ManifestReader(
					ctx, cm, schema, storageConfig, source.GetManifestPath(), tsStart, tsEnd, bufferSize, encryption, source, snapshotDeleteBudget, validate,
				)
			}
			// DataCoord broadcasts a metadata URI, then expands and persists
			// exact StorageV3 manifests during Pending preparation before dispatch.
			// Do not consult storage_version: snapshot metadata is the source of
			// truth and ValidateSnapshotSourceOptions rejects that ambiguity.
			if len(paths) != 1 {
				return nil, merr.WrapErrImportFailedMsg(
					"snapshot-source import file requires exactly one manifest path",
				)
			}
			return binlog.NewStorageV3ManifestReader(
				ctx, cm, schema, storageConfig, paths[0], tsStart, tsEnd, bufferSize, encryption, nil, 0, validate,
			)
		}
		storageVersion, err := GetStorageVersion(options)
		if err != nil {
			return nil, err
		}
		importEz, _ := GetEZK(options)
		return binlog.NewReader(ctx, cm, schema, storageConfig, storageVersion, paths, tsStart, tsEnd, bufferSize, importEz, nil)
	}

	fileType, err := GetFileType(importFile)
	if err != nil {
		return nil, err
	}
	switch fileType {
	case FileTypeJSON:
		return json.NewReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize)
	case FileTypeJSONLines:
		return json.NewLinesReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize)
	case FileTypeNumpy:
		return numpy.NewReader(ctx, cm, schema, importFile.GetPaths(), bufferSize)
	case FileTypeParquet:
		return parquet.NewReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize)
	case FileTypeCsv:
		sep, err := GetCSVSep(options)
		if err != nil {
			return nil, err
		}
		nullkey, err := GetCSVNullKey(options)
		if err != nil {
			return nil, err
		}
		return csv.NewReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize, sep, nullkey)
	}
	return nil, merr.WrapErrImportFailed("unexpected import file")
}

// resolveSnapshotV2Source reads only the captured per-segment Avro. The top-level
// metadata and live segment catalog are never consulted during worker execution.
func resolveSnapshotV2Source(ctx context.Context, cm storage.ChunkManager, source *internalpb.SnapshotImportSource, options Options) (*datapb.SegmentDescription, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	ref := source.GetV2Reference()
	uri, _ := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotSourceURI, options)
	layout, _ := funcutil.GetAttrByKeyFromRepeatedKV(SnapshotLayout, options)
	boundary := &snapshotstorage.SnapshotData{Layout: datapb.SnapshotLayout_SnapshotLayoutReferenced,
		ManifestPaths: []string{ref.GetSnapshotManifestPath()}}
	if layout == "self-contained" {
		boundary.Layout = datapb.SnapshotLayout_SnapshotLayoutSelfContained
	}
	if err := snapshotstorage.ValidateSnapshotObjectPathForBucket(cm, "snapshot Avro", ref.GetSnapshotManifestPath(), ""); err != nil {
		return nil, err
	}
	if err := snapshotstorage.ValidateExternalSnapshotPaths(uri, boundary, nil); err != nil {
		return nil, err
	}
	segment, err := snapshotstorage.NewSnapshotReader(cm).ReadSegmentManifest(ctx, ref.GetSnapshotManifestPath(), int(ref.GetSnapshotFormatVersion()))
	if err != nil {
		return nil, err
	}
	if segment.GetSegmentId() != ref.GetSegmentId() || segment.GetStorageVersion() != storage.StorageV2 ||
		segment.GetSegmentLevel() == datapb.SegmentLevel_L0 || segment.GetCommitTimestamp() != source.GetSourceCommitTimestamp() {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot Avro does not match its captured V2 source")
	}
	if source.GetSourceChannel() != "" && (segment.GetChannelName() != source.GetSourceChannel() || segment.GetPartitionId() != source.GetSourcePartitionId()) {
		return nil, merr.WrapErrDataIntegrityMsg("snapshot Avro source scope changed")
	}
	// Retain only row and delete inputs, not Avro index/stats metadata.
	segment = &datapb.SegmentDescription{Binlogs: segment.GetBinlogs(), Deltalogs: segment.GetDeltalogs()}
	pathCM := cm
	if ref.OriginalKeyPrefix != nil {
		// A moved bundle can retain original-bucket URIs inside its Avro.
		// Validate their shape now; prepareStorageV2Snapshot validates the
		// rebased paths against the current bucket and bundle before any IO.
		pathCM = nil
	}
	for _, groups := range [][]*datapb.FieldBinlog{segment.Binlogs, segment.Deltalogs} {
		for _, group := range groups {
			for _, log := range group.GetBinlogs() {
				if err := snapshotstorage.ValidateSnapshotObjectPathForBucket(pathCM, "snapshot binlog", log.GetLogPath(), ""); err != nil {
					return nil, err
				}
			}
		}
	}
	if ref.OriginalKeyPrefix != nil {
		root, found := snapshotstorage.DeriveSnapshotRootPath(uri)
		if !found {
			return nil, merr.WrapErrDataIntegrityMsg("cannot derive snapshot destination key prefix")
		}
		if err := snapshotstorage.RebaseSelfContainedSnapshotData(&snapshotstorage.SnapshotData{Segments: []*datapb.SegmentDescription{segment}}, ref.GetOriginalKeyPrefix(), root); err != nil {
			return nil, err
		}
	}
	return segment, nil
}
