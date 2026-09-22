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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/internal/util/importutilv2/binlog"
	"github.com/milvus-io/milvus/internal/util/importutilv2/csv"
	"github.com/milvus-io/milvus/internal/util/importutilv2/json"
	"github.com/milvus-io/milvus/internal/util/importutilv2/numpy"
	"github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
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
	bufferSize int,
) (Reader, error) {
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
	return newReader(ctx, resolved.cm, schema, file, f.options, bufferSize, resolved.cfg, resolved.encryption)
}

func NewReader(ctx context.Context,
	cm storage.ChunkManager,
	schema *schemapb.CollectionSchema,
	importFile *internalpb.ImportFile,
	options Options,
	bufferSize int,
	storageConfig *indexpb.StorageConfig,
) (Reader, error) {
	return NewReaderFactory(ctx, cm, storageConfig, options).NewReader(ctx, schema, importFile, bufferSize)
}

func newReader(ctx context.Context, cm storage.ChunkManager, schema *schemapb.CollectionSchema,
	importFile *internalpb.ImportFile, options Options, bufferSize int, storageConfig *indexpb.StorageConfig,
	encryption binlog.SourceEncryption,
) (Reader, error) {
	source := importFile.GetSnapshotSource()
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
				return binlog.NewStorageV3ManifestReader(
					ctx, cm, schema, storageConfig, source.GetManifestPath(), tsStart, tsEnd, bufferSize, encryption, source, validate,
				)
			}
			// DataCoord broadcasts captured metadata, then expands and persists
			// exact StorageV3 manifests during Pending preparation before dispatch.
			// Do not consult storage_version: snapshot metadata is the source of
			// truth and ValidateSnapshotSourceOptions rejects that ambiguity.
			if len(paths) != 1 {
				return nil, merr.WrapErrImportFailedMsg(
					"snapshot-source import file requires exactly one manifest path",
				)
			}
			return binlog.NewStorageV3ManifestReader(
				ctx, cm, schema, storageConfig, paths[0], tsStart, tsEnd, bufferSize, encryption, nil, validate,
			)
		}
		storageVersion, err := GetStorageVersion(options)
		if err != nil {
			return nil, err
		}
		importEz, _ := GetEZK(options)
		return binlog.NewReader(ctx, cm, schema, storageConfig, storageVersion, paths, tsStart, tsEnd, bufferSize, importEz)
	}

	fileType, err := GetFileType(importFile)
	if err != nil {
		return nil, err
	}
	switch fileType {
	case JSON:
		return json.NewReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize)
	case JSONLines:
		return json.NewLinesReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize)
	case Numpy:
		return numpy.NewReader(ctx, cm, schema, importFile.GetPaths(), bufferSize)
	case Parquet:
		return parquet.NewReader(ctx, cm, schema, importFile.GetPaths()[0], bufferSize)
	case CSV:
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
