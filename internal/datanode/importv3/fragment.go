// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	importv3pb "github.com/milvus-io/milvus/pkg/v3/proto/importv3pb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SourceFromFragment adapts the stable FragmentRef protocol to the generic
// merge source.  The reader validates exact rows and MergeSort validates the
// sort order while consuming the source.
func SourceFromFragment(
	ref *importv3pb.FragmentRef,
	temporarySchema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	pluginContext *indexcgopb.StoragePluginContext,
) (Source, error) {
	if ref == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg("nil import fragment ref")
	}
	if ref.GetPath() == "" || ref.GetRows() <= 0 {
		return Source{}, merr.WrapErrImportSysFailedMsg(
			"invalid import fragment ref: path=%q rows=%d", ref.GetPath(), ref.GetRows())
	}
	if temporarySchema == nil || storageConfig == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg("import fragment schema or storage config is nil")
	}
	return Source{
		ID:   ref.GetPath(),
		Rows: ref.GetRows(),
		Open: func(ctx context.Context) (storage.RecordReader, error) {
			return storage.NewImportFragmentRecordReader(ctx, storage.ImportFragmentReaderSpec{
				Path: ref.GetPath(),
				Rows: ref.GetRows(),
			}, temporarySchema,
				storage.WithBufferSize(bufferSize),
				storage.WithStorageConfig(storageConfig),
				storage.WithPluginContext(pluginContext),
			)
		},
	}, nil
}
