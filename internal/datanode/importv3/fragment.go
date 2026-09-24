// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// SourceFromFragment adapts the stable FragmentRef protocol to the generic
// merge source.  The reader validates exact rows and MergeSort validates the
// SortSpec order while consuming the source.
func SourceFromFragment(
	ref *datapb.FragmentRef,
	temporarySchema *schemapb.CollectionSchema,
	bufferSize int64,
	storageConfig *indexpb.StorageConfig,
	pluginContext *indexcgopb.StoragePluginContext,
) (Source, error) {
	if ref == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg("nil import fragment ref")
	}
	if ref.GetPath() == "" || ref.GetRowCount() <= 0 {
		return Source{}, merr.WrapErrImportSysFailedMsg(
			"invalid import fragment ref: path=%q rows=%d", ref.GetPath(), ref.GetRowCount())
	}
	if temporarySchema == nil || storageConfig == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg("import fragment schema or storage config is nil")
	}
	return Source{
		ID:   ref.GetPath(),
		Rows: ref.GetRowCount(),
		Open: func(ctx context.Context) (storage.RecordReader, error) {
			return storage.NewImportFragmentRecordReader(ctx, storage.ImportFragmentReaderSpec{
				Path: ref.GetPath(),
				Rows: ref.GetRowCount(),
			}, temporarySchema,
				storage.WithBufferSize(bufferSize),
				storage.WithStorageConfig(storageConfig),
				storage.WithPluginContext(pluginContext),
			)
		},
	}, nil
}

// SortFields validates that the persisted SortSpec still matches the schema
// and returns the field order storage.Sort/MergeSort consume.
func SortFields(spec *datapb.SortSpec, schema *schemapb.CollectionSchema) ([]int64, error) {
	if spec == nil || len(spec.GetFields()) == 0 || schema == nil {
		return nil, merr.WrapErrImportSysFailedMsg("invalid or missing import SortSpec")
	}
	fields := make(map[int64]*schemapb.FieldSchema)
	for _, field := range schema.GetFields() {
		fields[field.GetFieldID()] = field
	}
	for _, structField := range schema.GetStructArrayFields() {
		for _, field := range structField.GetFields() {
			fields[field.GetFieldID()] = field
		}
	}
	result := make([]int64, 0, len(spec.GetFields()))
	seen := make(map[int64]struct{}, len(spec.GetFields()))
	for _, sortField := range spec.GetFields() {
		field := fields[sortField.GetFieldId()]
		if field == nil {
			return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d does not exist", sortField.GetFieldId())
		}
		if _, ok := seen[field.GetFieldID()]; ok {
			return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d is duplicated", field.GetFieldID())
		}
		seen[field.GetFieldID()] = struct{}{}
		if sortField.GetDataType() != field.GetDataType() {
			return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d has data type %s, schema has %s", field.GetFieldID(), sortField.GetDataType(), field.GetDataType())
		}
		switch field.GetDataType() {
		case schemapb.DataType_Int64, schemapb.DataType_VarChar:
		default:
			return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d has unsupported type %s", field.GetFieldID(), field.GetDataType())
		}
		result = append(result, field.GetFieldID())
	}
	return result, nil
}
