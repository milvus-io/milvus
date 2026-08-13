// Licensed to the LF AI & Data foundation under one or more contributor
// license agreements. See the NOTICE file distributed with this work for
// additional information regarding copyright ownership.
// The ASF licenses this file to you under the Apache License, Version 2.0.

package importv3

import (
	"context"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
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
	if ref.GetSourceTaskId() == 0 || ref.GetPath() == "" || ref.GetRows() <= 0 {
		return Source{}, merr.WrapErrImportSysFailedMsg(
			"invalid import fragment ref: source_task=%d path=%q rows=%d",
			ref.GetSourceTaskId(), ref.GetPath(), ref.GetRows())
	}
	if ref.GetStartRow() < 0 || ref.GetEndRow()-ref.GetStartRow() != ref.GetRows() {
		return Source{}, merr.WrapErrImportSysFailedMsg(
			"import fragment range does not match rows: path=%q start=%d end=%d rows=%d",
			ref.GetPath(), ref.GetStartRow(), ref.GetEndRow(), ref.GetRows())
	}
	if temporarySchema == nil || storageConfig == nil {
		return Source{}, merr.WrapErrImportSysFailedMsg("import fragment schema or storage config is nil")
	}
	return Source{
		ID:   ref.GetPath(),
		Rows: ref.GetRows(),
		Open: func(ctx context.Context) (storage.RecordReader, error) {
			return storage.NewImportFragmentRecordReader(ctx, storage.ImportFragmentReaderSpec{
				Path:     ref.GetPath(),
				Format:   ref.GetFormat(),
				StartRow: ref.GetStartRow(),
				EndRow:   ref.GetEndRow(),
				Rows:     ref.GetRows(),
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
	if spec == nil || spec.GetFormatVersion() == 0 || len(spec.GetFields()) == 0 || schema == nil {
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
		switch sortField.GetKeyType() {
		case datapb.SortKeyType_SORT_KEY_TYPE_INT64:
			if field.GetDataType() != schemapb.DataType_Int64 {
				return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d is not int64", field.GetFieldID())
			}
		case datapb.SortKeyType_SORT_KEY_TYPE_STRING:
			if field.GetDataType() != schemapb.DataType_VarChar {
				return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d is not varchar", field.GetFieldID())
			}
		default:
			return nil, merr.WrapErrImportSysFailedMsg("SortSpec field %d has unspecified key type", field.GetFieldID())
		}
		result = append(result, field.GetFieldID())
	}
	return result, nil
}

// NewTTLOnlyPredicate creates the ordinary-import final predicate.  It never
// reads collection deltalogs and always gives EntityFilter an empty delete
// map.  A fresh time.Now value is captured once per input Record batch, not
// persisted in the task/snapshot/digest, so retries are free to physically
// remove rows that have expired in the meantime.
func NewTTLOnlyPredicate(
	schema *schemapb.CollectionSchema,
	collectionTTL int64,
	dataTS uint64,
) func(storage.Record, int, int) bool {
	return newTTLOnlyPredicate(schema, collectionTTL, dataTS, time.Now)
}

func newTTLOnlyPredicate(
	schema *schemapb.CollectionSchema,
	collectionTTL int64,
	dataTS uint64,
	now func() time.Time,
) func(storage.Record, int, int) bool {
	ttlFieldID := ttlField(schema)
	hasTimestamp := false
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if field.GetFieldID() == common.TimeStampField {
			hasTimestamp = true
			break
		}
	}
	type batchState struct {
		record storage.Record
		filter compaction.EntityFilter
	}
	batches := make(map[int]batchState)
	return func(record storage.Record, readerIndex, row int) bool {
		state := batches[readerIndex]
		if state.record != record {
			state = batchState{
				record: record,
				filter: compaction.NewEntityFilter(nil, collectionTTL, now(), 0),
			}
			batches[readerIndex] = state
		}
		expirationMicros := int64(-1)
		if ttlFieldID >= common.StartOfUserFieldID {
			if column, ok := record.Column(ttlFieldID).(*array.Int64); ok && column.IsValid(row) {
				expirationMicros = column.Value(row)
			}
		}
		rowTS := dataTS
		if hasTimestamp {
			if column, ok := record.Column(common.TimeStampField).(*array.Int64); ok && column.IsValid(row) {
				rowTS = uint64(column.Value(row))
			}
		}
		// PK is irrelevant because the delete map is deliberately empty.
		return !state.filter.Filtered(nil, rowTS, expirationMicros)
	}
}

func ttlField(schema *schemapb.CollectionSchema) int64 {
	if schema == nil {
		return -1
	}
	name := ""
	for _, property := range schema.GetProperties() {
		if property.GetKey() == common.CollectionTTLFieldKey {
			name = property.GetValue()
			break
		}
	}
	if name == "" {
		return -1
	}
	for _, field := range schema.GetFields() {
		if field.GetName() == name && field.GetDataType() == schemapb.DataType_Timestamptz {
			return field.GetFieldID()
		}
	}
	return -1
}
