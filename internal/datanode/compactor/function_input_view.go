// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package compactor

import (
	"context"
	"io"
	"path"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type functionInputPreparer struct {
	schema          *schemapb.CollectionSchema
	requiredTextIDs []int64
	fields          map[int64]*schemapb.FieldSchema
	existingFields  map[int64]struct{}
	sourceManifest  string
	storageConfig   *indexpb.StorageConfig
	sideTextIDs     []int64
	sideConfigured  bool
	reader          storage.RecordReader
	current         storage.Record
	currentOffset   int
	newReader       func(context.Context) (storage.RecordReader, error)
}

func newFunctionInputPreparer(
	schema *schemapb.CollectionSchema,
	missingFunctions []*schemapb.FunctionSchema,
	existingFields map[int64]struct{},
	sourceManifest string,
	storageConfig *indexpb.StorageConfig,
) (*functionInputPreparer, error) {
	p := &functionInputPreparer{
		schema:         schema,
		fields:         make(map[int64]*schemapb.FieldSchema),
		existingFields: existingFields,
		sourceManifest: sourceManifest,
		storageConfig:  storageConfig,
	}
	p.newReader = p.openReader
	for _, fn := range missingFunctions {
		outputIndexes, err := functionOutputIndexesToMaterialize(fn, existingFields)
		if err != nil {
			return nil, err
		}
		if len(outputIndexes) == 0 {
			continue
		}
		for _, fieldID := range fn.GetInputFieldIds() {
			field := typeutil.GetField(schema, fieldID)
			if field == nil {
				return nil, merr.WrapErrDataIntegrityMsg("function %s input field %d not found", fn.GetName(), fieldID)
			}
			if field.GetDataType() != schemapb.DataType_Text {
				continue
			}
			if _, seen := p.fields[fieldID]; seen {
				continue
			}
			p.fields[fieldID] = field
			p.requiredTextIDs = append(p.requiredTextIDs, fieldID)
		}
	}
	return p, nil
}

func (p *functionInputPreparer) sourceTextConfigs(fieldIDs []int64) ([]packed.TextColumnConfig, error) {
	segmentBase, _, err := packed.UnmarshalManifestPath(p.sourceManifest)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid source manifest path for TEXT input")
	}
	if segmentBase == "" {
		return nil, merr.WrapErrDataIntegrityMsg("empty source manifest base for TEXT input")
	}
	configs := make([]packed.TextColumnConfig, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		configs = append(configs, packed.TextColumnConfig{
			FieldID:     fieldID,
			LobBasePath: path.Join(path.Dir(segmentBase), "lobs", strconv.FormatInt(fieldID, 10)),
		})
	}
	return configs, nil
}

func (p *functionInputPreparer) presentTextIDs() []int64 {
	ids := make([]int64, 0, len(p.requiredTextIDs))
	for _, fieldID := range p.requiredTextIDs {
		if _, ok := p.existingFields[fieldID]; ok {
			ids = append(ids, fieldID)
		}
	}
	return ids
}

func (p *functionInputPreparer) openReader(ctx context.Context) (storage.RecordReader, error) {
	configs, err := p.sourceTextConfigs(p.sideTextIDs)
	if err != nil {
		return nil, err
	}
	readSchema := proto.Clone(p.schema).(*schemapb.CollectionSchema)
	readSchema.Fields = make([]*schemapb.FieldSchema, 0, len(p.sideTextIDs))
	readSchema.StructArrayFields = nil
	for _, fieldID := range p.sideTextIDs {
		readSchema.Fields = append(readSchema.Fields, p.fields[fieldID])
	}
	reader, err := storage.NewTextDecodedManifestRecordReader(ctx, p.sourceManifest, readSchema, configs,
		storage.WithPresentFields(p.existingFields),
		storage.WithVersion(storage.StorageV3),
		storage.WithStorageConfig(p.storageConfig))
	if err != nil {
		return nil, merr.Wrap(err, "open TEXT input reader")
	}
	return reader, nil
}

func (p *functionInputPreparer) configureSideReader(raw storage.Record) error {
	if p.sideConfigured {
		return nil
	}
	for _, fieldID := range p.requiredTextIDs {
		if _, physicallyPresent := p.existingFields[fieldID]; !physicallyPresent {
			continue
		}
		switch raw.Column(fieldID).(type) {
		case *array.String:
		case *array.Binary:
			p.sideTextIDs = append(p.sideTextIDs, fieldID)
		default:
			return merr.WrapErrDataIntegrityMsg("TEXT input field %d has unexpected Arrow representation %T", fieldID, raw.Column(fieldID))
		}
	}
	p.sideConfigured = true
	return nil
}

// readRows aligns two projections of the same manifest by physical row ordinal,
// not by their independent record-batch boundaries. Returned arrays are owned
// by the caller and remain valid after the secondary reader advances.
func (p *functionInputPreparer) readRows(ctx context.Context, raw storage.Record) (map[int64]arrow.Array, error) {
	if err := p.configureSideReader(raw); err != nil {
		return nil, err
	}
	if len(p.sideTextIDs) == 0 {
		return nil, nil
	}
	if p.reader == nil {
		reader, err := p.newReader(ctx)
		if err != nil {
			return nil, err
		}
		if reader == nil {
			return nil, merr.WrapErrServiceInternalMsg("nil TEXT input reader")
		}
		p.reader = reader
	}
	parts := make(map[int64][]arrow.Array, len(p.sideTextIDs))
	defer func() {
		for _, slices := range parts {
			for _, slice := range slices {
				slice.Release()
			}
		}
	}()
	remaining := raw.Len()
	for remaining > 0 {
		if p.current == nil || p.currentOffset == p.current.Len() {
			var err error
			p.current, err = p.reader.Next()
			if err == io.EOF {
				return nil, merr.WrapErrDataIntegrityMsg("TEXT input reader ended before physical record")
			}
			if err != nil {
				return nil, merr.Wrap(err, "read TEXT input record")
			}
			if p.current == nil || p.current.Len() == 0 {
				return nil, merr.WrapErrDataIntegrityMsg("TEXT input reader returned empty record")
			}
			p.currentOffset = 0
		}
		count := min(remaining, p.current.Len()-p.currentOffset)
		for _, fieldID := range p.sideTextIDs {
			values, ok := p.current.Column(fieldID).(*array.String)
			if !ok {
				return nil, merr.WrapErrDataIntegrityMsg("TEXT input field %d has unexpected decoded representation %T", fieldID, p.current.Column(fieldID))
			}
			parts[fieldID] = append(parts[fieldID], array.NewSlice(values, int64(p.currentOffset), int64(p.currentOffset+count)))
		}
		p.currentOffset += count
		remaining -= count
	}
	columns := make(map[int64]arrow.Array, len(p.sideTextIDs))
	for _, fieldID := range p.sideTextIDs {
		var combined arrow.Array
		if len(parts[fieldID]) == 1 {
			combined = parts[fieldID][0]
			combined.Retain()
		} else {
			var err error
			combined, err = array.Concatenate(parts[fieldID], memory.DefaultAllocator)
			if err != nil {
				releaseArrowArrays(columns)
				return nil, merr.WrapErrDataIntegrity(err, "join decoded TEXT input field %d", fieldID)
			}
		}
		columns[fieldID] = combined
	}
	return columns, nil
}

// Prepare borrows writeBase and returns a logical view for synchronous
// materialization. cleanup releases only arrays created by this call.
func (p *functionInputPreparer) Prepare(ctx context.Context, writeBase storage.Record) (storage.Record, func(), error) {
	return p.prepare(ctx, writeBase, writeBase, nil)
}

func (p *functionInputPreparer) prepare(ctx context.Context, raw, writeBase storage.Record, selection *recordSelection) (storage.Record, func(), error) {
	noop := func() {}
	if len(p.requiredTextIDs) == 0 {
		return writeBase, noop, nil
	}
	decoded, err := p.readRows(ctx, raw)
	if err != nil {
		return nil, noop, err
	}
	defer releaseArrowArrays(decoded)
	columns := make(map[int64]arrow.Array)
	var cleanupOnce sync.Once
	cleanup := func() { cleanupOnce.Do(func() { releaseArrowArrays(columns) }) }
	for _, fieldID := range p.requiredTextIDs {
		col := writeBase.Column(fieldID)
		if _, physicallyPresent := p.existingFields[fieldID]; !physicallyPresent {
			if _, alreadyString := col.(*array.String); alreadyString {
				continue
			}
			if !p.fields[fieldID].GetNullable() || (col != nil && col.NullN() != writeBase.Len()) {
				cleanup()
				return nil, noop, merr.WrapErrDataIntegrityMsg("absent TEXT input field %d cannot be filled with String nulls", fieldID)
			}
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			builder.AppendNulls(writeBase.Len())
			columns[fieldID] = builder.NewStringArray()
			builder.Release()
			continue
		}
		switch col.(type) {
		case *array.String:
			continue
		case *array.Binary:
			values := decoded[fieldID]
			if values == nil {
				cleanup()
				return nil, noop, merr.WrapErrDataIntegrityMsg("decoded TEXT input field %d missing", fieldID)
			}
			if selection == nil {
				values.Retain()
				columns[fieldID] = values
			} else {
				view := &functionInputView{base: raw, columns: decoded}
				selected, selectErr := buildSelectedColumn(view, p.fields[fieldID], selection)
				if selectErr != nil {
					cleanup()
					return nil, noop, selectErr
				}
				columns[fieldID] = selected
			}
		default:
			cleanup()
			return nil, noop, merr.WrapErrDataIntegrityMsg("TEXT input field %d has unexpected Arrow representation %T", fieldID, col)
		}
	}
	if len(columns) == 0 {
		return writeBase, noop, nil
	}
	return &functionInputView{base: writeBase, columns: columns}, cleanup, nil
}

func (p *functionInputPreparer) Skip(ctx context.Context, raw storage.Record) error {
	decoded, err := p.readRows(ctx, raw)
	releaseArrowArrays(decoded)
	return err
}

func (p *functionInputPreparer) Finish() error {
	if p.reader == nil {
		return nil
	}
	if p.current != nil && p.currentOffset < p.current.Len() {
		return merr.WrapErrDataIntegrityMsg("TEXT input reader has trailing physical rows")
	}
	record, err := p.reader.Next()
	if err == io.EOF {
		return nil
	}
	if err != nil {
		return merr.Wrap(err, "finish TEXT input reader")
	}
	if record != nil {
		return merr.WrapErrDataIntegrityMsg("TEXT input reader has trailing physical rows")
	}
	return nil
}

func (p *functionInputPreparer) Close() error {
	if p.reader != nil {
		err := p.reader.Close()
		p.reader = nil
		p.current = nil
		if err != nil {
			return merr.Wrap(err, "close TEXT input reader")
		}
	}
	return nil
}

type functionInputView struct {
	base    storage.Record
	columns map[int64]arrow.Array
}

var _ storage.Record = (*functionInputView)(nil)

func (v *functionInputView) Column(fieldID storage.FieldID) arrow.Array {
	if col, ok := v.columns[fieldID]; ok {
		return col
	}
	return v.base.Column(fieldID)
}

func (v *functionInputView) Len() int { return v.base.Len() }

func (v *functionInputView) Retain() {
	v.base.Retain()
	for _, col := range v.columns {
		col.Retain()
	}
}

func (v *functionInputView) Release() {
	v.base.Release()
	for _, col := range v.columns {
		col.Release()
	}
}

// materializePreparedRecord follows the existing selection order, but supplies
// decoded strings only to function runners. The returned record still writes
// the original physical columns from writeBase.
func materializePreparedRecord(ctx context.Context, record storage.Record, selection *recordSelection,
	materializer *RecordMaterializer, preparer *functionInputPreparer,
) (storage.Record, error) {
	writeBase := record
	if selection != nil {
		selected, err := newSelectedRecord(record, materializer.schema, materializer.pendingOutputs, selection)
		if err != nil {
			return nil, err
		}
		writeBase = selected
	}
	logicalInputs, cleanupInputs, err := preparer.prepare(ctx, record, writeBase, selection)
	if err != nil {
		cleanupMaterializedRecord(writeBase)
		return nil, err
	}
	wrapped, err := materializer.WrapWithInputs(writeBase, logicalInputs)
	cleanupInputs()
	if err != nil {
		cleanupMaterializedRecord(writeBase)
		return nil, err
	}
	return wrapped, nil
}
