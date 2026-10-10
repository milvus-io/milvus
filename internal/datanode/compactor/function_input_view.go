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
	"path"
	"strconv"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type textLOBDecoder interface {
	Decode(context.Context, *array.Binary) (*array.String, error)
	Close() error
}

type functionInputPreparer struct {
	requiredTextIDs []int64
	fields          map[int64]*schemapb.FieldSchema
	existingFields  map[int64]struct{}
	sourceManifest  string
	storageConfig   *indexpb.StorageConfig
	decoders        map[int64]textLOBDecoder
	newDecoder      func(int64, string, *indexpb.StorageConfig) (textLOBDecoder, error)
}

func newFunctionInputPreparer(
	schema *schemapb.CollectionSchema,
	missingFunctions []*schemapb.FunctionSchema,
	existingFields map[int64]struct{},
	sourceManifest string,
	storageConfig *indexpb.StorageConfig,
) (*functionInputPreparer, error) {
	p := &functionInputPreparer{
		fields:         make(map[int64]*schemapb.FieldSchema),
		existingFields: existingFields,
		sourceManifest: sourceManifest,
		storageConfig:  storageConfig,
		decoders:       make(map[int64]textLOBDecoder),
		newDecoder: func(fieldID int64, lobBase string, cfg *indexpb.StorageConfig) (textLOBDecoder, error) {
			return packed.NewTextLOBDecoder(fieldID, lobBase, cfg)
		},
	}
	for _, fn := range missingFunctions {
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

func (p *functionInputPreparer) decoderFor(fieldID int64) (textLOBDecoder, error) {
	if d, ok := p.decoders[fieldID]; ok {
		return d, nil
	}
	segmentBase, _, err := packed.UnmarshalManifestPath(p.sourceManifest)
	if err != nil {
		return nil, merr.WrapErrDataIntegrity(err, "invalid source manifest path for TEXT field %d", fieldID)
	}
	if segmentBase == "" {
		return nil, merr.WrapErrDataIntegrityMsg("empty source manifest base for TEXT field %d", fieldID)
	}
	lobBase := path.Join(path.Dir(segmentBase), "lobs", strconv.FormatInt(fieldID, 10))
	d, err := p.newDecoder(fieldID, lobBase, p.storageConfig)
	if err != nil {
		return nil, merr.Wrapf(err, "open TEXT LOB decoder for field %d", fieldID)
	}
	if d == nil {
		return nil, merr.WrapErrServiceInternalMsg("nil TEXT LOB decoder for field %d", fieldID)
	}
	p.decoders[fieldID] = d
	return d, nil
}

// Prepare borrows writeBase and returns a logical view for synchronous
// materialization. cleanup releases only arrays created by this call.
func (p *functionInputPreparer) Prepare(ctx context.Context, writeBase storage.Record) (storage.Record, func(), error) {
	noop := func() {}
	if len(p.requiredTextIDs) == 0 {
		return writeBase, noop, nil
	}
	columns := make(map[int64]arrow.Array)
	var cleanupOnce sync.Once
	cleanup := func() {
		cleanupOnce.Do(func() { releaseArrowArrays(columns) })
	}
	for _, fieldID := range p.requiredTextIDs {
		col := writeBase.Column(fieldID)
		_, physicallyPresent := p.existingFields[fieldID]
		if !physicallyPresent {
			if _, alreadyString := col.(*array.String); alreadyString {
				continue
			}
			if !p.fields[fieldID].GetNullable() {
				cleanup()
				return nil, noop, merr.WrapErrDataIntegrityMsg("absent TEXT input field %d has no String default", fieldID)
			}
			if col != nil && col.NullN() != writeBase.Len() {
				cleanup()
				return nil, noop, merr.WrapErrDataIntegrityMsg("absent TEXT input field %d has non-null physical values", fieldID)
			}
			builder := array.NewStringBuilder(memory.DefaultAllocator)
			builder.AppendNulls(writeBase.Len())
			columns[fieldID] = builder.NewStringArray()
			builder.Release()
			continue
		}
		switch values := col.(type) {
		case *array.String:
			continue
		case *array.Binary:
			decoder, err := p.decoderFor(fieldID)
			if err != nil {
				cleanup()
				return nil, noop, err
			}
			strings, err := decoder.Decode(ctx, values)
			if err != nil {
				cleanup()
				return nil, noop, merr.Wrapf(err, "decode TEXT input field %d", fieldID)
			}
			if strings == nil {
				cleanup()
				return nil, noop, merr.WrapErrDataIntegrityMsg("nil decoded TEXT input field %d", fieldID)
			}
			if strings.Len() != writeBase.Len() {
				strings.Release()
				cleanup()
				return nil, noop, merr.WrapErrDataIntegrityMsg("decoded TEXT input field %d row count mismatch", fieldID)
			}
			columns[fieldID] = strings
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

func (p *functionInputPreparer) Close() error {
	var firstErr error
	for fieldID, d := range p.decoders {
		if err := d.Close(); err != nil && firstErr == nil {
			firstErr = merr.Wrapf(err, "close TEXT LOB decoder for field %d", fieldID)
		}
		delete(p.decoders, fieldID)
	}
	return firstErr
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
	logicalInputs, cleanupInputs, err := preparer.Prepare(ctx, writeBase)
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
