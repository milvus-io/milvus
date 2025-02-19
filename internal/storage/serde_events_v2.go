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

package storage

import (
	"fmt"
	"io"

	"github.com/apache/arrow/go/v12/arrow"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
)

type packedRecordReader struct {
	reader *packed.PackedReader

	bufferSize int
	path       string
	schema     *schemapb.CollectionSchema
	r          *simpleArrowRecord
	field2Col  map[FieldID]int
}

func (pr *packedRecordReader) Next() error {
	if pr.reader == nil {
		return io.EOF
	}
	rec, err := pr.reader.ReadNext()
	if err != nil || rec == nil {
		return io.EOF
	}
	pr.r = NewSimpleArrowRecord(rec, pr.field2Col)
	return nil
}

func (pr *packedRecordReader) Record() Record {
	return pr.r
}

func (pr *packedRecordReader) Close() error {
	if pr.reader != nil {
		return pr.reader.Close()
	}
	return nil
}

func NewPackedRecordReader(path string, schema *schemapb.CollectionSchema, bufferSize int,
) (*packedRecordReader, error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("convert collection schema [%s] to arrow schema error: %s", schema.Name, err.Error())
	}
	reader, err := packed.NewPackedReader(path, arrowSchema, bufferSize)
	if err != nil {
		return nil, merr.WrapErrParameterInvalid("New binlog record packed reader error: %s", err.Error())
	}
	field2Col := make(map[FieldID]int)
	for i, field := range schema.Fields {
		field2Col[field.FieldID] = i
	}
	return &packedRecordReader{
		reader:     reader,
		schema:     schema,
		bufferSize: bufferSize,
		path:       path,
		field2Col:  field2Col,
	}, nil
}

func NewPackedDeserializeReader(path string, schema *schemapb.CollectionSchema,
	bufferSize int, pkFieldID FieldID,
) (*DeserializeReader[*Value], error) {
	reader, err := NewPackedRecordReader(path, schema, bufferSize)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		rec, ok := r.(*simpleArrowRecord)
		if !ok {
			return merr.WrapErrServiceInternal("can not cast to simple arrow record")
		}

		schema := reader.schema
		numFields := len(schema.Fields)
		for i := 0; i < rec.Len(); i++ {
			if v[i] == nil {
				v[i] = &Value{
					Value: make(map[FieldID]interface{}, numFields),
				}
			}
			value := v[i]
			m := value.Value.(map[FieldID]interface{})
			for _, field := range schema.Fields {
				fieldID := field.FieldID
				column := r.Column(fieldID)
				if column.IsNull(i) {
					m[fieldID] = nil
				} else {
					d, ok := serdeMap[field.DataType].deserialize(column, i)
					if ok {
						m[fieldID] = d
					} else {
						return merr.WrapErrServiceInternal(fmt.Sprintf("can not deserialize field [%s]", field.Name))
					}
				}
			}

			rowID, ok := m[common.RowIDField].(int64)
			if !ok {
				return merr.WrapErrIoKeyNotFound("no row id column found")
			}
			value.ID = rowID
			value.Timestamp = m[common.TimeStampField].(int64)

			pkCol := rec.field2Col[pkFieldID]
			pk, err := GenPrimaryKeyByRawData(m[pkFieldID], schema.Fields[pkCol].DataType)
			if err != nil {
				return err
			}

			value.PK = pk
			value.IsDeleted = false
			value.Value = m
		}
		return nil
	}), nil
}

var _ RecordWriter = (*packedRecordWriter)(nil)

type packedRecordWriter struct {
	writer *packed.PackedWriter

	bufferSize int
	path       string
	schema     *arrow.Schema

	numRows             int
	writtenUncompressed uint64
}

func (pw *packedRecordWriter) Write(r Record) error {
	rec, ok := r.(*simpleArrowRecord)
	if !ok {
		return merr.WrapErrServiceInternal("can not cast to simple arrow record")
	}
	pw.numRows += r.Len()
	for _, arr := range rec.r.Columns() {
		pw.writtenUncompressed += uint64(calculateArraySize(arr))
	}
	defer rec.Release()
	return pw.writer.WriteRecordBatch(rec.r)
}

func (pw *packedRecordWriter) GetWrittenUncompressed() uint64 {
	return pw.writtenUncompressed
}

func (pw *packedRecordWriter) Close() error {
	if pw.writer != nil {
		return pw.writer.Close()
	}
	return nil
}

func NewPackedRecordWriter(path string, schema *arrow.Schema, bufferSize int) (*packedRecordWriter, error) {
	writer, err := packed.NewPackedWriter(path, schema, bufferSize)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	return &packedRecordWriter{
		writer:     writer,
		schema:     schema,
		bufferSize: bufferSize,
		path:       path,
	}, nil
}

func NewPackedSerializeWriter(schema *schemapb.CollectionSchema, partitionID, segmentID UniqueID,
	batchSize int, path string, bufferSize int,
) (*SerializeWriter[*Value], error) {
	arrowSchema, err := ConvertToArrowSchema(schema.Fields)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not convert collection schema %s to arrow schema: %s", schema.Name, err.Error()))
	}
	packedRecordWriter, err := NewPackedRecordWriter(path, arrowSchema, bufferSize)
	if err != nil {
		return nil, merr.WrapErrServiceInternal(
			fmt.Sprintf("can not new packed record writer %s", err.Error()))
	}
	return NewSerializeRecordWriter[*Value](packedRecordWriter, func(v []*Value) (Record, error) {
		return ValueSerializer(v, schema.Fields)
	}, batchSize), nil
}
