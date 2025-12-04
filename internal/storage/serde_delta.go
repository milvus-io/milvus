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
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"strconv"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// newDeltalogOneFieldReader creates a reader for the old single-field deltalog format
func newDeltalogOneFieldReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	reader := newIterativeCompositeBinlogRecordReader(
		&schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					DataType: schemapb.DataType_VarChar,
				},
			},
		},
		nil,
		MakeBlobsReader(blobs))
	return NewDeserializeReader(reader, func(r Record, v []*DeleteLog) error {
		for i := 0; i < r.Len(); i++ {
			if v[i] == nil {
				v[i] = &DeleteLog{}
			}
			// retrieve the only field
			a := r.(*compositeRecord).recs[0].(*array.String)
			strVal := a.Value(i)
			if err := v[i].Parse(strVal); err != nil {
				return err
			}
		}
		return nil
	}), nil
}

// DeltalogStreamWriter writes deltalog in the old JSON format
type DeltalogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	fieldSchema  *schemapb.FieldSchema

	buf bytes.Buffer
	rw  *singleFieldRecordWriter
}

func (dsw *DeltalogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if dsw.rw != nil {
		return dsw.rw, nil
	}
	rw, err := newSingleFieldRecordWriter(dsw.fieldSchema, &dsw.buf, WithRecordWriterProps(getFieldWriterProps(dsw.fieldSchema)))
	if err != nil {
		return nil, err
	}
	dsw.rw = rw
	return rw, nil
}

func (dsw *DeltalogStreamWriter) Finalize() (*Blob, error) {
	if dsw.rw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	dsw.rw.Close()

	var b bytes.Buffer
	if err := dsw.writeDeltalogHeaders(&b); err != nil {
		return nil, err
	}
	if _, err := b.Write(dsw.buf.Bytes()); err != nil {
		return nil, err
	}
	return &Blob{
		Value:      b.Bytes(),
		RowNum:     int64(dsw.rw.numRows),
		MemorySize: int64(dsw.rw.writtenUncompressed),
	}, nil
}

func (dsw *DeltalogStreamWriter) writeDeltalogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := NewBaseDescriptorEvent(dsw.collectionID, dsw.partitionID, dsw.segmentID)
	de.PayloadDataType = dsw.fieldSchema.DataType
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(int(dsw.rw.writtenUncompressed)))
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(DeleteEventType)
	// Write event data
	ev := newDeleteEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(dsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()
	if err := eh.Write(w); err != nil {
		return err
	}
	if err := ev.WriteEventData(w); err != nil {
		return err
	}
	return nil
}

func newDeltalogStreamWriter(collectionID, partitionID, segmentID UniqueID) *DeltalogStreamWriter {
	return &DeltalogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		fieldSchema: &schemapb.FieldSchema{
			FieldID:  common.RowIDField,
			Name:     "delta",
			DataType: schemapb.DataType_String,
		},
	}
}

func newDeltalogSerializeWriter(eventWriter *DeltalogStreamWriter, batchSize int) (*SerializeWriterImpl[*DeleteLog], error) {
	rws := make(map[FieldID]RecordWriter, 1)
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	rws[0] = rw
	compositeRecordWriter := NewCompositeRecordWriter(rws)
	return NewSerializeRecordWriter(compositeRecordWriter, func(v []*DeleteLog) (Record, error) {
		builder := array.NewBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)

		for _, vv := range v {
			strVal, err := json.Marshal(vv)
			if err != nil {
				return nil, err
			}

			builder.AppendValueFromString(string(strVal))
		}
		arr := []arrow.Array{builder.NewArray()}
		field := []arrow.Field{{
			Name:     "delta",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		}}
		field2Col := map[FieldID]int{
			0: 0,
		}
		return NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(field, nil), arr, int64(len(v))), field2Col), nil
	}, batchSize), nil
}

var _ RecordReader = (*simpleArrowRecordReader)(nil)

// simpleArrowRecordReader reads simple arrow records from blobs
type simpleArrowRecordReader struct {
	blobs []*Blob

	blobPos int
	rr      array.RecordReader
	closer  func()

	r simpleArrowRecord
}

func (crr *simpleArrowRecordReader) iterateNextBatch() error {
	if crr.closer != nil {
		crr.closer()
	}

	crr.blobPos++
	if crr.blobPos >= len(crr.blobs) {
		return io.EOF
	}

	reader, err := NewBinlogReader(crr.blobs[crr.blobPos].Value)
	if err != nil {
		return err
	}

	er, err := reader.NextEventReader()
	if err != nil {
		return err
	}
	rr, err := er.GetArrowRecordReader()
	if err != nil {
		return err
	}
	crr.rr = rr
	crr.closer = func() {
		crr.rr.Release()
		er.Close()
		reader.Close()
	}

	return nil
}

func (crr *simpleArrowRecordReader) Next() (Record, error) {
	if crr.rr == nil {
		if len(crr.blobs) == 0 {
			return nil, io.EOF
		}
		crr.blobPos = -1
		crr.r = simpleArrowRecord{
			field2Col: make(map[FieldID]int),
		}
		if err := crr.iterateNextBatch(); err != nil {
			return nil, err
		}
	}

	composeRecord := func() bool {
		if ok := crr.rr.Next(); !ok {
			return false
		}
		record := crr.rr.Record()
		for i := range record.Schema().Fields() {
			crr.r.field2Col[FieldID(i)] = i
		}
		crr.r.r = record
		return true
	}

	if ok := composeRecord(); !ok {
		if err := crr.iterateNextBatch(); err != nil {
			return nil, err
		}
		if ok := composeRecord(); !ok {
			return nil, io.EOF
		}
	}
	return &crr.r, nil
}

func (crr *simpleArrowRecordReader) SetNeededFields(_ typeutil.Set[int64]) {
	// no-op for simple arrow record reader
}

func (crr *simpleArrowRecordReader) Close() error {
	if crr.closer != nil {
		crr.closer()
	}
	return nil
}

func newSimpleArrowRecordReader(blobs []*Blob) (*simpleArrowRecordReader, error) {
	return &simpleArrowRecordReader{
		blobs: blobs,
	}, nil
}

// MultiFieldDeltalogStreamWriter writes deltalog in the new multi-field parquet format
type MultiFieldDeltalogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	pkType       schemapb.DataType

	buf bytes.Buffer
	rw  *multiFieldRecordWriter
}

func newMultiFieldDeltalogStreamWriter(collectionID, partitionID, segmentID UniqueID, pkType schemapb.DataType) *MultiFieldDeltalogStreamWriter {
	return &MultiFieldDeltalogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		pkType:       pkType,
	}
}

func (dsw *MultiFieldDeltalogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if dsw.rw != nil {
		return dsw.rw, nil
	}

	fieldIDs := []FieldID{common.RowIDField, common.TimeStampField} // Not used.
	fields := []arrow.Field{
		{
			Name:     "pk",
			Type:     serdeMap[dsw.pkType].arrowType(0, schemapb.DataType_None),
			Nullable: false,
		},
		{
			Name:     "ts",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
	}

	rw, err := newMultiFieldRecordWriter(fieldIDs, fields, &dsw.buf)
	if err != nil {
		return nil, err
	}
	dsw.rw = rw
	return rw, nil
}

func (dsw *MultiFieldDeltalogStreamWriter) Finalize() (*Blob, error) {
	if dsw.rw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	dsw.rw.Close()

	var b bytes.Buffer
	if err := dsw.writeDeltalogHeaders(&b); err != nil {
		return nil, err
	}
	if _, err := b.Write(dsw.buf.Bytes()); err != nil {
		return nil, err
	}
	return &Blob{
		Value:      b.Bytes(),
		RowNum:     int64(dsw.rw.numRows),
		MemorySize: int64(dsw.rw.writtenUncompressed),
	}, nil
}

func (dsw *MultiFieldDeltalogStreamWriter) writeDeltalogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := NewBaseDescriptorEvent(dsw.collectionID, dsw.partitionID, dsw.segmentID)
	de.PayloadDataType = schemapb.DataType_Int64
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(int(dsw.rw.writtenUncompressed)))
	de.descriptorEventData.AddExtra(version, MultiField)
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(DeleteEventType)
	// Write event data
	ev := newDeleteEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(dsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()
	if err := eh.Write(w); err != nil {
		return err
	}
	if err := ev.WriteEventData(w); err != nil {
		return err
	}
	return nil
}

func newDeltalogMultiFieldWriter(eventWriter *MultiFieldDeltalogStreamWriter, batchSize int) (*SerializeWriterImpl[*DeleteLog], error) {
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	return NewSerializeRecordWriter[*DeleteLog](rw, func(v []*DeleteLog) (Record, error) {
		fields := []arrow.Field{
			{
				Name:     "pk",
				Type:     serdeMap[schemapb.DataType(v[0].PkType)].arrowType(0, schemapb.DataType_None),
				Nullable: false,
			},
			{
				Name:     "ts",
				Type:     arrow.PrimitiveTypes.Int64,
				Nullable: false,
			},
		}
		arrowSchema := arrow.NewSchema(fields, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()

		pkType := schemapb.DataType(v[0].PkType)
		switch pkType {
		case schemapb.DataType_Int64:
			pb := builder.Field(0).(*array.Int64Builder)
			for _, vv := range v {
				pk := vv.Pk.GetValue().(int64)
				pb.Append(pk)
			}
		case schemapb.DataType_VarChar:
			pb := builder.Field(0).(*array.StringBuilder)
			for _, vv := range v {
				pk := vv.Pk.GetValue().(string)
				pb.Append(pk)
			}
		default:
			return nil, fmt.Errorf("unexpected pk type %v", v[0].PkType)
		}

		for _, vv := range v {
			builder.Field(1).(*array.Int64Builder).Append(int64(vv.Ts))
		}

		arr := []arrow.Array{builder.Field(0).NewArray(), builder.Field(1).NewArray()}

		field2Col := map[FieldID]int{
			common.RowIDField:     0,
			common.TimeStampField: 1,
		}
		return NewSimpleArrowRecord(array.NewRecord(arrowSchema, arr, int64(len(v))), field2Col), nil
	}, batchSize), nil
}

func newDeltalogMultiFieldReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	reader, err := newSimpleArrowRecordReader(blobs)
	if err != nil {
		return nil, err
	}
	return NewDeserializeReader(reader, func(r Record, v []*DeleteLog) error {
		rec, ok := r.(*simpleArrowRecord)
		if !ok {
			return errors.New("can not cast to simple arrow record")
		}
		fields := rec.r.Schema().Fields()
		switch fields[0].Type.ID() {
		case arrow.INT64:
			arr := r.Column(0).(*array.Int64)
			for j := 0; j < r.Len(); j++ {
				if v[j] == nil {
					v[j] = &DeleteLog{}
				}
				v[j].Pk = NewInt64PrimaryKey(arr.Value(j))
			}
		case arrow.STRING:
			arr := r.Column(0).(*array.String)
			for j := 0; j < r.Len(); j++ {
				if v[j] == nil {
					v[j] = &DeleteLog{}
				}
				v[j].Pk = NewVarCharPrimaryKey(arr.Value(j))
			}
		default:
			return fmt.Errorf("unexpected delta log pkType %v", fields[0].Type.Name())
		}

		arr := r.Column(1).(*array.Int64)
		for j := 0; j < r.Len(); j++ {
			v[j].Ts = uint64(arr.Value(j))
		}
		return nil
	}), nil
}

// newDeltalogDeserializeReader is the entry point for the delta log reader.
// It includes newDeltalogOneFieldReader, which uses the existing log format with only one column in a log file,
// and newDeltalogMultiFieldReader, which uses the new format and supports multiple fields in a log file.
func newDeltalogDeserializeReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	if supportMultiFieldFormat(blobs) {
		return newDeltalogMultiFieldReader(blobs)
	}
	return newDeltalogOneFieldReader(blobs)
}

// supportMultiFieldFormat checks delta log description data to see if it is the format with
// pk and ts column separately
func supportMultiFieldFormat(blobs []*Blob) bool {
	if len(blobs) > 0 {
		reader, err := NewBinlogReader(blobs[0].Value)
		if err != nil {
			return false
		}
		defer reader.Close()
		version := reader.descriptorEventData.Extras[version]
		return version != nil && version.(string) == MultiField
	}
	return false
}

// CreateDeltalogReader creates a deltalog reader based on the format version
func CreateDeltalogReader(blobs []*Blob) (*DeserializeReaderImpl[*DeleteLog], error) {
	return newDeltalogDeserializeReader(blobs)
}

// createDeltalogWriter creates a deltalog writer based on the configured format
func createDeltalogWriter(collectionID, partitionID, segmentID UniqueID, pkType schemapb.DataType, batchSize int,
) (*SerializeWriterImpl[*DeleteLog], func() (*Blob, error), error) {
	format := paramtable.Get().DataNodeCfg.DeltalogFormat.GetValue()
	switch format {
	case "json":
		eventWriter := newDeltalogStreamWriter(collectionID, partitionID, segmentID)
		writer, err := newDeltalogSerializeWriter(eventWriter, batchSize)
		return writer, eventWriter.Finalize, err
	case "parquet":
		eventWriter := newMultiFieldDeltalogStreamWriter(collectionID, partitionID, segmentID, pkType)
		writer, err := newDeltalogMultiFieldWriter(eventWriter, batchSize)
		return writer, eventWriter.Finalize, err
	default:
		return nil, nil, merr.WrapErrParameterInvalid("unsupported deltalog format %s", format)
	}
}

type LegacyDeltalogWriter struct {
	path                string
	pkType              schemapb.DataType
	writer              *SerializeWriterImpl[*DeleteLog]
	finalizer           func() (*Blob, error)
	writtenUncompressed uint64

	uploader uploaderFn
}

var _ RecordWriter = (*LegacyDeltalogWriter)(nil)

func NewLegacyDeltalogWriter(
	collectionID, partitionID, segmentID, logID UniqueID, pkType schemapb.DataType, uploader uploaderFn, path string,
) (*LegacyDeltalogWriter, error) {
	writer, finalizer, err := createDeltalogWriter(collectionID, partitionID, segmentID, pkType, 4096)
	if err != nil {
		return nil, err
	}

	return &LegacyDeltalogWriter{
		path:      path,
		pkType:    pkType,
		writer:    writer,
		finalizer: finalizer,
		uploader:  uploader,
	}, nil
}

func (w *LegacyDeltalogWriter) Write(rec Record) error {
	newDeleteLog := func(i int) (*DeleteLog, error) {
		ts := Timestamp(rec.Column(1).(*array.Int64).Value(i))
		switch w.pkType {
		case schemapb.DataType_Int64:
			pk := NewInt64PrimaryKey(rec.Column(0).(*array.Int64).Value(i))
			return NewDeleteLog(pk, ts), nil
		case schemapb.DataType_VarChar:
			pk := NewVarCharPrimaryKey(rec.Column(0).(*array.String).Value(i))
			return NewDeleteLog(pk, ts), nil
		default:
			return nil, fmt.Errorf("unexpected pk type %v", w.pkType)
		}
	}

	for i := range rec.Len() {
		deleteLog, err := newDeleteLog(i)
		if err != nil {
			return err
		}
		err = w.writer.WriteValue(deleteLog)
		if err != nil {
			return err
		}
	}
	w.writtenUncompressed += (rec.Column(0).Data().SizeInBytes() + rec.Column(1).Data().SizeInBytes())
	return nil
}

func (w *LegacyDeltalogWriter) Close() error {
	err := w.writer.Close()
	if err != nil {
		return err
	}
	blob, err := w.finalizer()
	if err != nil {
		return err
	}

	return w.uploader(context.Background(), map[string][]byte{blob.Key: blob.Value})
}

func (w *LegacyDeltalogWriter) GetWrittenUncompressed() uint64 {
	return w.writtenUncompressed
}

func NewLegacyDeltalogReader(pkField *schemapb.FieldSchema, downloader downloaderFn, paths []string) (RecordReader, error) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			pkField,
			{
				FieldID:  common.TimeStampField,
				DataType: schemapb.DataType_Int64,
			},
		},
	}

	chunkPos := 0
	blobsReader := func() ([]*Blob, error) {
		path := paths[chunkPos]
		chunkPos++
		blobs, err := downloader(context.Background(), []string{path})
		if err != nil {
			return nil, err
		}
		return []*Blob{{Key: path, Value: blobs[0]}}, nil
	}

	return newIterativeCompositeBinlogRecordReader(
		schema,
		nil,
		blobsReader,
		nil,
	), nil
}
