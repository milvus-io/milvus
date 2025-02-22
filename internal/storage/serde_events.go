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
	"encoding/binary"
	"fmt"
	"io"
	"sort"
	"strconv"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ RecordReader = (*CompositeBinlogRecordReader)(nil)

// ChunkedBlobsReader returns a chunk composed of blobs, or io.EOF if no more data
type ChunkedBlobsReader func() ([]*Blob, error)

type CompositeBinlogRecordReader struct {
	BlobsReader ChunkedBlobsReader

	brs []*BinlogReader
	rrs []array.RecordReader

	schema *schemapb.CollectionSchema
	index  map[FieldID]int16
	r      *compositeRecord
}

func (crr *CompositeBinlogRecordReader) iterateNextBatch() error {
	if crr.brs != nil {
		for _, er := range crr.brs {
			er.Close()
		}
		for _, rr := range crr.rrs {
			rr.Release()
		}
	}

	blobs, err := crr.BlobsReader()
	if err != nil {
		return err
	}

	if crr.rrs == nil {
		crr.rrs = make([]array.RecordReader, len(blobs))
		crr.brs = make([]*BinlogReader, len(blobs))
		crr.index = make(map[FieldID]int16, len(blobs))
	}

	for i, b := range blobs {
		reader, err := NewBinlogReader(b.Value)
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
		crr.rrs[i] = rr
		crr.index[reader.FieldID] = int16(i)
		crr.brs[i] = reader
	}
	return nil
}

func (crr *CompositeBinlogRecordReader) Next() error {
	if crr.rrs == nil {
		if err := crr.iterateNextBatch(); err != nil {
			return err
		}
	}

	composeRecord := func() bool {
		recs := make([]arrow.Array, len(crr.rrs))
		for i, rr := range crr.rrs {
			if ok := rr.Next(); !ok {
				return false
			}
			recs[i] = rr.Record().Column(0)
		}
		crr.r = &compositeRecord{
			index: crr.index,
			recs:  recs,
		}
		return true
	}

	// Try compose records
	if ok := composeRecord(); !ok {
		// If failed the first time, try iterate next batch (blob), the error may be io.EOF
		if err := crr.iterateNextBatch(); err != nil {
			return err
		}
		// If iterate next batch success, try compose again
		if ok := composeRecord(); !ok {
			// If the next blob is empty, return io.EOF (it's rare).
			return io.EOF
		}
	}
	return nil
}

func (crr *CompositeBinlogRecordReader) Record() Record {
	return crr.r
}

func (crr *CompositeBinlogRecordReader) Close() error {
	if crr.brs != nil {
		for _, er := range crr.brs {
			if er != nil {
				er.Close()
			}
		}
	}
	if crr.rrs != nil {
		for _, rr := range crr.rrs {
			if rr != nil {
				rr.Release()
			}
		}
	}
	crr.r = nil
	return nil
}

func parseBlobKey(blobKey string) (colId FieldID, logId UniqueID) {
	if _, _, _, colId, logId, ok := metautil.ParseInsertLogPath(blobKey); ok {
		return colId, logId
	}
	if colId, err := strconv.ParseInt(blobKey, 10, 64); err == nil {
		// data_codec.go generate single field id as blob key.
		return colId, 0
	}
	return InvalidUniqueID, InvalidUniqueID
}

func MakeBlobsReader(blobs []*Blob) ChunkedBlobsReader {
	blobMap := make(map[FieldID][]*Blob)
	for _, blob := range blobs {
		colId, _ := parseBlobKey(blob.Key)
		if _, exists := blobMap[colId]; !exists {
			blobMap[colId] = []*Blob{blob}
		} else {
			blobMap[colId] = append(blobMap[colId], blob)
		}
	}
	sortedBlobs := make([][]*Blob, 0, len(blobMap))
	for _, blobsForField := range blobMap {
		sort.Slice(blobsForField, func(i, j int) bool {
			_, iLog := parseBlobKey(blobsForField[i].Key)
			_, jLog := parseBlobKey(blobsForField[j].Key)

			return iLog < jLog
		})
		sortedBlobs = append(sortedBlobs, blobsForField)
	}
	chunkPos := 0
	return func() ([]*Blob, error) {
		if len(sortedBlobs) == 0 || chunkPos >= len(sortedBlobs[0]) {
			return nil, io.EOF
		}
		blobs := make([]*Blob, len(sortedBlobs))
		for fieldPos := range blobs {
			blobs[fieldPos] = sortedBlobs[fieldPos][chunkPos]
		}
		chunkPos++
		return blobs, nil
	}
}

func NewCompositeBinlogRecordReader(schema *schemapb.CollectionSchema, blobsReader ChunkedBlobsReader) (*CompositeBinlogRecordReader, error) {
	return &CompositeBinlogRecordReader{
		schema:      schema,
		BlobsReader: blobsReader,
	}, nil
}

func ValueDeserializer(r Record, v []*Value, fieldSchema []*schemapb.FieldSchema) error {
	pkField := func() *schemapb.FieldSchema {
		for _, field := range fieldSchema {
			if field.GetIsPrimaryKey() {
				return field
			}
		}
		return nil
	}()
	if pkField == nil {
		return merr.WrapErrServiceInternal("no primary key field found")
	}

	for i := 0; i < r.Len(); i++ {
		value := v[i]
		if value == nil {
			value = &Value{}
			value.Value = make(map[FieldID]interface{}, len(fieldSchema))
			v[i] = value
		}

		m := value.Value.(map[FieldID]interface{})
		for _, f := range fieldSchema {
			j := f.FieldID
			dt := f.DataType
			if r.Column(j) == nil {
				if f.GetDefaultValue() != nil {
					m[j] = getDefaultValue(f)
				} else {
					m[j] = nil
				}
				continue
			}
			if r.Column(j).IsNull(i) {
				m[j] = nil
			} else {
				d, ok := serdeMap[dt].deserialize(r.Column(j), i)
				if ok {
					m[j] = d // TODO: avoid memory copy here.
				} else {
					return merr.WrapErrServiceInternal(fmt.Sprintf("unexpected type %s", dt))
				}
			}
		}

		rowID, ok := m[common.RowIDField].(int64)
		if !ok {
			return merr.WrapErrIoKeyNotFound("no row id column found")
		}
		value.ID = rowID
		value.Timestamp = m[common.TimeStampField].(int64)

		pk, err := GenPrimaryKeyByRawData(m[pkField.FieldID], pkField.DataType)
		if err != nil {
			return err
		}

		value.PK = pk
		value.IsDeleted = false
		value.Value = m
	}
	return nil
}

func NewBinlogDeserializeReader(schema *schemapb.CollectionSchema, blobsReader ChunkedBlobsReader) (*DeserializeReader[*Value], error) {
	reader, err := NewCompositeBinlogRecordReader(schema, blobsReader)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		return ValueDeserializer(r, v, schema.Fields)
	}), nil
}

func newDeltalogOneFieldReader(blobs []*Blob) (*DeserializeReader[*DeleteLog], error) {
	reader, err := NewCompositeBinlogRecordReader(nil, MakeBlobsReader(blobs))
	if err != nil {
		return nil, err
	}
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

type BinlogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	fieldSchema  *schemapb.FieldSchema

	buf bytes.Buffer
	rw  *singleFieldRecordWriter
}

func (bsw *BinlogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if bsw.rw != nil {
		return bsw.rw, nil
	}

	fid := bsw.fieldSchema.FieldID
	dim, _ := typeutil.GetDim(bsw.fieldSchema)
	rw, err := newSingleFieldRecordWriter(fid, arrow.Field{
		Name:     strconv.Itoa(int(fid)),
		Type:     serdeMap[bsw.fieldSchema.DataType].arrowType(int(dim)),
		Nullable: true, // No nullable check here.
	}, &bsw.buf, WithRecordWriterProps(getFieldWriterProps(bsw.fieldSchema)))
	if err != nil {
		return nil, err
	}
	bsw.rw = rw
	return rw, nil
}

func (bsw *BinlogStreamWriter) Finalize() (*Blob, error) {
	if bsw.rw == nil {
		return nil, io.ErrUnexpectedEOF
	}
	bsw.rw.Close()

	var b bytes.Buffer
	if err := bsw.writeBinlogHeaders(&b); err != nil {
		return nil, err
	}
	if _, err := b.Write(bsw.buf.Bytes()); err != nil {
		return nil, err
	}
	return &Blob{
		Key:        strconv.Itoa(int(bsw.fieldSchema.FieldID)),
		Value:      b.Bytes(),
		RowNum:     int64(bsw.rw.numRows),
		MemorySize: int64(bsw.rw.writtenUncompressed),
	}, nil
}

func (bsw *BinlogStreamWriter) writeBinlogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := NewBaseDescriptorEvent(bsw.collectionID, bsw.partitionID, bsw.segmentID)
	de.PayloadDataType = bsw.fieldSchema.DataType
	de.FieldID = bsw.fieldSchema.FieldID
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(int(bsw.rw.writtenUncompressed)))
	de.descriptorEventData.AddExtra(nullableKey, bsw.fieldSchema.Nullable)
	if err := de.Write(w); err != nil {
		return err
	}
	// Write event header
	eh := newEventHeader(InsertEventType)
	// Write event data
	ev := newInsertEventData()
	ev.StartTimestamp = 1
	ev.EndTimestamp = 1
	eh.EventLength = int32(bsw.buf.Len()) + eh.GetMemoryUsageInBytes() + int32(binary.Size(ev))
	// eh.NextPosition = eh.EventLength + w.Offset()
	if err := eh.Write(w); err != nil {
		return err
	}
	if err := ev.WriteEventData(w); err != nil {
		return err
	}
	return nil
}

func NewBinlogStreamWriters(collectionID, partitionID, segmentID UniqueID,
	schema []*schemapb.FieldSchema,
) map[FieldID]*BinlogStreamWriter {
	bws := make(map[FieldID]*BinlogStreamWriter, len(schema))
	for _, f := range schema {
		bws[f.FieldID] = &BinlogStreamWriter{
			collectionID: collectionID,
			partitionID:  partitionID,
			segmentID:    segmentID,
			fieldSchema:  f,
		}
	}
	return bws
}

func ValueSerializer(v []*Value, fieldSchema []*schemapb.FieldSchema) (Record, error) {
	builders := make(map[FieldID]array.Builder, len(fieldSchema))
	types := make(map[FieldID]schemapb.DataType, len(fieldSchema))
	for _, f := range fieldSchema {
		dim, _ := typeutil.GetDim(f)
		builders[f.FieldID] = array.NewBuilder(memory.DefaultAllocator, serdeMap[f.DataType].arrowType(int(dim)))
		types[f.FieldID] = f.DataType
	}

	for _, vv := range v {
		m := vv.Value.(map[FieldID]any)

		for fid, e := range m {
			typeEntry, ok := serdeMap[types[fid]]
			if !ok {
				panic("unknown type")
			}
			ok = typeEntry.serialize(builders[fid], e)
			if !ok {
				return nil, merr.WrapErrServiceInternal(fmt.Sprintf("serialize error on type %s", types[fid]))
			}
		}
	}
	arrays := make([]arrow.Array, len(fieldSchema))
	fields := make([]arrow.Field, len(fieldSchema))
	field2Col := make(map[FieldID]int, len(fieldSchema))
	for i, field := range fieldSchema {
		builder := builders[field.FieldID]
		arrays[i] = builder.NewArray()
		builder.Release()
		fields[i] = arrow.Field{
			Name:     field.Name,
			Type:     arrays[i].DataType(),
			Metadata: arrow.NewMetadata([]string{"FieldID"}, []string{strconv.Itoa(int(field.FieldID))}),
		}
		field2Col[field.FieldID] = i
	}
	return NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(len(v))), field2Col), nil
}

func NewBinlogSerializeWriter(schema *schemapb.CollectionSchema, partitionID, segmentID UniqueID,
	eventWriters map[FieldID]*BinlogStreamWriter, batchSize int,
) (*SerializeWriter[*Value], error) {
	rws := make(map[FieldID]RecordWriter, len(eventWriters))
	for fid := range eventWriters {
		w := eventWriters[fid]
		rw, err := w.GetRecordWriter()
		if err != nil {
			return nil, err
		}
		rws[fid] = rw
	}
	compositeRecordWriter := NewCompositeRecordWriter(rws)
	return NewSerializeRecordWriter[*Value](compositeRecordWriter, func(v []*Value) (Record, error) {
		return ValueSerializer(v, schema.Fields)
	}, batchSize), nil
}

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
	dim, _ := typeutil.GetDim(dsw.fieldSchema)
	rw, err := newSingleFieldRecordWriter(dsw.fieldSchema.FieldID, arrow.Field{
		Name:     dsw.fieldSchema.Name,
		Type:     serdeMap[dsw.fieldSchema.DataType].arrowType(int(dim)),
		Nullable: false,
	}, &dsw.buf, WithRecordWriterProps(getFieldWriterProps(dsw.fieldSchema)))
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

func newDeltalogSerializeWriter(eventWriter *DeltalogStreamWriter, batchSize int) (*SerializeWriter[*DeleteLog], error) {
	rws := make(map[FieldID]RecordWriter, 1)
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	rws[0] = rw
	compositeRecordWriter := NewCompositeRecordWriter(rws)
	return NewSerializeRecordWriter[*DeleteLog](compositeRecordWriter, func(v []*DeleteLog) (Record, error) {
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

func (crr *simpleArrowRecordReader) Next() error {
	if crr.rr == nil {
		if len(crr.blobs) == 0 {
			return io.EOF
		}
		crr.blobPos = -1
		crr.r = simpleArrowRecord{
			field2Col: make(map[FieldID]int),
		}
		if err := crr.iterateNextBatch(); err != nil {
			return err
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
			return err
		}
		if ok := composeRecord(); !ok {
			return io.EOF
		}
	}
	return nil
}

func (crr *simpleArrowRecordReader) Record() Record {
	return &crr.r
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

func newMultiFieldDeltalogStreamWriter(collectionID, partitionID, segmentID UniqueID, pkType schemapb.DataType) *MultiFieldDeltalogStreamWriter {
	return &MultiFieldDeltalogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
		pkType:       pkType,
	}
}

type MultiFieldDeltalogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID
	pkType       schemapb.DataType

	buf bytes.Buffer
	rw  *multiFieldRecordWriter
}

func (dsw *MultiFieldDeltalogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if dsw.rw != nil {
		return dsw.rw, nil
	}

	fieldIds := []FieldID{common.RowIDField, common.TimeStampField} // Not used.
	fields := []arrow.Field{
		{
			Name:     "pk",
			Type:     serdeMap[dsw.pkType].arrowType(0),
			Nullable: false,
		},
		{
			Name:     "ts",
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
		},
	}

	rw, err := newMultiFieldRecordWriter(fieldIds, fields, &dsw.buf)
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

func newDeltalogMultiFieldWriter(eventWriter *MultiFieldDeltalogStreamWriter, batchSize int) (*SerializeWriter[*DeleteLog], error) {
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	return NewSerializeRecordWriter[*DeleteLog](rw, func(v []*DeleteLog) (Record, error) {
		fields := []arrow.Field{
			{
				Name:     "pk",
				Type:     serdeMap[schemapb.DataType(v[0].PkType)].arrowType(0),
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

func newDeltalogMultiFieldReader(blobs []*Blob) (*DeserializeReader[*DeleteLog], error) {
	reader, err := newSimpleArrowRecordReader(blobs)
	if err != nil {
		return nil, err
	}
	return NewDeserializeReader(reader, func(r Record, v []*DeleteLog) error {
		rec, ok := r.(*simpleArrowRecord)
		if !ok {
			return fmt.Errorf("can not cast to simple arrow record")
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

// NewDeltalogDeserializeReader is the entry point for the delta log reader.
// It includes NewDeltalogOneFieldReader, which uses the existing log format with only one column in a log file,
// and NewDeltalogMultiFieldReader, which uses the new format and supports multiple fields in a log file.
func newDeltalogDeserializeReader(blobs []*Blob) (*DeserializeReader[*DeleteLog], error) {
	if supportMultiFieldFormat(blobs) {
		return newDeltalogMultiFieldReader(blobs)
	}
	return newDeltalogOneFieldReader(blobs)
}

// check delta log description data to see if it is the format with
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

func CreateDeltalogReader(blobs []*Blob) (*DeserializeReader[*DeleteLog], error) {
	return newDeltalogDeserializeReader(blobs)
}

func CreateDeltalogWriter(collectionID, partitionID, segmentID UniqueID, pkType schemapb.DataType, batchSize int) (*SerializeWriter[*DeleteLog], func() (*Blob, error), error) {
	format := paramtable.Get().DataNodeCfg.DeltalogFormat.GetValue()
	if format == "json" {
		eventWriter := newDeltalogStreamWriter(collectionID, partitionID, segmentID)
		writer, err := newDeltalogSerializeWriter(eventWriter, batchSize)
		return writer, eventWriter.Finalize, err
	} else if format == "parquet" {
		eventWriter := newMultiFieldDeltalogStreamWriter(collectionID, partitionID, segmentID, pkType)
		writer, err := newDeltalogMultiFieldWriter(eventWriter, batchSize)
		return writer, eventWriter.Finalize, err
	}
	return nil, nil, merr.WrapErrParameterInvalid("unsupported deltalog format %s", format)
}
