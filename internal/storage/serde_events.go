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
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/metautil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ RecordReader = (*compositeBinlogRecordReader)(nil)

type compositeBinlogRecordReader struct {
	blobs [][]*Blob

	blobPos int
	rrs     []array.RecordReader
	closers []func()
	fields  []FieldID

	r compositeRecord
}

func (crr *compositeBinlogRecordReader) iterateNextBatch() error {
	if crr.closers != nil {
		for _, close := range crr.closers {
			if close != nil {
				close()
			}
		}
	}
	crr.blobPos++
	if crr.blobPos >= len(crr.blobs[0]) {
		return io.EOF
	}

	for i, b := range crr.blobs {
		reader, err := NewBinlogReader(b[crr.blobPos].Value)
		if err != nil {
			return err
		}

		crr.fields[i] = reader.FieldID
		// TODO: assert schema being the same in every blobs
		crr.r.schema[reader.FieldID] = reader.PayloadDataType
		er, err := reader.NextEventReader()
		if err != nil {
			return err
		}
		rr, err := er.GetArrowRecordReader()
		if err != nil {
			return err
		}
		crr.rrs[i] = rr
		crr.closers[i] = func() {
			rr.Release()
			er.Close()
			reader.Close()
		}
	}
	return nil
}

func (crr *compositeBinlogRecordReader) Next() error {
	if crr.rrs == nil {
		if crr.blobs == nil || len(crr.blobs) == 0 {
			return io.EOF
		}
		crr.rrs = make([]array.RecordReader, len(crr.blobs))
		crr.closers = make([]func(), len(crr.blobs))
		crr.blobPos = -1
		crr.fields = make([]FieldID, len(crr.rrs))
		crr.r = compositeRecord{
			recs:   make(map[FieldID]arrow.Record, len(crr.rrs)),
			schema: make(map[FieldID]schemapb.DataType, len(crr.rrs)),
		}
		if err := crr.iterateNextBatch(); err != nil {
			return err
		}
	}

	composeRecord := func() bool {
		for i, rr := range crr.rrs {
			if ok := rr.Next(); !ok {
				return false
			}
			// compose record
			crr.r.recs[crr.fields[i]] = rr.Record()
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

func (crr *compositeBinlogRecordReader) Record() Record {
	return &crr.r
}

func (crr *compositeBinlogRecordReader) Close() {
	for _, close := range crr.closers {
		if close != nil {
			close()
		}
	}
}

func parseBlobKey(blobKey string) (colId FieldID, logId UniqueID) {
	if blobKey == "" {
		return 0, 0
	}
	if _, _, _, colId, logId, ok := metautil.ParseInsertLogPath(blobKey); ok {
		return colId, logId
	}
	if colId, err := strconv.ParseInt(blobKey, 10, 64); err == nil {
		// data_codec.go generate single field id as blob key.
		return colId, 0
	}
	return -1, -1
}

func newCompositeBinlogRecordReader(blobs []*Blob) (*compositeBinlogRecordReader, error) {
	sort.Slice(blobs, func(i, j int) bool {
		iCol, iLog := parseBlobKey(blobs[i].Key)
		jCol, jLog := parseBlobKey(blobs[j].Key)

		if iCol == jCol {
			return iLog < jLog
		}
		return iCol < jCol
	})

	blobm := make([][]*Blob, 0)
	var fieldId FieldID = -1
	var currentCol []*Blob

	for _, blob := range blobs {
		colId, _ := parseBlobKey(blob.Key)
		if colId != fieldId {
			if currentCol != nil {
				blobm = append(blobm, currentCol)
			}
			currentCol = make([]*Blob, 0)
			fieldId = colId
		}
		currentCol = append(currentCol, blob)
	}
	if currentCol != nil {
		blobm = append(blobm, currentCol)
	}

	return &compositeBinlogRecordReader{
		blobs: blobm,
	}, nil
}

func NewBinlogDeserializeReader(blobs []*Blob, PKfieldID UniqueID) (*DeserializeReader[*Value], error) {
	reader, err := newCompositeBinlogRecordReader(blobs)
	if err != nil {
		return nil, err
	}

	return NewDeserializeReader(reader, func(r Record, v []*Value) error {
		// Note: the return value `Value` is reused.
		for i := 0; i < r.Len(); i++ {
			value := v[i]
			if value == nil {
				value = &Value{}
				m := make(map[FieldID]interface{}, len(r.Schema()))
				value.Value = m
				v[i] = value
			}

			m := value.Value.(map[FieldID]interface{})
			for j, dt := range r.Schema() {
				if r.Column(j).IsNull(i) {
					m[j] = nil
				} else {
					d, ok := serdeMap[dt].deserialize(r.Column(j), i)
					if ok {
						m[j] = d // TODO: avoid memory copy here.
					} else {
						return errors.New(fmt.Sprintf("unexpected type %s", dt))
					}
				}
			}

			if _, ok := m[common.RowIDField]; !ok {
				panic("no row id column found")
			}
			value.ID = m[common.RowIDField].(int64)
			value.Timestamp = m[common.TimeStampField].(int64)

			pk, err := GenPrimaryKeyByRawData(m[PKfieldID], r.Schema()[PKfieldID])
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

func NewDeltalogDeserializeReader(blobs []*Blob) (*DeserializeReader[*DeleteLog], error) {
	reader, err := newCompositeBinlogRecordReader(blobs)
	if err != nil {
		return nil, err
	}
	return NewDeserializeReader(reader, func(r Record, v []*DeleteLog) error {
		var fid FieldID // The only fid from delete file
		for k := range r.Schema() {
			fid = k
			break
		}
		for i := 0; i < r.Len(); i++ {
			if v[i] == nil {
				v[i] = &DeleteLog{}
			}
			a := r.Column(fid).(*array.String)
			strVal := a.Value(i)
			if err = json.Unmarshal([]byte(strVal), v[i]); err != nil {
				// compatible with versions that only support int64 type primary keys
				// compatible with fmt.Sprintf("%d,%d", pk, ts)
				// compatible error info (unmarshal err invalid character ',' after top-level value)
				splits := strings.Split(strVal, ",")
				if len(splits) != 2 {
					return fmt.Errorf("the format of delta log is incorrect, %v can not be split", strVal)
				}
				pk, err := strconv.ParseInt(splits[0], 10, 64)
				if err != nil {
					return err
				}
				v[i].Pk = &Int64PrimaryKey{
					Value: pk,
				}
				v[i].Ts, err = strconv.ParseUint(splits[1], 10, 64)
				if err != nil {
					return err
				}
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

	memorySize int // To be updated on the fly

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
	}, &bsw.buf)
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
		MemorySize: int64(bsw.memorySize),
	}, nil
}

func (bsw *BinlogStreamWriter) writeBinlogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := newDescriptorEvent()
	de.PayloadDataType = bsw.fieldSchema.DataType
	de.CollectionID = bsw.collectionID
	de.PartitionID = bsw.partitionID
	de.SegmentID = bsw.segmentID
	de.FieldID = bsw.fieldSchema.FieldID
	de.StartTimestamp = 0
	de.EndTimestamp = 0
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(bsw.memorySize))
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
	compositeRecordWriter := newCompositeRecordWriter(rws)
	return NewSerializeRecordWriter[*Value](compositeRecordWriter, func(v []*Value) (Record, uint64, error) {
		builders := make(map[FieldID]array.Builder, len(schema.Fields))
		types := make(map[FieldID]schemapb.DataType, len(schema.Fields))
		for _, f := range schema.Fields {
			dim, _ := typeutil.GetDim(f)
			builders[f.FieldID] = array.NewBuilder(memory.DefaultAllocator, serdeMap[f.DataType].arrowType(int(dim)))
			types[f.FieldID] = f.DataType
		}

		var memorySize uint64
		for _, vv := range v {
			m := vv.Value.(map[FieldID]any)

			for fid, e := range m {
				typeEntry, ok := serdeMap[types[fid]]
				if !ok {
					panic("unknown type")
				}
				ok = typeEntry.serialize(builders[fid], e)
				if !ok {
					return nil, 0, errors.New(fmt.Sprintf("serialize error on type %s", types[fid]))
				}
				eventWriters[fid].memorySize += int(typeEntry.sizeof(e))
				memorySize += typeEntry.sizeof(e)
			}
		}
		arrays := make([]arrow.Array, len(types))
		fields := make([]arrow.Field, len(types))
		field2Col := make(map[FieldID]int, len(types))
		i := 0
		for fid, builder := range builders {
			arrays[i] = builder.NewArray()
			builder.Release()
			fields[i] = arrow.Field{
				Name:     strconv.Itoa(int(fid)),
				Type:     arrays[i].DataType(),
				Nullable: true, // No nullable check here.
			}
			field2Col[fid] = i
			i++
		}
		return newSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrays, int64(len(v))), types, field2Col), memorySize, nil
	}, batchSize), nil
}

type DeltalogStreamWriter struct {
	collectionID UniqueID
	partitionID  UniqueID
	segmentID    UniqueID

	memorySize int // To be updated on the fly
	buf        bytes.Buffer
	rw         *singleFieldRecordWriter
}

func (dsw *DeltalogStreamWriter) GetRecordWriter() (RecordWriter, error) {
	if dsw.rw != nil {
		return dsw.rw, nil
	}

	rw, err := newSingleFieldRecordWriter(0, arrow.Field{
		Name:     "delta",
		Type:     arrow.BinaryTypes.String,
		Nullable: false,
	}, &dsw.buf)
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
		MemorySize: int64(dsw.memorySize),
	}, nil
}

func (dsw *DeltalogStreamWriter) writeDeltalogHeaders(w io.Writer) error {
	// Write magic number
	if err := binary.Write(w, common.Endian, MagicNumber); err != nil {
		return err
	}
	// Write descriptor
	de := newDescriptorEvent()
	de.PayloadDataType = schemapb.DataType_String
	de.CollectionID = dsw.collectionID
	de.PartitionID = dsw.partitionID
	de.SegmentID = dsw.segmentID
	de.StartTimestamp = 0
	de.EndTimestamp = 0
	de.descriptorEventData.AddExtra(originalSizeKey, strconv.Itoa(dsw.memorySize))
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

func NewDeltalogStreamWriter(collectionID, partitionID, segmentID UniqueID) *DeltalogStreamWriter {
	return &DeltalogStreamWriter{
		collectionID: collectionID,
		partitionID:  partitionID,
		segmentID:    segmentID,
	}
}

func NewDeltalogSerializeWriter(partitionID, segmentID UniqueID, eventWriter *DeltalogStreamWriter, batchSize int,
) (*SerializeWriter[*DeleteLog], error) {
	rws := make(map[FieldID]RecordWriter, 1)
	rw, err := eventWriter.GetRecordWriter()
	if err != nil {
		return nil, err
	}
	rws[0] = rw
	compositeRecordWriter := newCompositeRecordWriter(rws)
	return NewSerializeRecordWriter[*DeleteLog](compositeRecordWriter, func(v []*DeleteLog) (Record, uint64, error) {
		builder := array.NewBuilder(memory.DefaultAllocator, arrow.BinaryTypes.String)

		var memorySize uint64
		for _, vv := range v {
			strVal, err := json.Marshal(vv)
			if err != nil {
				return nil, memorySize, err
			}

			builder.AppendValueFromString(string(strVal))
			memorySize += uint64(len(strVal))
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
		schema := map[FieldID]schemapb.DataType{
			0: schemapb.DataType_String,
		}
		return newSimpleArrowRecord(array.NewRecord(arrow.NewSchema(field, nil), arr, int64(len(v))), schema, field2Col), memorySize, nil
	}, batchSize), nil
}
