// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
)

// BinlogType is to distinguish different files saving different data.
type BinlogType int32

const (
	InsertBinlog BinlogType = iota
	DeleteBinlog
	DDLBinlog
)
const (
	MagicNumber int32 = 0xfffabc
)

type baseBinlogWriter struct {
	descriptorEvent
	magicNumber  int32
	binlogType   BinlogType
	eventWriters []EventWriter
	buffer       *bytes.Buffer
	length       int32
}

func (writer *baseBinlogWriter) isClosed() bool {
	return writer.buffer != nil
}

func (writer *baseBinlogWriter) GetEventNums() int32 {
	return int32(len(writer.eventWriters))
}

func (writer *baseBinlogWriter) GetRowNums() (int32, error) {
	if writer.isClosed() {
		return writer.length, nil
	}

	length := 0
	for _, e := range writer.eventWriters {
		rows, err := e.GetPayloadLengthFromWriter()
		if err != nil {
			return 0, err
		}
		length += rows
	}
	return int32(length), nil
}

func (writer *baseBinlogWriter) GetBinlogType() BinlogType {
	return writer.binlogType
}

// GetBuffer get binlog buffer. Return nil if binlog is not finished yet.
func (writer *baseBinlogWriter) GetBuffer() ([]byte, error) {
	if writer.buffer == nil {
		return nil, fmt.Errorf("please close binlog before get buffer")
	}
	return writer.buffer.Bytes(), nil
}

// Close allocate buffer and release resource
func (writer *baseBinlogWriter) Close() error {
	if writer.buffer != nil {
		return nil
	}
	if writer.StartTimestamp == 0 || writer.EndTimestamp == 0 {
		return fmt.Errorf("invalid start/end timestamp")
	}

	var offset int32 = 0
	writer.buffer = new(bytes.Buffer)
	if err := binary.Write(writer.buffer, binary.LittleEndian, int32(MagicNumber)); err != nil {
		return err
	}
	offset += int32(binary.Size(MagicNumber))
	if err := writer.descriptorEvent.Write(writer.buffer); err != nil {
		return err
	}
	offset += writer.descriptorEvent.GetMemoryUsageInBytes()

	writer.length = 0
	for _, w := range writer.eventWriters {
		w.SetOffset(offset)
		if err := w.Finish(); err != nil {
			return err
		}
		if err := w.Write(writer.buffer); err != nil {
			return err
		}
		length, err := w.GetMemoryUsageInBytes()
		if err != nil {
			return err
		}
		offset += length
		rows, err := w.GetPayloadLengthFromWriter()
		if err != nil {
			return err
		}
		writer.length += int32(rows)
		if err := w.ReleasePayloadWriter(); err != nil {
			return err
		}
	}
	return nil
}

// InsertBinlogWriter is an object to write binlog file which saves insert data.
type InsertBinlogWriter struct {
	baseBinlogWriter
}

// NextInsertEventWriter returns an event writer to write insert data to an event.
func (writer *InsertBinlogWriter) NextInsertEventWriter() (*insertEventWriter, error) {
	if writer.isClosed() {
		return nil, fmt.Errorf("binlog has closed")
	}
	event, err := newInsertEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

// DeleteBinlogWriter is an object to write binlog file which saves delete data.
type DeleteBinlogWriter struct {
	baseBinlogWriter
}

// NextDeleteEventWriter returns an event writer to write delete data to an event.
func (writer *DeleteBinlogWriter) NextDeleteEventWriter() (*deleteEventWriter, error) {
	if writer.isClosed() {
		return nil, fmt.Errorf("binlog has closed")
	}
	event, err := newDeleteEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

// DDLBinlogWriter is an object to write binlog file which saves ddl information.
type DDLBinlogWriter struct {
	baseBinlogWriter
}

// NextCreateCollectionEventWriter returns an event writer to write CreateCollection
// information to an event.
func (writer *DDLBinlogWriter) NextCreateCollectionEventWriter() (*createCollectionEventWriter, error) {
	if writer.isClosed() {
		return nil, fmt.Errorf("binlog has closed")
	}
	event, err := newCreateCollectionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

// NextDropCollectionEventWriter returns an event writer to write DropCollection
// information to an event.
func (writer *DDLBinlogWriter) NextDropCollectionEventWriter() (*dropCollectionEventWriter, error) {
	if writer.isClosed() {
		return nil, fmt.Errorf("binlog has closed")
	}
	event, err := newDropCollectionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

// NextCreatePartitionEventWriter returns an event writer to write CreatePartition
// information to an event.
func (writer *DDLBinlogWriter) NextCreatePartitionEventWriter() (*createPartitionEventWriter, error) {
	if writer.isClosed() {
		return nil, fmt.Errorf("binlog has closed")
	}
	event, err := newCreatePartitionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

// NextDropPartitionEventWriter returns an event writer to write DropPartition
// information to an event.
func (writer *DDLBinlogWriter) NextDropPartitionEventWriter() (*dropPartitionEventWriter, error) {
	if writer.isClosed() {
		return nil, fmt.Errorf("binlog has closed")
	}
	event, err := newDropPartitionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

// NewInsertBinlogWriter creates InsertBinlogWriter to write binlog file.
func NewInsertBinlogWriter(dataType schemapb.DataType, collectionID, partitionID, segmentID, FieldID int64) *InsertBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.PayloadDataType = dataType
	descriptorEvent.CollectionID = collectionID
	descriptorEvent.PartitionID = partitionID
	descriptorEvent.SegmentID = segmentID
	descriptorEvent.FieldID = FieldID
	return &InsertBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent: *descriptorEvent,
			magicNumber:     MagicNumber,
			binlogType:      InsertBinlog,
			eventWriters:    make([]EventWriter, 0),
			buffer:          nil,
		},
	}
}

// NewDeleteBinlogWriter creates DeleteBinlogWriter to write binlog file.
func NewDeleteBinlogWriter(dataType schemapb.DataType, collectionID int64) *DeleteBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.PayloadDataType = dataType
	descriptorEvent.CollectionID = collectionID
	return &DeleteBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent: *descriptorEvent,
			magicNumber:     MagicNumber,
			binlogType:      DeleteBinlog,
			eventWriters:    make([]EventWriter, 0),
			buffer:          nil,
		},
	}
}

// NewDDLBinlogWriter creates DDLBinlogWriter to write binlog file.
func NewDDLBinlogWriter(dataType schemapb.DataType, collectionID int64) *DDLBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.PayloadDataType = dataType
	descriptorEvent.CollectionID = collectionID
	return &DDLBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent: *descriptorEvent,
			magicNumber:     MagicNumber,
			binlogType:      DDLBinlog,
			eventWriters:    make([]EventWriter, 0),
			buffer:          nil,
		},
	}
}
