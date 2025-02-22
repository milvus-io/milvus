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
	"io"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

// EventTypeCode represents event type by code
type EventTypeCode int8

// EventTypeCode definitions
const (
	DescriptorEventType EventTypeCode = iota
	InsertEventType
	DeleteEventType
	CreateCollectionEventType
	DropCollectionEventType
	CreatePartitionEventType
	DropPartitionEventType
	IndexFileEventType
	EventTypeEnd
)

// String returns the string representation
func (code EventTypeCode) String() string {
	codes := map[EventTypeCode]string{
		DescriptorEventType:       "DescriptorEventType",
		InsertEventType:           "InsertEventType",
		DeleteEventType:           "DeleteEventType",
		CreateCollectionEventType: "CreateCollectionEventType",
		DropCollectionEventType:   "DropCollectionEventType",
		CreatePartitionEventType:  "CreatePartitionEventType",
		DropPartitionEventType:    "DropPartitionEventType",
		IndexFileEventType:        "IndexFileEventType",
	}
	if eventTypeStr, ok := codes[code]; ok {
		return eventTypeStr
	}
	return "InvalidEventType"
}

type descriptorEvent struct {
	descriptorEventHeader
	descriptorEventData
}

// GetMemoryUsageInBytes returns descriptor Event memory usage in bytes
func (event *descriptorEvent) GetMemoryUsageInBytes() int32 {
	return event.descriptorEventHeader.GetMemoryUsageInBytes() + event.descriptorEventData.GetMemoryUsageInBytes()
}

// Write writes descriptor event into buffer
func (event *descriptorEvent) Write(buffer io.Writer) error {
	err := event.descriptorEventData.FinishExtra()
	if err != nil {
		return err
	}
	event.descriptorEventHeader.EventLength = event.descriptorEventHeader.GetMemoryUsageInBytes() + event.descriptorEventData.GetMemoryUsageInBytes()
	event.descriptorEventHeader.NextPosition = int32(binary.Size(MagicNumber)) + event.descriptorEventHeader.EventLength

	if err := event.descriptorEventHeader.Write(buffer); err != nil {
		return err
	}
	if err := event.descriptorEventData.Write(buffer); err != nil {
		return err
	}
	return nil
}

// EventWriter abstracts event writer
type EventWriter interface {
	PayloadWriterInterface
	// Finish set meta in header and no data can be added to event writer
	Finish() error
	// Close release resources
	Close()
	// Write serialize to buffer, should call Finish first
	Write(buffer *bytes.Buffer) error
	GetMemoryUsageInBytes() (int32, error)
	SetOffset(offset int32)
}

type baseEventWriter struct {
	eventHeader
	PayloadWriterInterface
	isClosed         bool
	isFinish         bool
	offset           int32
	getEventDataSize func() int32
	writeEventData   func(buffer io.Writer) error
}

func (writer *baseEventWriter) GetMemoryUsageInBytes() (int32, error) {
	data, err := writer.GetPayloadBufferFromWriter()
	if err != nil {
		return -1, err
	}
	size := writer.getEventDataSize() + writer.eventHeader.GetMemoryUsageInBytes() + int32(len(data))
	return size, nil
}

func (writer *baseEventWriter) Write(buffer *bytes.Buffer) error {
	if err := writer.eventHeader.Write(buffer); err != nil {
		return err
	}
	if err := writer.writeEventData(buffer); err != nil {
		return err
	}
	data, err := writer.GetPayloadBufferFromWriter()
	if err != nil {
		return err
	}
	if err := binary.Write(buffer, common.Endian, data); err != nil {
		return err
	}
	return nil
}

func (writer *baseEventWriter) Finish() error {
	if !writer.isFinish {
		writer.isFinish = true
		if err := writer.FinishPayloadWriter(); err != nil {
			return err
		}
		eventLength, err := writer.GetMemoryUsageInBytes()
		if err != nil {
			return err
		}
		writer.EventLength = eventLength
		writer.NextPosition = eventLength + writer.offset
	}
	return nil
}

func (writer *baseEventWriter) Close() {
	if !writer.isClosed {
		writer.isFinish = true
		writer.isClosed = true
		writer.ReleasePayloadWriter()
	}
}

func (writer *baseEventWriter) SetOffset(offset int32) {
	writer.offset = offset
}

type insertEventWriter struct {
	baseEventWriter
	insertEventData
}

type deleteEventWriter struct {
	baseEventWriter
	deleteEventData
}

type createCollectionEventWriter struct {
	baseEventWriter
	createCollectionEventData
}

type dropCollectionEventWriter struct {
	baseEventWriter
	dropCollectionEventData
}

type createPartitionEventWriter struct {
	baseEventWriter
	createPartitionEventData
}

type dropPartitionEventWriter struct {
	baseEventWriter
	dropPartitionEventData
}

type indexFileEventWriter struct {
	baseEventWriter
	indexFileEventData
}

func newDescriptorEvent() *descriptorEvent {
	header := newDescriptorEventHeader()
	data := newDescriptorEventData()
	return &descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}
}

func NewBaseDescriptorEvent(collectionID int64, partitionID int64, segmentID int64) *descriptorEvent {
	de := newDescriptorEvent()
	de.CollectionID = collectionID
	de.PartitionID = partitionID
	de.SegmentID = segmentID
	de.StartTimestamp = 0
	de.EndTimestamp = 0
	return de
}

func newInsertEventWriter(dataType schemapb.DataType, opts ...PayloadWriterOptions) (*insertEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType, opts...)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(InsertEventType)
	data := newInsertEventData()

	writer := &insertEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		insertEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.insertEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.insertEventData.WriteEventData
	return writer, nil
}

func newDeleteEventWriter(dataType schemapb.DataType, opts ...PayloadWriterOptions) (*deleteEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType, opts...)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(DeleteEventType)
	data := newDeleteEventData()

	writer := &deleteEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		deleteEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.deleteEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.deleteEventData.WriteEventData
	return writer, nil
}

func newCreateCollectionEventWriter(dataType schemapb.DataType) (*createCollectionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(CreateCollectionEventType)
	data := newCreateCollectionEventData()

	writer := &createCollectionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		createCollectionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.createCollectionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.createCollectionEventData.WriteEventData
	return writer, nil
}

func newDropCollectionEventWriter(dataType schemapb.DataType) (*dropCollectionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(DropCollectionEventType)
	data := newDropCollectionEventData()

	writer := &dropCollectionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		dropCollectionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.dropCollectionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.dropCollectionEventData.WriteEventData
	return writer, nil
}

func newCreatePartitionEventWriter(dataType schemapb.DataType) (*createPartitionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(CreatePartitionEventType)
	data := newCreatePartitionEventData()

	writer := &createPartitionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		createPartitionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.createPartitionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.createPartitionEventData.WriteEventData
	return writer, nil
}

func newDropPartitionEventWriter(dataType schemapb.DataType) (*dropPartitionEventWriter, error) {
	if dataType != schemapb.DataType_String && dataType != schemapb.DataType_Int64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(DropPartitionEventType)
	data := newDropPartitionEventData()

	writer := &dropPartitionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		dropPartitionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.dropPartitionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.dropPartitionEventData.WriteEventData
	return writer, nil
}

func newIndexFileEventWriter(dataType schemapb.DataType) (*indexFileEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header := newEventHeader(IndexFileEventType)
	data := newIndexFileEventData()

	writer := &indexFileEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
		},
		indexFileEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.indexFileEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.indexFileEventData.WriteEventData

	return writer, nil
}
