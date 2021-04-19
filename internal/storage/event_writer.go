package storage

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type EventTypeCode int8

const (
	DescriptorEventType EventTypeCode = iota
	InsertEventType
	DeleteEventType
	CreateCollectionEventType
	DropCollectionEventType
	CreatePartitionEventType
	DropPartitionEventType
	EventTypeEnd
)

func (code EventTypeCode) String() string {
	codes := []string{"DescriptorEventType", "InsertEventType", "DeleteEventType", "CreateCollectionEventType", "DropCollectionEventType",
		"CreatePartitionEventType", "DropPartitionEventType"}
	if len(codes) < int(code) {
		return ""
	}
	return codes[code]
}

type descriptorEvent struct {
	descriptorEventHeader
	descriptorEventData
}

func (event *descriptorEvent) GetMemoryUsageInBytes() int32 {
	return event.descriptorEventHeader.GetMemoryUsageInBytes() + event.descriptorEventData.GetMemoryUsageInBytes()
}

func (event *descriptorEvent) Write(buffer io.Writer) error {
	if err := event.descriptorEventHeader.Write(buffer); err != nil {
		return err
	}
	if err := event.descriptorEventData.Write(buffer); err != nil {
		return err
	}
	return nil
}

func ReadDescriptorEvent(buffer io.Reader) (*descriptorEvent, error) {
	header, err := readDescriptorEventHeader(buffer)
	if err != nil {
		return nil, err
	}
	data, err := readDescriptorEventData(buffer)
	if err != nil {
		return nil, err
	}
	return &descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}, nil
}

type EventWriter interface {
	PayloadWriterInterface
	// Finish set meta in header and no data can be added to event writer
	Finish() error
	// Close release resources
	Close() error
	// Write serialize to buffer, should call Finish first
	Write(buffer *bytes.Buffer) error
	GetMemoryUsageInBytes() (int32, error)
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
	return writer.getEventDataSize() + writer.eventHeader.GetMemoryUsageInBytes() +
		int32(len(data)), nil
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
	if err := binary.Write(buffer, binary.LittleEndian, data); err != nil {
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

func (writer *baseEventWriter) Close() error {
	if !writer.isClosed {
		writer.isFinish = true
		writer.isClosed = true
		if err := writer.ReleasePayloadWriter(); err != nil {
			return err
		}
	}
	return nil
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

func newDescriptorEvent() (*descriptorEvent, error) {
	header, err := newDescriptorEventHeader()
	if err != nil {
		return nil, err
	}
	data, err := newDescriptorEventData()
	if err != nil {
		return nil, err
	}
	header.EventLength = header.GetMemoryUsageInBytes() + data.GetMemoryUsageInBytes()
	header.NextPosition = int32(binary.Size(MagicNumber)) + header.EventLength
	data.HeaderLength = int8(binary.Size(eventHeader{}))
	return &descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}, err
}

func newInsertEventWriter(dataType schemapb.DataType, offset int32) (*insertEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header, err := newEventHeader(InsertEventType)
	if err != nil {
		return nil, err
	}
	data, err := newInsertEventData()
	if err != nil {
		return nil, err
	}

	writer := &insertEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			offset:                 offset,
		},
		insertEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.insertEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.insertEventData.WriteEventData
	return writer, nil
}

func newDeleteEventWriter(dataType schemapb.DataType, offset int32) (*deleteEventWriter, error) {
	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header, err := newEventHeader(DeleteEventType)
	if err != nil {
		return nil, err
	}
	data, err := newDeleteEventData()
	if err != nil {
		return nil, err
	}
	writer := &deleteEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			offset:                 offset,
		},
		deleteEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.deleteEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.deleteEventData.WriteEventData
	return writer, nil
}
func newCreateCollectionEventWriter(dataType schemapb.DataType, offset int32) (*createCollectionEventWriter, error) {
	if dataType != schemapb.DataType_STRING && dataType != schemapb.DataType_INT64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header, err := newEventHeader(CreateCollectionEventType)
	if err != nil {
		return nil, err
	}
	data, err := newCreateCollectionEventData()
	if err != nil {
		return nil, err
	}

	writer := &createCollectionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			offset:                 offset,
		},
		createCollectionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.createCollectionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.createCollectionEventData.WriteEventData
	return writer, nil
}
func newDropCollectionEventWriter(dataType schemapb.DataType, offset int32) (*dropCollectionEventWriter, error) {
	if dataType != schemapb.DataType_STRING && dataType != schemapb.DataType_INT64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header, err := newEventHeader(DropCollectionEventType)
	if err != nil {
		return nil, err
	}
	data, err := newDropCollectionEventData()
	if err != nil {
		return nil, err
	}
	writer := &dropCollectionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			offset:                 offset,
		},
		dropCollectionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.dropCollectionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.dropCollectionEventData.WriteEventData
	return writer, nil
}
func newCreatePartitionEventWriter(dataType schemapb.DataType, offset int32) (*createPartitionEventWriter, error) {
	if dataType != schemapb.DataType_STRING && dataType != schemapb.DataType_INT64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header, err := newEventHeader(CreatePartitionEventType)
	if err != nil {
		return nil, err
	}
	data, err := newCreatePartitionEventData()
	if err != nil {
		return nil, err
	}

	writer := &createPartitionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			offset:                 offset,
		},
		createPartitionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.createPartitionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.createPartitionEventData.WriteEventData
	return writer, nil
}
func newDropPartitionEventWriter(dataType schemapb.DataType, offset int32) (*dropPartitionEventWriter, error) {
	if dataType != schemapb.DataType_STRING && dataType != schemapb.DataType_INT64 {
		return nil, errors.New("incorrect data type")
	}

	payloadWriter, err := NewPayloadWriter(dataType)
	if err != nil {
		return nil, err
	}
	header, err := newEventHeader(DropPartitionEventType)
	if err != nil {
		return nil, err
	}
	data, err := newDropPartitionEventData()
	if err != nil {
		return nil, err
	}
	writer := &dropPartitionEventWriter{
		baseEventWriter: baseEventWriter{
			eventHeader:            *header,
			PayloadWriterInterface: payloadWriter,
			isClosed:               false,
			isFinish:               false,
			offset:                 offset,
		},
		dropPartitionEventData: *data,
	}
	writer.baseEventWriter.getEventDataSize = writer.dropPartitionEventData.GetEventDataFixPartSize
	writer.baseEventWriter.writeEventData = writer.dropPartitionEventData.WriteEventData
	return writer, nil
}
