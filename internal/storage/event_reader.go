package storage

import (
	"bytes"
	"errors"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

type EventReader struct {
	eventHeader
	eventData
	PayloadReaderInterface
	buffer   *bytes.Buffer
	isClosed bool
}

func (reader *EventReader) checkClose() error {
	if reader.isClosed {
		return errors.New("event reader is closed")
	}
	return nil
}

func (reader *EventReader) readHeader() (*eventHeader, error) {
	if err := reader.checkClose(); err != nil {
		return nil, err
	}
	header, err := readEventHeader(reader.buffer)
	if err != nil {
		return nil, err
	}
	reader.eventHeader = *header
	return &reader.eventHeader, nil
}

func (reader *EventReader) readData() (eventData, error) {
	if err := reader.checkClose(); err != nil {
		return nil, err
	}
	var data eventData
	var err error
	switch reader.TypeCode {
	case InsertEventType:
		data, err = readInsertEventDataFixPart(reader.buffer)
	case DeleteEventType:
		data, err = readDeleteEventDataFixPart(reader.buffer)
	case CreateCollectionEventType:
		data, err = readCreateCollectionEventDataFixPart(reader.buffer)
	case DropCollectionEventType:
		data, err = readDropCollectionEventDataFixPart(reader.buffer)
	case CreatePartitionEventType:
		data, err = readCreatePartitionEventDataFixPart(reader.buffer)
	case DropPartitionEventType:
		data, err = readDropPartitionEventDataFixPart(reader.buffer)
	default:
		return nil, errors.New("unknown header type code: " + strconv.Itoa(int(reader.TypeCode)))
	}

	if err != nil {
		return nil, err
	}

	reader.eventData = data
	return reader.eventData, nil
}

func (reader *EventReader) Close() error {
	if !reader.isClosed {
		reader.isClosed = true
		return reader.PayloadReaderInterface.Close()
	}
	return nil
}
func newEventReader(datatype schemapb.DataType, buffer *bytes.Buffer) (*EventReader, error) {
	reader := &EventReader{
		eventHeader: eventHeader{
			baseEventHeader{},
		},
		buffer:   buffer,
		isClosed: false,
	}

	if _, err := reader.readHeader(); err != nil {
		return nil, err
	}

	if _, err := reader.readData(); err != nil {
		return nil, err
	}

	payloadReader, err := NewPayloadReader(datatype, buffer.Bytes())
	if err != nil {
		return nil, err
	}
	reader.PayloadReaderInterface = payloadReader
	return reader, nil
}
