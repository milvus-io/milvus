package storage

import (
	"bytes"
	"encoding/binary"
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/errors"
)

type BinlogReader struct {
	magicNumber int32
	descriptorEvent
	buffer    *bytes.Buffer
	eventList []*EventReader
	isClose   bool
}

func (reader *BinlogReader) NextEventReader() (*EventReader, error) {
	if reader.isClose {
		return nil, errors.New("bin log reader is closed")
	}
	if reader.buffer.Len() <= 0 {
		return nil, nil
	}
	eventReader, err := newEventReader(reader.descriptorEvent.PayloadDataType, reader.buffer)
	if err != nil {
		return nil, err
	}
	reader.eventList = append(reader.eventList, eventReader)
	return eventReader, nil
}

func (reader *BinlogReader) readMagicNumber() (int32, error) {
	if err := binary.Read(reader.buffer, binary.LittleEndian, &reader.magicNumber); err != nil {
		return -1, err
	}
	if reader.magicNumber != MagicNumber {
		return -1, errors.New("parse magic number failed, expected: " + strconv.Itoa(int(MagicNumber)) +
			", actual: " + strconv.Itoa(int(reader.magicNumber)))
	}

	return reader.magicNumber, nil
}

func (reader *BinlogReader) readDescriptorEvent() (*descriptorEvent, error) {
	event, err := ReadDescriptorEvent(reader.buffer)
	if err != nil {
		return nil, err
	}
	reader.descriptorEvent = *event
	return &reader.descriptorEvent, nil
}

func (reader *BinlogReader) Close() error {
	if reader.isClose {
		return nil
	}
	for _, e := range reader.eventList {
		if err := e.Close(); err != nil {
			return err
		}
	}
	reader.isClose = true
	return nil
}

func NewBinlogReader(data []byte) (*BinlogReader, error) {
	reader := &BinlogReader{
		buffer:    bytes.NewBuffer(data),
		eventList: []*EventReader{},
		isClose:   false,
	}

	if _, err := reader.readMagicNumber(); err != nil {
		return nil, err
	}
	if _, err := reader.readDescriptorEvent(); err != nil {
		return nil, err
	}
	return reader, nil
}
