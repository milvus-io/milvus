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
	currentEventReader *EventReader
	buffer             *bytes.Buffer
	bufferLength       int
	currentOffset      int32
	isClose            bool
}

func (reader *BinlogReader) NextEventReader() (*EventReader, error) {
	if reader.isClose {
		return nil, errors.New("bin log reader is closed")
	}
	if reader.currentEventReader != nil {
		reader.currentOffset = reader.currentEventReader.NextPosition
		if err := reader.currentEventReader.Close(); err != nil {
			return nil, err
		}
		reader.currentEventReader = nil
	}
	if reader.buffer.Len() <= 0 {
		return nil, nil
	}
	eventReader, err := newEventReader(reader.descriptorEvent.PayloadDataType, reader.buffer)
	if err != nil {
		return nil, err
	}
	reader.currentEventReader = eventReader
	return reader.currentEventReader, nil
}

func (reader *BinlogReader) readMagicNumber() (int32, error) {
	if err := binary.Read(reader.buffer, binary.LittleEndian, &reader.magicNumber); err != nil {
		return -1, err
	}
	reader.currentOffset = 4
	if reader.magicNumber != MagicNumber {
		return -1, errors.New("parse magic number failed, expected: " + strconv.Itoa(MagicNumber) +
			", actual: " + strconv.Itoa(int(reader.magicNumber)))
	}

	return reader.magicNumber, nil
}

func (reader *BinlogReader) readDescriptorEvent() (*descriptorEvent, error) {
	event, err := ReadDescriptorEvent(reader.buffer)
	reader.currentOffset = event.NextPosition
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
	reader.isClose = true
	if reader.currentEventReader != nil {
		if err := reader.currentEventReader.Close(); err != nil {
			return err
		}
	}
	return nil
}

func NewBinlogReader(data []byte) (*BinlogReader, error) {
	reader := &BinlogReader{
		buffer:       bytes.NewBuffer(data),
		bufferLength: len(data),
		isClose:      false,
	}

	if _, err := reader.readMagicNumber(); err != nil {
		return nil, err
	}
	if _, err := reader.readDescriptorEvent(); err != nil {
		return nil, err
	}
	return reader, nil
}
