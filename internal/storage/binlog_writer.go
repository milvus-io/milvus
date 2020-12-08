package storage

import (
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

const (
	// todo : put to param table
	ServerID      = 1
	BinlogVersion = 1
	CommitID      = 1
	ServerVersion = 1
	HeaderLength  = 17
)

type BinlogType int32

const (
	InsertBinlog BinlogType = iota
	DeleteBinlog
	DDLBinlog
)
const (
	MagicNumber = 0xfffabc
)

type baseBinlogWriter struct {
	descriptorEvent
	magicNumber        int32
	binlogType         BinlogType
	eventWriters       []EventWriter
	currentEventWriter EventWriter
	buffer             *bytes.Buffer
	numEvents          int32
	numRows            int32
	isClose            bool
	offset             int32
}

func (writer *baseBinlogWriter) checkClose() error {
	if writer.isClose {
		return errors.New("insert binlog writer is already closed")
	}
	return nil
}

func (writer *baseBinlogWriter) appendEventWriter() error {
	if writer.currentEventWriter != nil {
		if err := writer.currentEventWriter.Finish(); err != nil {
			return err
		}

		writer.eventWriters = append(writer.eventWriters, writer.currentEventWriter)
		length, err := writer.currentEventWriter.GetMemoryUsageInBytes()
		if err != nil {
			return err
		}
		writer.offset += length
		writer.numEvents++
		nums, err := writer.currentEventWriter.GetPayloadLengthFromWriter()
		if err != nil {
			return err
		}
		writer.numRows += int32(nums)
		writer.currentEventWriter = nil
	}
	return nil
}

func (writer *baseBinlogWriter) GetEventNums() int32 {
	return writer.numEvents
}

func (writer *baseBinlogWriter) GetRowNums() (int32, error) {
	var res = writer.numRows
	if writer.currentEventWriter != nil {
		nums, err := writer.currentEventWriter.GetPayloadLengthFromWriter()
		if err != nil {
		}
		res += int32(nums)
	}
	return res, nil
}

func (writer *baseBinlogWriter) GetBinlogType() BinlogType {
	return writer.binlogType
}

// GetBuffer get binlog buffer. Return nil if binlog is not finished yet.
func (writer *baseBinlogWriter) GetBuffer() []byte {
	if writer.buffer != nil {
		return writer.buffer.Bytes()
	}
	return nil
}

// Close allocate buffer and release resource
func (writer *baseBinlogWriter) Close() error {
	if writer.isClose {
		return nil
	}
	writer.isClose = true
	if err := writer.appendEventWriter(); err != nil {
		return err
	}
	writer.buffer = new(bytes.Buffer)
	if err := binary.Write(writer.buffer, binary.LittleEndian, int32(MagicNumber)); err != nil {
		return err
	}
	if err := writer.descriptorEvent.Write(writer.buffer); err != nil {
		return err
	}
	for _, w := range writer.eventWriters {
		if err := w.Write(writer.buffer); err != nil {
			return err
		}
	}

	// close all writers
	for _, e := range writer.eventWriters {
		if err := e.Close(); err != nil {
			return err
		}
	}
	return nil
}

type InsertBinlogWriter struct {
	baseBinlogWriter
}

func (writer *InsertBinlogWriter) NextInsertEventWriter() (*insertEventWriter, error) {
	if err := writer.checkClose(); err != nil {
		return nil, err
	}
	if err := writer.appendEventWriter(); err != nil {
		return nil, err
	}

	event, err := newInsertEventWriter(writer.payloadDataType, writer.offset)
	if err != nil {
		return nil, err
	}
	writer.currentEventWriter = event

	return event, nil
}

type DeleteBinlogWriter struct {
	baseBinlogWriter
}

func (writer *DeleteBinlogWriter) NextDeleteEventWriter() (*deleteEventWriter, error) {
	if err := writer.checkClose(); err != nil {
		return nil, err
	}
	if err := writer.appendEventWriter(); err != nil {
		return nil, err
	}

	event, err := newDeleteEventWriter(writer.payloadDataType, writer.offset)
	if err != nil {
		return nil, err
	}
	writer.currentEventWriter = event

	return event, nil
}

type DDLBinlogWriter struct {
	baseBinlogWriter
}

func (writer *DDLBinlogWriter) NextCreateCollectionEventWriter() (*createCollectionEventWriter, error) {
	if err := writer.checkClose(); err != nil {
		return nil, err
	}
	if err := writer.appendEventWriter(); err != nil {
		return nil, err
	}

	event, err := newCreateCollectionEventWriter(writer.payloadDataType, writer.offset)
	if err != nil {
		return nil, err
	}
	writer.currentEventWriter = event

	return event, nil
}

func (writer *DDLBinlogWriter) NextDropCollectionEventWriter() (*dropCollectionEventWriter, error) {
	if err := writer.checkClose(); err != nil {
		return nil, err
	}
	if err := writer.appendEventWriter(); err != nil {
		return nil, err
	}

	event, err := newDropCollectionEventWriter(writer.payloadDataType, writer.offset)
	if err != nil {
		return nil, err
	}
	writer.currentEventWriter = event

	return event, nil
}

func (writer *DDLBinlogWriter) NextCreatePartitionEventWriter() (*createPartitionEventWriter, error) {
	if err := writer.checkClose(); err != nil {
		return nil, err
	}
	if err := writer.appendEventWriter(); err != nil {
		return nil, err
	}

	event, err := newCreatePartitionEventWriter(writer.payloadDataType, writer.offset)
	if err != nil {
		return nil, err
	}
	writer.currentEventWriter = event

	return event, nil
}

func (writer *DDLBinlogWriter) NextDropPartitionEventWriter() (*dropPartitionEventWriter, error) {
	if err := writer.checkClose(); err != nil {
		return nil, err
	}
	if err := writer.appendEventWriter(); err != nil {
		return nil, err
	}

	event, err := newDropPartitionEventWriter(writer.payloadDataType, writer.offset)
	if err != nil {
		return nil, err
	}
	writer.currentEventWriter = event

	return event, nil
}

func NewInsertBinlogWriter(dataType schemapb.DataType) *InsertBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.payloadDataType = dataType
	return &InsertBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent:    descriptorEvent,
			magicNumber:        MagicNumber,
			binlogType:         InsertBinlog,
			eventWriters:       make([]EventWriter, 0),
			currentEventWriter: nil,
			buffer:             nil,
			numEvents:          0,
			numRows:            0,
			isClose:            false,
			offset:             4 + descriptorEvent.descriptorEventData.GetMemoryUsageInBytes(),
		},
	}
}
func NewDeleteBinlogWriter(dataType schemapb.DataType) *DeleteBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.payloadDataType = dataType
	return &DeleteBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent:    descriptorEvent,
			magicNumber:        MagicNumber,
			binlogType:         DeleteBinlog,
			eventWriters:       make([]EventWriter, 0),
			currentEventWriter: nil,
			buffer:             nil,
			numEvents:          0,
			numRows:            0,
			isClose:            false,
			offset:             4 + descriptorEvent.descriptorEventData.GetMemoryUsageInBytes(),
		},
	}
}
func NewDDLBinlogWriter(dataType schemapb.DataType) *DDLBinlogWriter {
	descriptorEvent := newDescriptorEvent()
	descriptorEvent.payloadDataType = dataType
	return &DDLBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent:    descriptorEvent,
			magicNumber:        MagicNumber,
			binlogType:         DDLBinlog,
			eventWriters:       make([]EventWriter, 0),
			currentEventWriter: nil,
			buffer:             nil,
			numEvents:          0,
			numRows:            0,
			isClose:            false,
			offset:             4 + descriptorEvent.descriptorEventData.GetMemoryUsageInBytes(),
		},
	}
}
