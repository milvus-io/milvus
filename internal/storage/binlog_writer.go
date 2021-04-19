package storage

import (
	"bytes"
	"encoding/binary"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
)

const (
	// todo : put to param table
	ServerID      = 1
	BinlogVersion = 1
	CommitID      = 1
	ServerVersion = 1
)

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
		return nil, errors.New("please close binlog before get buffer")
	}
	return writer.buffer.Bytes(), nil
}

// Close allocate buffer and release resource
func (writer *baseBinlogWriter) Close() error {
	if writer.buffer != nil {
		return nil
	}
	if writer.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if writer.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}

	var offset int32
	writer.buffer = new(bytes.Buffer)
	if err := binary.Write(writer.buffer, binary.LittleEndian, int32(MagicNumber)); err != nil {
		return err
	}
	if err := writer.descriptorEvent.Write(writer.buffer); err != nil {
		return err
	}
	offset = writer.descriptorEvent.GetMemoryUsageInBytes() + int32(binary.Size(MagicNumber))
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

type InsertBinlogWriter struct {
	baseBinlogWriter
}

func (writer *InsertBinlogWriter) NextInsertEventWriter() (*insertEventWriter, error) {
	if writer.isClosed() {
		return nil, errors.New("binlog has closed")
	}
	event, err := newInsertEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

type DeleteBinlogWriter struct {
	baseBinlogWriter
}

func (writer *DeleteBinlogWriter) NextDeleteEventWriter() (*deleteEventWriter, error) {
	if writer.isClosed() {
		return nil, errors.New("binlog has closed")
	}
	event, err := newDeleteEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

type DDLBinlogWriter struct {
	baseBinlogWriter
}

func (writer *DDLBinlogWriter) NextCreateCollectionEventWriter() (*createCollectionEventWriter, error) {
	if writer.isClosed() {
		return nil, errors.New("binlog has closed")
	}
	event, err := newCreateCollectionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

func (writer *DDLBinlogWriter) NextDropCollectionEventWriter() (*dropCollectionEventWriter, error) {
	if writer.isClosed() {
		return nil, errors.New("binlog has closed")
	}
	event, err := newDropCollectionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

func (writer *DDLBinlogWriter) NextCreatePartitionEventWriter() (*createPartitionEventWriter, error) {
	if writer.isClosed() {
		return nil, errors.New("binlog has closed")
	}
	event, err := newCreatePartitionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

func (writer *DDLBinlogWriter) NextDropPartitionEventWriter() (*dropPartitionEventWriter, error) {
	if writer.isClosed() {
		return nil, errors.New("binlog has closed")
	}
	event, err := newDropPartitionEventWriter(writer.PayloadDataType)
	if err != nil {
		return nil, err
	}
	writer.eventWriters = append(writer.eventWriters, event)
	return event, nil
}

func NewInsertBinlogWriter(dataType schemapb.DataType, collectionID, partitionID, segmentID, FieldID int64) (*InsertBinlogWriter, error) {
	descriptorEvent, err := newDescriptorEvent()
	if err != nil {
		return nil, err
	}
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
	}, nil
}
func NewDeleteBinlogWriter(dataType schemapb.DataType) (*DeleteBinlogWriter, error) {
	descriptorEvent, err := newDescriptorEvent()
	if err != nil {
		return nil, err
	}
	descriptorEvent.PayloadDataType = dataType
	return &DeleteBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent: *descriptorEvent,
			magicNumber:     MagicNumber,
			binlogType:      DeleteBinlog,
			eventWriters:    make([]EventWriter, 0),
			buffer:          nil,
		},
	}, nil
}
func NewDDLBinlogWriter(dataType schemapb.DataType) (*DDLBinlogWriter, error) {
	descriptorEvent, err := newDescriptorEvent()
	if err != nil {
		return nil, err
	}
	descriptorEvent.PayloadDataType = dataType
	return &DDLBinlogWriter{
		baseBinlogWriter: baseBinlogWriter{
			descriptorEvent: *descriptorEvent,
			magicNumber:     MagicNumber,
			binlogType:      DDLBinlog,
			eventWriters:    make([]EventWriter, 0),
			buffer:          nil,
		},
	}, nil
}
