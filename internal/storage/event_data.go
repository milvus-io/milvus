package storage

import (
	"encoding/binary"
	"io"

	"github.com/zilliztech/milvus-distributed/internal/errors"
	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type descriptorEventData struct {
	DescriptorEventDataFixPart
	PostHeaderLengths []uint8
}

type DescriptorEventDataFixPart struct {
	BinlogVersion   int16
	ServerVersion   int64
	CommitID        int64
	HeaderLength    int8
	CollectionID    int64
	PartitionID     int64
	SegmentID       int64
	StartTimestamp  typeutil.Timestamp
	EndTimestamp    typeutil.Timestamp
	PayloadDataType schemapb.DataType
}

func (data *descriptorEventData) SetStartTimeStamp(ts typeutil.Timestamp) {
	data.StartTimestamp = ts
}

func (data *descriptorEventData) SetEndTimeStamp(ts typeutil.Timestamp) {
	data.EndTimestamp = ts
}

func (data *descriptorEventData) GetMemoryUsageInBytes() int32 {
	return int32(binary.Size(data.DescriptorEventDataFixPart) + binary.Size(data.PostHeaderLengths))
}

func (data *descriptorEventData) Write(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.DescriptorEventDataFixPart); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, data.PostHeaderLengths); err != nil {
		return err
	}
	return nil
}

func readDescriptorEventData(buffer io.Reader) (*descriptorEventData, error) {
	event, err := newDescriptorEventData()
	if err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &event.DescriptorEventDataFixPart); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &event.PostHeaderLengths); err != nil {
		return nil, err
	}

	return event, nil
}

type eventData interface {
	GetEventDataFixPartSize() int32
	WriteEventData(buffer io.Writer) error
}

// all event types' fixed part only have start Timestamp and end Timestamp yet, but maybe different events will
// have different fields later, so we just create a event data struct per event type.
type insertEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *insertEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.StartTimestamp = timestamp
}

func (data *insertEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.EndTimestamp = timestamp
}

func (data *insertEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *insertEventData) WriteEventData(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, data)
}

type deleteEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *deleteEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.StartTimestamp = timestamp
}

func (data *deleteEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.EndTimestamp = timestamp
}

func (data *deleteEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *deleteEventData) WriteEventData(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, data)
}

type createCollectionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *createCollectionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.StartTimestamp = timestamp
}

func (data *createCollectionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.EndTimestamp = timestamp
}

func (data *createCollectionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *createCollectionEventData) WriteEventData(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, data)
}

type dropCollectionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *dropCollectionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.StartTimestamp = timestamp
}

func (data *dropCollectionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.EndTimestamp = timestamp
}

func (data *dropCollectionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *dropCollectionEventData) WriteEventData(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, data)
}

type createPartitionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *createPartitionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.StartTimestamp = timestamp
}

func (data *createPartitionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.EndTimestamp = timestamp
}

func (data *createPartitionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *createPartitionEventData) WriteEventData(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, data)
}

type dropPartitionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *dropPartitionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.StartTimestamp = timestamp
}

func (data *dropPartitionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.EndTimestamp = timestamp
}

func (data *dropPartitionEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *dropPartitionEventData) WriteEventData(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, data)
}

func getEventFixPartSize(code EventTypeCode) int32 {
	switch code {
	case DescriptorEventType:
		return int32(binary.Size(descriptorEventData{}.DescriptorEventDataFixPart))
	case InsertEventType:
		return (&insertEventData{}).GetEventDataFixPartSize()
	case DeleteEventType:
		return (&deleteEventData{}).GetEventDataFixPartSize()
	case CreateCollectionEventType:
		return (&createCollectionEventData{}).GetEventDataFixPartSize()
	case DropCollectionEventType:
		return (&dropCollectionEventData{}).GetEventDataFixPartSize()
	case CreatePartitionEventType:
		return (&createCollectionEventData{}).GetEventDataFixPartSize()
	case DropPartitionEventType:
		return (&dropPartitionEventData{}).GetEventDataFixPartSize()
	default:
		return -1
	}
}

func newDescriptorEventData() (*descriptorEventData, error) {
	data := descriptorEventData{
		DescriptorEventDataFixPart: DescriptorEventDataFixPart{
			BinlogVersion:   BinlogVersion,
			ServerVersion:   ServerVersion,
			CommitID:        CommitID,
			CollectionID:    -1,
			PartitionID:     -1,
			SegmentID:       -1,
			StartTimestamp:  0,
			EndTimestamp:    0,
			PayloadDataType: -1,
		},
		PostHeaderLengths: []uint8{},
	}
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
		if size == -1 {
			return nil, errors.Errorf("undefined event type %d", i)
		}
		data.PostHeaderLengths = append(data.PostHeaderLengths, uint8(size))
	}
	data.HeaderLength = int8(data.GetMemoryUsageInBytes())
	return &data, nil
}

func newInsertEventData() (*insertEventData, error) {
	return &insertEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}, nil
}
func newDeleteEventData() (*deleteEventData, error) {
	return &deleteEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}, nil
}
func newCreateCollectionEventData() (*createCollectionEventData, error) {
	return &createCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}, nil
}
func newDropCollectionEventData() (*dropCollectionEventData, error) {
	return &dropCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}, nil
}
func newCreatePartitionEventData() (*createPartitionEventData, error) {
	return &createPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}, nil
}
func newDropPartitionEventData() (*dropPartitionEventData, error) {
	return &dropPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}, nil
}

func readInsertEventDataFixPart(buffer io.Reader) (*insertEventData, error) {
	data := &insertEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDeleteEventDataFixPart(buffer io.Reader) (*deleteEventData, error) {
	data := &deleteEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreateCollectionEventDataFixPart(buffer io.Reader) (*createCollectionEventData, error) {
	data := &createCollectionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropCollectionEventDataFixPart(buffer io.Reader) (*dropCollectionEventData, error) {
	data := &dropCollectionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreatePartitionEventDataFixPart(buffer io.Reader) (*createPartitionEventData, error) {
	data := &createPartitionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropPartitionEventDataFixPart(buffer io.Reader) (*dropPartitionEventData, error) {
	data := &dropPartitionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}
