package storage

import (
	"bytes"
	"encoding/binary"
	"io"

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
	buf := new(bytes.Buffer)
	_ = data.Write(buf)
	return int32(buf.Len())
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
	event := newDescriptorEventData()

	if err := binary.Read(buffer, binary.LittleEndian, &event.DescriptorEventDataFixPart); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &event.PostHeaderLengths); err != nil {
		return nil, err
	}

	return &event, nil
}

type eventData interface {
	GetEventDataSize() int32
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

func (data *insertEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
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

func (data *deleteEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *deleteEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.StartTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.EndTimestamp); err != nil {
		return err
	}
	return nil
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

func (data *createCollectionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *createCollectionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.StartTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.EndTimestamp); err != nil {
		return err
	}
	return nil
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

func (data *dropCollectionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *dropCollectionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.StartTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.EndTimestamp); err != nil {
		return err
	}
	return nil
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

func (data *createPartitionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *createPartitionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.StartTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.EndTimestamp); err != nil {
		return err
	}
	return nil
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

func (data *dropPartitionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *dropPartitionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.StartTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.EndTimestamp); err != nil {
		return err
	}
	return nil
}

func newDescriptorEventData() descriptorEventData {
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
		PostHeaderLengths: []uint8{16, 16, 16, 16, 16, 16},
	}
	data.HeaderLength = int8(data.GetMemoryUsageInBytes())
	return data
}

func newInsertEventData() insertEventData {
	return insertEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newDeleteEventData() deleteEventData {
	return deleteEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newCreateCollectionEventData() createCollectionEventData {
	return createCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newDropCollectionEventData() dropCollectionEventData {
	return dropCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newCreatePartitionEventData() createPartitionEventData {
	return createPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newDropPartitionEventData() dropPartitionEventData {
	return dropPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func readInsertEventData(buffer io.Reader) (*insertEventData, error) {
	data := &insertEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDeleteEventData(buffer io.Reader) (*deleteEventData, error) {
	data := &deleteEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreateCollectionEventData(buffer io.Reader) (*createCollectionEventData, error) {
	data := &createCollectionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropCollectionEventData(buffer io.Reader) (*dropCollectionEventData, error) {
	data := &dropCollectionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreatePartitionEventData(buffer io.Reader) (*createPartitionEventData, error) {
	data := &createPartitionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropPartitionEventData(buffer io.Reader) (*dropPartitionEventData, error) {
	data := &dropPartitionEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}
