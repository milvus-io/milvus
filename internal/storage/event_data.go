package storage

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/zilliztech/milvus-distributed/internal/proto/schemapb"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type descriptorEventData struct {
	descriptorEventDataFixPart
	postHeaderLengths []uint8
}

type descriptorEventDataFixPart struct {
	binlogVersion   int16
	serverVersion   int64
	commitID        int64
	headerLength    int8
	collectionID    int64
	partitionID     int64
	segmentID       int64
	startTimestamp  typeutil.Timestamp
	endTimestamp    typeutil.Timestamp
	payloadDataType schemapb.DataType
}

func (data *descriptorEventData) SetStartTimeStamp(ts typeutil.Timestamp) {
	data.startTimestamp = ts
}

func (data *descriptorEventData) SetEndTimeStamp(ts typeutil.Timestamp) {
	data.endTimestamp = ts
}

func (data *descriptorEventData) GetMemoryUsageInBytes() int32 {
	buf := new(bytes.Buffer)
	_ = data.Write(buf)
	return int32(buf.Len())
}

func (data *descriptorEventData) Write(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.descriptorEventDataFixPart); err != nil {
		return err
	}

	if err := binary.Write(buffer, binary.LittleEndian, data.postHeaderLengths); err != nil {
		return err
	}
	return nil
}

func readDescriptorEventData(buffer io.Reader) (*descriptorEventData, error) {
	event := newDescriptorEventData()

	if err := binary.Read(buffer, binary.LittleEndian, &event.descriptorEventDataFixPart); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binary.LittleEndian, &event.postHeaderLengths); err != nil {
		return nil, err
	}

	return &event, nil
}

type eventData interface {
	GetEventDataSize() int32
	WriteEventData(buffer io.Writer) error
}

// all event types' fixed part only have start timestamp and end timestamp yet, but maybe different events will
// have different fields later, so we just create a event data struct per event type.
type insertEventData struct {
	startTimestamp typeutil.Timestamp
	endTimestamp   typeutil.Timestamp
}

func (data *insertEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.startTimestamp = timestamp
}

func (data *insertEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.endTimestamp = timestamp
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
	startTimestamp typeutil.Timestamp
	endTimestamp   typeutil.Timestamp
}

func (data *deleteEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.startTimestamp = timestamp
}

func (data *deleteEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.endTimestamp = timestamp
}

func (data *deleteEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *deleteEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.startTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.endTimestamp); err != nil {
		return err
	}
	return nil
}

type createCollectionEventData struct {
	startTimestamp typeutil.Timestamp
	endTimestamp   typeutil.Timestamp
}

func (data *createCollectionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.startTimestamp = timestamp
}

func (data *createCollectionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.endTimestamp = timestamp
}

func (data *createCollectionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *createCollectionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.startTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.endTimestamp); err != nil {
		return err
	}
	return nil
}

type dropCollectionEventData struct {
	startTimestamp typeutil.Timestamp
	endTimestamp   typeutil.Timestamp
}

func (data *dropCollectionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.startTimestamp = timestamp
}

func (data *dropCollectionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.endTimestamp = timestamp
}

func (data *dropCollectionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *dropCollectionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.startTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.endTimestamp); err != nil {
		return err
	}
	return nil
}

type createPartitionEventData struct {
	startTimestamp typeutil.Timestamp
	endTimestamp   typeutil.Timestamp
}

func (data *createPartitionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.startTimestamp = timestamp
}

func (data *createPartitionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.endTimestamp = timestamp
}

func (data *createPartitionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *createPartitionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.startTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.endTimestamp); err != nil {
		return err
	}
	return nil
}

type dropPartitionEventData struct {
	startTimestamp typeutil.Timestamp
	endTimestamp   typeutil.Timestamp
}

func (data *dropPartitionEventData) SetStartTimestamp(timestamp typeutil.Timestamp) {
	data.startTimestamp = timestamp
}

func (data *dropPartitionEventData) SetEndTimestamp(timestamp typeutil.Timestamp) {
	data.endTimestamp = timestamp
}

func (data *dropPartitionEventData) GetEventDataSize() int32 {
	buf := new(bytes.Buffer)
	_ = binary.Write(buf, binary.LittleEndian, data)
	return int32(buf.Len())
}

func (data *dropPartitionEventData) WriteEventData(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.startTimestamp); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.endTimestamp); err != nil {
		return err
	}
	return nil
}

func newDescriptorEventData() descriptorEventData {
	data := descriptorEventData{
		descriptorEventDataFixPart: descriptorEventDataFixPart{
			binlogVersion:   BinlogVersion,
			serverVersion:   ServerVersion,
			commitID:        CommitID,
			collectionID:    -1,
			partitionID:     -1,
			segmentID:       -1,
			startTimestamp:  0,
			endTimestamp:    0,
			payloadDataType: -1,
		},
		postHeaderLengths: []uint8{16, 16, 16, 16, 16, 16},
	}
	data.headerLength = int8(data.GetMemoryUsageInBytes())
	return data
}

func newInsertEventData() insertEventData {
	return insertEventData{
		startTimestamp: 0,
		endTimestamp:   0,
	}
}
func newDeleteEventData() deleteEventData {
	return deleteEventData{
		startTimestamp: 0,
		endTimestamp:   0,
	}
}
func newCreateCollectionEventData() createCollectionEventData {
	return createCollectionEventData{
		startTimestamp: 0,
		endTimestamp:   0,
	}
}
func newDropCollectionEventData() dropCollectionEventData {
	return dropCollectionEventData{
		startTimestamp: 0,
		endTimestamp:   0,
	}
}
func newCreatePartitionEventData() createPartitionEventData {
	return createPartitionEventData{
		startTimestamp: 0,
		endTimestamp:   0,
	}
}
func newDropPartitionEventData() dropPartitionEventData {
	return dropPartitionEventData{
		startTimestamp: 0,
		endTimestamp:   0,
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
