// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package storage

import (
	"encoding/binary"
	"io"

	"errors"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type descriptorEventData struct {
	DescriptorEventDataFixPart
	StartPositionLen  int32
	StartPositionMsg  []byte
	EndPositionLen    int32
	EndPositionMsg    []byte
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
	FieldID         int64
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

func (data *descriptorEventData) SetStartPositionMsg(msg []byte) {
	data.StartPositionLen = int32(len(msg))
	data.StartPositionMsg = msg
}

func (data *descriptorEventData) SetEndPositionMsg(msg []byte) {
	data.EndPositionLen = int32(len(msg))
	data.EndPositionMsg = msg
}

func (data *descriptorEventData) GetMemoryUsageInBytes() int32 {
	return int32(binary.Size(data.DescriptorEventDataFixPart) + binary.Size(data.PostHeaderLengths) + binary.Size(data.StartPositionLen) + binary.Size(data.StartPositionMsg) +
		binary.Size(data.EndPositionLen) + binary.Size(data.EndPositionMsg))
}

func (data *descriptorEventData) Write(buffer io.Writer) error {
	if err := binary.Write(buffer, binlogEndian, data.DescriptorEventDataFixPart); err != nil {
		return err
	}

	if err := binary.Write(buffer, binlogEndian, data.StartPositionLen); err != nil {
		return err
	}

	buffer.Write(data.StartPositionMsg)

	if err := binary.Write(buffer, binlogEndian, data.EndPositionLen); err != nil {
		return err
	}

	buffer.Write(data.EndPositionMsg)
	buffer.Write(data.PostHeaderLengths)

	return nil
}

func readDescriptorEventData(buffer io.Reader, size int32) (*descriptorEventData, error) {
	event := &descriptorEventData{}

	if err := binary.Read(buffer, binlogEndian, &event.DescriptorEventDataFixPart); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binlogEndian, &event.StartPositionLen); err != nil {
		return nil, err
	}
	event.StartPositionMsg = make([]byte, event.StartPositionLen)
	if _, err := buffer.Read(event.StartPositionMsg); err != nil {
		return nil, err
	}

	if err := binary.Read(buffer, binlogEndian, &event.EndPositionLen); err != nil {
		return nil, err
	}
	event.EndPositionMsg = make([]byte, event.EndPositionLen)
	if _, err := buffer.Read(event.EndPositionMsg); err != nil {
		return nil, err
	}

	remain := size - event.GetMemoryUsageInBytes()
	event.PostHeaderLengths = make([]uint8, remain)
	if _, err := buffer.Read(event.PostHeaderLengths); err != nil {
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
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, binlogEndian, data)
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
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, binlogEndian, data)
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
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, binlogEndian, data)
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
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, binlogEndian, data)
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
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, binlogEndian, data)
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
	if data.StartTimestamp == 0 {
		return errors.New("hasn't set start time stamp")
	}
	if data.EndTimestamp == 0 {
		return errors.New("hasn't set end time stamp")
	}
	return binary.Write(buffer, binlogEndian, data)
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
		return (&createPartitionEventData{}).GetEventDataFixPartSize()
	case DropPartitionEventType:
		return (&dropPartitionEventData{}).GetEventDataFixPartSize()
	default:
		return -1
	}
}

func newDescriptorEventData() *descriptorEventData {
	data := descriptorEventData{
		DescriptorEventDataFixPart: DescriptorEventDataFixPart{
			BinlogVersion:   BinlogVersion,
			ServerVersion:   ServerVersion,
			CommitID:        CommitID,
			CollectionID:    -1,
			PartitionID:     -1,
			SegmentID:       -1,
			FieldID:         -1,
			StartTimestamp:  0,
			EndTimestamp:    0,
			PayloadDataType: -1,
		},
		StartPositionLen:  0,
		StartPositionMsg:  []byte{},
		EndPositionLen:    0,
		EndPositionMsg:    []byte{},
		PostHeaderLengths: []uint8{},
	}
	for _, v := range EventTypes {
		size := getEventFixPartSize(v)
		data.PostHeaderLengths = append(data.PostHeaderLengths, uint8(size))
	}
	return &data
}

func newInsertEventData() *insertEventData {
	return &insertEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newDeleteEventData() *deleteEventData {
	return &deleteEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newCreateCollectionEventData() *createCollectionEventData {
	return &createCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newDropCollectionEventData() *dropCollectionEventData {
	return &dropCollectionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newCreatePartitionEventData() *createPartitionEventData {
	return &createPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}
func newDropPartitionEventData() *dropPartitionEventData {
	return &dropPartitionEventData{
		StartTimestamp: 0,
		EndTimestamp:   0,
	}
}

func readInsertEventDataFixPart(buffer io.Reader) (*insertEventData, error) {
	data := &insertEventData{}
	if err := binary.Read(buffer, binlogEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDeleteEventDataFixPart(buffer io.Reader) (*deleteEventData, error) {
	data := &deleteEventData{}
	if err := binary.Read(buffer, binlogEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreateCollectionEventDataFixPart(buffer io.Reader) (*createCollectionEventData, error) {
	data := &createCollectionEventData{}
	if err := binary.Read(buffer, binlogEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropCollectionEventDataFixPart(buffer io.Reader) (*dropCollectionEventData, error) {
	data := &dropCollectionEventData{}
	if err := binary.Read(buffer, binlogEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readCreatePartitionEventDataFixPart(buffer io.Reader) (*createPartitionEventData, error) {
	data := &createPartitionEventData{}
	if err := binary.Read(buffer, binlogEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}

func readDropPartitionEventDataFixPart(buffer io.Reader) (*dropPartitionEventData, error) {
	data := &dropPartitionEventData{}
	if err := binary.Read(buffer, binlogEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}
