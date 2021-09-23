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
	"errors"
	"io"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type descriptorEventData struct {
	DescriptorEventDataFixPart
	PostHeaderLengths []uint8
}

// DescriptorEventDataFixPart is a memorty struct saves events' DescriptorEventData.
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

// SetEventTimeStamp set the timestamp value of DescriptorEventDataFixPart.
func (data *descriptorEventData) SetEventTimeStamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

// SetEventTimeStamp returns the memory size of DescriptorEventDataFixPart.
func (data *descriptorEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data.DescriptorEventDataFixPart))
}

// SetEventTimeStamp returns the memory size of DescriptorEventDataFixPart.
func (data *descriptorEventData) GetMemoryUsageInBytes() int32 {
	return data.GetEventDataFixPartSize() + int32(binary.Size(data.PostHeaderLengths))
}

// Write transfer DescriptorEventDataFixPart to binary buffer.
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

func (data *insertEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
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
	return binary.Write(buffer, binary.LittleEndian, data)
}

type deleteEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *deleteEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
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
	return binary.Write(buffer, binary.LittleEndian, data)
}

type createCollectionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *createCollectionEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
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
	return binary.Write(buffer, binary.LittleEndian, data)
}

type dropCollectionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *dropCollectionEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
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
	return binary.Write(buffer, binary.LittleEndian, data)
}

type createPartitionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *createPartitionEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
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
	return binary.Write(buffer, binary.LittleEndian, data)
}

type dropPartitionEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *dropPartitionEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
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
	return binary.Write(buffer, binary.LittleEndian, data)
}

func getEventFixPartSize(code EventTypeCode) int32 {
	switch code {
	case DescriptorEventType:
		return (&descriptorEventData{}).GetEventDataFixPartSize()
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
		PostHeaderLengths: []uint8{},
	}
	for i := DescriptorEventType; i < EventTypeEnd; i++ {
		size := getEventFixPartSize(i)
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
