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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const originalSizeKey = "original_size"

type descriptorEventData struct {
	DescriptorEventDataFixPart
	ExtraLength       int32
	ExtraBytes        []byte
	Extras            map[string]interface{}
	PostHeaderLengths []uint8
}

// DescriptorEventDataFixPart is a memorty struct saves events' DescriptorEventData.
type DescriptorEventDataFixPart struct {
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

// GetEventDataFixPartSize returns the memory size of DescriptorEventDataFixPart.
func (data *descriptorEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data.DescriptorEventDataFixPart))
}

// GetMemoryUsageInBytes returns the memory size of DescriptorEventDataFixPart.
func (data *descriptorEventData) GetMemoryUsageInBytes() int32 {
	return data.GetEventDataFixPartSize() + int32(binary.Size(data.PostHeaderLengths)) + int32(binary.Size(data.ExtraLength)) + data.ExtraLength

}

// AddExtra add extra params to description event.
func (data *descriptorEventData) AddExtra(k string, v interface{}) {
	data.Extras[k] = v
}

// FinishExtra marshal extras to json format.
// Call before GetMemoryUsageInBytes to get a accurate length of description event.
func (data *descriptorEventData) FinishExtra() error {
	var err error

	// keep all binlog file records the original size
	sizeStored, ok := data.Extras[originalSizeKey]
	if !ok {
		return fmt.Errorf("%v not in extra", originalSizeKey)
	}
	// if we store a large int directly, golang will use scientific notation, we then will get a float value.
	// so it's better to store the original size in string format.
	sizeStr, ok := sizeStored.(string)
	if !ok {
		return fmt.Errorf("value of %v must in string format", originalSizeKey)
	}
	_, err = strconv.Atoi(sizeStr)
	if err != nil {
		return fmt.Errorf("value of %v must be able to be converted into int format", originalSizeKey)
	}

	data.ExtraBytes, err = json.Marshal(data.Extras)
	if err != nil {
		return err
	}
	data.ExtraLength = int32(len(data.ExtraBytes))
	return nil
}

// Write transfer DescriptorEventDataFixPart to binary buffer.
func (data *descriptorEventData) Write(buffer io.Writer) error {
	if err := binary.Write(buffer, binary.LittleEndian, data.DescriptorEventDataFixPart); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.PostHeaderLengths); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.ExtraLength); err != nil {
		return err
	}
	if err := binary.Write(buffer, binary.LittleEndian, data.ExtraBytes); err != nil {
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

	if err := binary.Read(buffer, binary.LittleEndian, &event.ExtraLength); err != nil {
		return nil, err
	}
	event.ExtraBytes = make([]byte, event.ExtraLength)
	if err := binary.Read(buffer, binary.LittleEndian, &event.ExtraBytes); err != nil {
		return nil, err
	}
	if err := json.Unmarshal(event.ExtraBytes, &event.Extras); err != nil {
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

type indexFileEventData struct {
	StartTimestamp typeutil.Timestamp
	EndTimestamp   typeutil.Timestamp
}

func (data *indexFileEventData) SetEventTimestamp(start typeutil.Timestamp, end typeutil.Timestamp) {
	data.StartTimestamp = start
	data.EndTimestamp = end
}

func (data *indexFileEventData) GetEventDataFixPartSize() int32 {
	return int32(binary.Size(data))
}

func (data *indexFileEventData) WriteEventData(buffer io.Writer) error {
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
	case IndexFileEventType:
		return (&indexFileEventData{}).GetEventDataFixPartSize()
	default:
		return -1
	}
}

func newDescriptorEventData() *descriptorEventData {
	data := descriptorEventData{
		DescriptorEventDataFixPart: DescriptorEventDataFixPart{
			CollectionID:    -1,
			PartitionID:     -1,
			SegmentID:       -1,
			FieldID:         -1,
			StartTimestamp:  0,
			EndTimestamp:    0,
			PayloadDataType: -1,
		},
		PostHeaderLengths: []uint8{},
		Extras:            make(map[string]interface{}),
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
func newIndexFileEventData() *indexFileEventData {
	return &indexFileEventData{
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

func readIndexFileEventDataFixPart(buffer io.Reader) (*indexFileEventData, error) {
	data := &indexFileEventData{}
	if err := binary.Read(buffer, binary.LittleEndian, data); err != nil {
		return nil, err
	}
	return data, nil
}
