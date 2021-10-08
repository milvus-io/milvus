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
	"bytes"
	"encoding/binary"
	"fmt"
	"strconv"

	"errors"
)

// BinlogReader is an object to read binlog file. Binlog file's format can be
// found in design docs.
type BinlogReader struct {
	magicNumber int32
	descriptorEvent
	buffer    *bytes.Buffer
	eventList []*EventReader
	isClose   bool
}

// NextEventReader iters all events reader to read the binlog file.
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
		return -1, fmt.Errorf("parse magic number failed, expected: %s, actual: %s", strconv.Itoa(int(MagicNumber)), strconv.Itoa(int(reader.magicNumber)))
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

// Close closes the BinlogReader object.
// It mainly calls the Close method of the internal events, reclaims resources, and marks itself as closed.
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

// NewBinlogReader creates binlogReader to read binlog file.
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
