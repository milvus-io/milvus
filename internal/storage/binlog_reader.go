// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/pkg/common"
)

// BinlogReader is an object to read binlog file. Binlog file's format can be
// found in design docs.
type BinlogReader struct {
	magicNumber int32
	descriptorEvent
	buffer      *bytes.Buffer
	eventReader *EventReader
	isClose     bool
}

// NextEventReader iters all events reader to read the binlog file.
func (reader *BinlogReader) NextEventReader() (*EventReader, error) {
	if reader.isClose {
		return nil, errors.New("bin log reader is closed")
	}
	if reader.buffer.Len() <= 0 {
		return nil, nil
	}
	// close the previous reader
	if reader.eventReader != nil {
		reader.eventReader.Close()
	}
	nullable, err := reader.descriptorEvent.GetNullable()
	if err != nil {
		return nil, err
	}
	reader.eventReader, err = newEventReader(reader.descriptorEvent.PayloadDataType, reader.buffer, nullable)
	if err != nil {
		return nil, err
	}
	return reader.eventReader, nil
}

func (reader *BinlogReader) readMagicNumber() (int32, error) {
	var err error
	reader.magicNumber, err = readMagicNumber(reader.buffer)
	return reader.magicNumber, err
}

func (reader *BinlogReader) readDescriptorEvent() (*descriptorEvent, error) {
	event, err := ReadDescriptorEvent(reader.buffer)
	if err != nil {
		return nil, err
	}
	reader.descriptorEvent = *event
	return &reader.descriptorEvent, nil
}

func readMagicNumber(buffer io.Reader) (int32, error) {
	var magicNumber int32
	if err := binary.Read(buffer, common.Endian, &magicNumber); err != nil {
		return -1, err
	}
	if magicNumber != MagicNumber {
		return -1, fmt.Errorf("parse magic number failed, expected: %d, actual: %d", MagicNumber, magicNumber)
	}

	return magicNumber, nil
}

// ReadDescriptorEvent reads a descriptorEvent from buffer
func ReadDescriptorEvent(buffer io.Reader) (*descriptorEvent, error) {
	header, err := readDescriptorEventHeader(buffer)
	if err != nil {
		return nil, err
	}
	data, err := readDescriptorEventData(buffer)
	if err != nil {
		return nil, err
	}
	return &descriptorEvent{
		descriptorEventHeader: *header,
		descriptorEventData:   *data,
	}, nil
}

// Close closes the BinlogReader object.
// It mainly calls the Close method of the internal events, reclaims resources, and marks itself as closed.
func (reader *BinlogReader) Close() {
	if reader == nil {
		return
	}
	if reader.isClose {
		return
	}
	if reader.eventReader != nil {
		reader.eventReader.Close()
	}
	reader.isClose = true
}

// NewBinlogReader creates binlogReader to read binlog file.
func NewBinlogReader(data []byte) (*BinlogReader, error) {
	reader := &BinlogReader{
		buffer:  bytes.NewBuffer(data),
		isClose: false,
	}

	if _, err := reader.readMagicNumber(); err != nil {
		return nil, err
	}
	if _, err := reader.readDescriptorEvent(); err != nil {
		return nil, err
	}
	return reader, nil
}
