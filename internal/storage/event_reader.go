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
	"fmt"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// EventReader is used to parse the events contained in the Binlog file.
type EventReader struct {
	eventHeader
	eventData
	PayloadReaderInterface
	buffer   *bytes.Buffer
	isClosed bool
}

func (reader *EventReader) readHeader() error {
	if reader.isClosed {
		return errors.New("event reader is closed")
	}
	header, err := readEventHeader(reader.buffer)
	if err != nil {
		return err
	}
	reader.eventHeader = *header
	return nil
}

func (reader *EventReader) readData() error {
	if reader.isClosed {
		return errors.New("event reader is closed")
	}
	var data eventData
	var err error
	switch reader.TypeCode {
	case InsertEventType:
		data, err = readInsertEventDataFixPart(reader.buffer)
	case DeleteEventType:
		data, err = readDeleteEventDataFixPart(reader.buffer)
	case CreateCollectionEventType:
		data, err = readCreateCollectionEventDataFixPart(reader.buffer)
	case DropCollectionEventType:
		data, err = readDropCollectionEventDataFixPart(reader.buffer)
	case CreatePartitionEventType:
		data, err = readCreatePartitionEventDataFixPart(reader.buffer)
	case DropPartitionEventType:
		data, err = readDropPartitionEventDataFixPart(reader.buffer)
	case IndexFileEventType:
		data, err = readIndexFileEventDataFixPart(reader.buffer)
	default:
		return fmt.Errorf("unknown header type code: %d", reader.TypeCode)
	}
	if err != nil {
		return err
	}

	reader.eventData = data
	return nil
}

// Close closes EventReader object. It mainly calls the Close method of inner PayloadReaderInterface and
// mark itself as closed.
func (reader *EventReader) Close() {
	if !reader.isClosed {
		reader.isClosed = true
		reader.PayloadReaderInterface.Close()
	}
}

func newEventReader(datatype schemapb.DataType, buffer *bytes.Buffer, nullable bool) (*EventReader, error) {
	reader := &EventReader{
		eventHeader: eventHeader{
			baseEventHeader{},
		},
		buffer:   buffer,
		isClosed: false,
	}

	if err := reader.readHeader(); err != nil {
		return nil, err
	}
	if err := reader.readData(); err != nil {
		return nil, err
	}

	next := int(reader.EventLength - reader.eventHeader.GetMemoryUsageInBytes() - reader.GetEventDataFixPartSize())
	payloadBuffer := buffer.Next(next)
	payloadReader, err := NewPayloadReader(datatype, payloadBuffer, nullable)
	if err != nil {
		return nil, err
	}
	reader.PayloadReaderInterface = payloadReader
	return reader, nil
}
