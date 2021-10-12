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
	"time"

	"github.com/milvus-io/milvus/internal/util/tsoutil"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type baseEventHeader struct {
	Timestamp    typeutil.Timestamp
	TypeCode     EventTypeCode
	EventLength  int32
	NextPosition int32
}

func (header *baseEventHeader) GetMemoryUsageInBytes() int32 {
	return int32(binary.Size(header))
}

func (header *baseEventHeader) Write(buffer io.Writer) error {
	return binary.Write(buffer, binary.LittleEndian, header)
}

type descriptorEventHeader = baseEventHeader

type eventHeader struct {
	baseEventHeader
}

func readEventHeader(buffer io.Reader) (*eventHeader, error) {
	header := &eventHeader{}
	if err := binary.Read(buffer, binary.LittleEndian, header); err != nil {
		return nil, err
	}

	return header, nil
}

func readDescriptorEventHeader(buffer io.Reader) (*descriptorEventHeader, error) {
	header := &descriptorEventHeader{}
	if err := binary.Read(buffer, binary.LittleEndian, header); err != nil {
		return nil, err
	}
	return header, nil
}

func newDescriptorEventHeader() *descriptorEventHeader {
	header := descriptorEventHeader{
		Timestamp: tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
		TypeCode:  DescriptorEventType,
	}
	return &header
}

func newEventHeader(eventTypeCode EventTypeCode) *eventHeader {
	return &eventHeader{
		baseEventHeader: baseEventHeader{
			Timestamp:    tsoutil.ComposeTS(time.Now().UnixNano()/int64(time.Millisecond), 0),
			TypeCode:     eventTypeCode,
			EventLength:  -1,
			NextPosition: -1,
		},
	}
}
