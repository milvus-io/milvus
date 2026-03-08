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
	"encoding/binary"
	"io"
	"time"

	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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
	return binary.Write(buffer, common.Endian, header)
}

type descriptorEventHeader = baseEventHeader

type eventHeader struct {
	baseEventHeader
}

func readEventHeader(buffer io.Reader) (*eventHeader, error) {
	header := &eventHeader{}
	if err := binary.Read(buffer, common.Endian, header); err != nil {
		return nil, err
	}

	return header, nil
}

func readDescriptorEventHeader(buffer io.Reader) (*descriptorEventHeader, error) {
	header := &descriptorEventHeader{}
	if err := binary.Read(buffer, common.Endian, header); err != nil {
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
