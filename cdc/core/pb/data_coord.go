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

package pb

import (
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/commonpb"
)

type SegmentStats struct {
	SegmentID            int64    `protobuf:"varint,1,opt,name=SegmentID,proto3" json:"SegmentID,omitempty"`
	NumRows              int64    `protobuf:"varint,2,opt,name=NumRows,proto3" json:"NumRows,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *SegmentStats) Reset()         { *m = SegmentStats{} }
func (m *SegmentStats) String() string { return proto.CompactTextString(m) }
func (*SegmentStats) ProtoMessage()    {}

func (m *SegmentStats) GetSegmentID() int64 {
	if m != nil {
		return m.SegmentID
	}
	return 0
}

func (m *SegmentStats) GetNumRows() int64 {
	if m != nil {
		return m.NumRows
	}
	return 0
}

type DataNodeTtMsg struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	ChannelName          string            `protobuf:"bytes,2,opt,name=channel_name,json=channelName,proto3" json:"channel_name,omitempty"`
	Timestamp            uint64            `protobuf:"varint,3,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	SegmentsStats        []*SegmentStats   `protobuf:"bytes,4,rep,name=segments_stats,json=segmentsStats,proto3" json:"segments_stats,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *DataNodeTtMsg) Reset()         { *m = DataNodeTtMsg{} }
func (m *DataNodeTtMsg) String() string { return proto.CompactTextString(m) }
func (*DataNodeTtMsg) ProtoMessage()    {}

func (m *DataNodeTtMsg) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *DataNodeTtMsg) GetChannelName() string {
	if m != nil {
		return m.ChannelName
	}
	return ""
}

func (m *DataNodeTtMsg) GetTimestamp() uint64 {
	if m != nil {
		return m.Timestamp
	}
	return 0
}

func (m *DataNodeTtMsg) GetSegmentsStats() []*SegmentStats {
	if m != nil {
		return m.SegmentsStats
	}
	return nil
}
