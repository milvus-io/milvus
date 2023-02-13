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
	"github.com/milvus-io/milvus-proto/go-api/schemapb"
)

type InsertDataVersion int32

const (
	// 0 must refer to row-based format, since it's the first version in Milvus.
	InsertDataVersion_RowBased    InsertDataVersion = 0
	InsertDataVersion_ColumnBased InsertDataVersion = 1
)

var InsertDataVersion_name = map[int32]string{
	0: "RowBased",
	1: "ColumnBased",
}

var InsertDataVersion_value = map[string]int32{
	"RowBased":    0,
	"ColumnBased": 1,
}

func (x InsertDataVersion) String() string {
	return proto.EnumName(InsertDataVersion_name, int32(x))
}

type MsgPosition struct {
	ChannelName          string   `protobuf:"bytes,1,opt,name=channel_name,json=channelName,proto3" json:"channel_name,omitempty"`
	MsgID                []byte   `protobuf:"bytes,2,opt,name=msgID,proto3" json:"msgID,omitempty"`
	MsgGroup             string   `protobuf:"bytes,3,opt,name=msgGroup,proto3" json:"msgGroup,omitempty"`
	Timestamp            uint64   `protobuf:"varint,4,opt,name=timestamp,proto3" json:"timestamp,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *MsgPosition) Reset()         { *m = MsgPosition{} }
func (m *MsgPosition) String() string { return proto.CompactTextString(m) }
func (*MsgPosition) ProtoMessage()    {}

type InsertRequest struct {
	Base           *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	ShardName      string            `protobuf:"bytes,2,opt,name=shardName,proto3" json:"shardName,omitempty"`
	DbName         string            `protobuf:"bytes,3,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName string            `protobuf:"bytes,4,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	PartitionName  string            `protobuf:"bytes,5,opt,name=partition_name,json=partitionName,proto3" json:"partition_name,omitempty"`
	DbID           int64             `protobuf:"varint,6,opt,name=dbID,proto3" json:"dbID,omitempty"`
	CollectionID   int64             `protobuf:"varint,7,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID    int64             `protobuf:"varint,8,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	SegmentID      int64             `protobuf:"varint,9,opt,name=segmentID,proto3" json:"segmentID,omitempty"`
	Timestamps     []uint64          `protobuf:"varint,10,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	RowIDs         []int64           `protobuf:"varint,11,rep,packed,name=rowIDs,proto3" json:"rowIDs,omitempty"`
	// row_data was reserved for compatibility
	RowData              []*commonpb.Blob      `protobuf:"bytes,12,rep,name=row_data,json=rowData,proto3" json:"row_data,omitempty"`
	FieldsData           []*schemapb.FieldData `protobuf:"bytes,13,rep,name=fields_data,json=fieldsData,proto3" json:"fields_data,omitempty"`
	NumRows              uint64                `protobuf:"varint,14,opt,name=num_rows,json=numRows,proto3" json:"num_rows,omitempty"`
	Version              InsertDataVersion     `protobuf:"varint,15,opt,name=version,proto3,enum=milvus.proto.internal.InsertDataVersion" json:"version,omitempty"`
	XXX_NoUnkeyedLiteral struct{}              `json:"-"`
	XXX_unrecognized     []byte                `json:"-"`
	XXX_sizecache        int32                 `json:"-"`
}

func (m *InsertRequest) Reset()         { *m = InsertRequest{} }
func (m *InsertRequest) String() string { return proto.CompactTextString(m) }
func (*InsertRequest) ProtoMessage()    {}

func (m *InsertRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *InsertRequest) GetShardName() string {
	if m != nil {
		return m.ShardName
	}
	return ""
}

func (m *InsertRequest) GetDbName() string {
	if m != nil {
		return m.DbName
	}
	return ""
}

func (m *InsertRequest) GetCollectionName() string {
	if m != nil {
		return m.CollectionName
	}
	return ""
}

func (m *InsertRequest) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *InsertRequest) GetDbID() int64 {
	if m != nil {
		return m.DbID
	}
	return 0
}

func (m *InsertRequest) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *InsertRequest) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}

func (m *InsertRequest) GetSegmentID() int64 {
	if m != nil {
		return m.SegmentID
	}
	return 0
}

func (m *InsertRequest) GetTimestamps() []uint64 {
	if m != nil {
		return m.Timestamps
	}
	return nil
}

func (m *InsertRequest) GetRowIDs() []int64 {
	if m != nil {
		return m.RowIDs
	}
	return nil
}

func (m *InsertRequest) GetRowData() []*commonpb.Blob {
	if m != nil {
		return m.RowData
	}
	return nil
}

func (m *InsertRequest) GetFieldsData() []*schemapb.FieldData {
	if m != nil {
		return m.FieldsData
	}
	return nil
}

func (m *InsertRequest) GetNumRows() uint64 {
	if m != nil {
		return m.NumRows
	}
	return 0
}

func (m *InsertRequest) GetVersion() InsertDataVersion {
	if m != nil {
		return m.Version
	}
	return InsertDataVersion_RowBased
}

type DeleteRequest struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	ShardName            string            `protobuf:"bytes,2,opt,name=shardName,proto3" json:"shardName,omitempty"`
	DbName               string            `protobuf:"bytes,3,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName       string            `protobuf:"bytes,4,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	PartitionName        string            `protobuf:"bytes,5,opt,name=partition_name,json=partitionName,proto3" json:"partition_name,omitempty"`
	DbID                 int64             `protobuf:"varint,6,opt,name=dbID,proto3" json:"dbID,omitempty"`
	CollectionID         int64             `protobuf:"varint,7,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID          int64             `protobuf:"varint,8,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	Int64PrimaryKeys     []int64           `protobuf:"varint,9,rep,packed,name=int64_primary_keys,json=int64PrimaryKeys,proto3" json:"int64_primary_keys,omitempty"`
	Timestamps           []uint64          `protobuf:"varint,10,rep,packed,name=timestamps,proto3" json:"timestamps,omitempty"`
	NumRows              int64             `protobuf:"varint,11,opt,name=num_rows,json=numRows,proto3" json:"num_rows,omitempty"`
	PrimaryKeys          *schemapb.IDs     `protobuf:"bytes,12,opt,name=primary_keys,json=primaryKeys,proto3" json:"primary_keys,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *DeleteRequest) Reset()         { *m = DeleteRequest{} }
func (m *DeleteRequest) String() string { return proto.CompactTextString(m) }
func (*DeleteRequest) ProtoMessage()    {}

func (m *DeleteRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *DeleteRequest) GetShardName() string {
	if m != nil {
		return m.ShardName
	}
	return ""
}

func (m *DeleteRequest) GetDbName() string {
	if m != nil {
		return m.DbName
	}
	return ""
}

func (m *DeleteRequest) GetCollectionName() string {
	if m != nil {
		return m.CollectionName
	}
	return ""
}

func (m *DeleteRequest) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *DeleteRequest) GetDbID() int64 {
	if m != nil {
		return m.DbID
	}
	return 0
}

func (m *DeleteRequest) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *DeleteRequest) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}

func (m *DeleteRequest) GetInt64PrimaryKeys() []int64 {
	if m != nil {
		return m.Int64PrimaryKeys
	}
	return nil
}

func (m *DeleteRequest) GetTimestamps() []uint64 {
	if m != nil {
		return m.Timestamps
	}
	return nil
}

func (m *DeleteRequest) GetNumRows() int64 {
	if m != nil {
		return m.NumRows
	}
	return 0
}

func (m *DeleteRequest) GetPrimaryKeys() *schemapb.IDs {
	if m != nil {
		return m.PrimaryKeys
	}
	return nil
}

type TimeTickMsg struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *TimeTickMsg) Reset()         { *m = TimeTickMsg{} }
func (m *TimeTickMsg) String() string { return proto.CompactTextString(m) }
func (*TimeTickMsg) ProtoMessage()    {}

func (m *TimeTickMsg) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

type CreateCollectionRequest struct {
	Base           *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	DbName         string            `protobuf:"bytes,2,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName string            `protobuf:"bytes,3,opt,name=collectionName,proto3" json:"collectionName,omitempty"`
	PartitionName  string            `protobuf:"bytes,4,opt,name=partitionName,proto3" json:"partitionName,omitempty"`
	// `schema` is the serialized `schema.CollectionSchema`
	DbID                 int64    `protobuf:"varint,5,opt,name=dbID,proto3" json:"dbID,omitempty"`
	CollectionID         int64    `protobuf:"varint,6,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID          int64    `protobuf:"varint,7,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	Schema               []byte   `protobuf:"bytes,8,opt,name=schema,proto3" json:"schema,omitempty"`
	VirtualChannelNames  []string `protobuf:"bytes,9,rep,name=virtualChannelNames,proto3" json:"virtualChannelNames,omitempty"`
	PhysicalChannelNames []string `protobuf:"bytes,10,rep,name=physicalChannelNames,proto3" json:"physicalChannelNames,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_unrecognized     []byte   `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *CreateCollectionRequest) Reset()         { *m = CreateCollectionRequest{} }
func (m *CreateCollectionRequest) String() string { return proto.CompactTextString(m) }
func (*CreateCollectionRequest) ProtoMessage()    {}

func (m *CreateCollectionRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *CreateCollectionRequest) GetDbName() string {
	if m != nil {
		return m.DbName
	}
	return ""
}

func (m *CreateCollectionRequest) GetCollectionName() string {
	if m != nil {
		return m.CollectionName
	}
	return ""
}

func (m *CreateCollectionRequest) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *CreateCollectionRequest) GetDbID() int64 {
	if m != nil {
		return m.DbID
	}
	return 0
}

func (m *CreateCollectionRequest) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *CreateCollectionRequest) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}

func (m *CreateCollectionRequest) GetSchema() []byte {
	if m != nil {
		return m.Schema
	}
	return nil
}

func (m *CreateCollectionRequest) GetVirtualChannelNames() []string {
	if m != nil {
		return m.VirtualChannelNames
	}
	return nil
}

func (m *CreateCollectionRequest) GetPhysicalChannelNames() []string {
	if m != nil {
		return m.PhysicalChannelNames
	}
	return nil
}

type DropCollectionRequest struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	DbName               string            `protobuf:"bytes,2,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName       string            `protobuf:"bytes,3,opt,name=collectionName,proto3" json:"collectionName,omitempty"`
	DbID                 int64             `protobuf:"varint,4,opt,name=dbID,proto3" json:"dbID,omitempty"`
	CollectionID         int64             `protobuf:"varint,5,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *DropCollectionRequest) Reset()         { *m = DropCollectionRequest{} }
func (m *DropCollectionRequest) String() string { return proto.CompactTextString(m) }
func (*DropCollectionRequest) ProtoMessage()    {}

func (m *DropCollectionRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *DropCollectionRequest) GetDbName() string {
	if m != nil {
		return m.DbName
	}
	return ""
}

func (m *DropCollectionRequest) GetCollectionName() string {
	if m != nil {
		return m.CollectionName
	}
	return ""
}

func (m *DropCollectionRequest) GetDbID() int64 {
	if m != nil {
		return m.DbID
	}
	return 0
}

func (m *DropCollectionRequest) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

type CreatePartitionRequest struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	DbName               string            `protobuf:"bytes,2,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName       string            `protobuf:"bytes,3,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	PartitionName        string            `protobuf:"bytes,4,opt,name=partition_name,json=partitionName,proto3" json:"partition_name,omitempty"`
	DbID                 int64             `protobuf:"varint,5,opt,name=dbID,proto3" json:"dbID,omitempty"`
	CollectionID         int64             `protobuf:"varint,6,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID          int64             `protobuf:"varint,7,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *CreatePartitionRequest) Reset()         { *m = CreatePartitionRequest{} }
func (m *CreatePartitionRequest) String() string { return proto.CompactTextString(m) }
func (*CreatePartitionRequest) ProtoMessage()    {}

func (m *CreatePartitionRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *CreatePartitionRequest) GetDbName() string {
	if m != nil {
		return m.DbName
	}
	return ""
}

func (m *CreatePartitionRequest) GetCollectionName() string {
	if m != nil {
		return m.CollectionName
	}
	return ""
}

func (m *CreatePartitionRequest) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *CreatePartitionRequest) GetDbID() int64 {
	if m != nil {
		return m.DbID
	}
	return 0
}

func (m *CreatePartitionRequest) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *CreatePartitionRequest) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}

type DropPartitionRequest struct {
	Base                 *commonpb.MsgBase `protobuf:"bytes,1,opt,name=base,proto3" json:"base,omitempty"`
	DbName               string            `protobuf:"bytes,2,opt,name=db_name,json=dbName,proto3" json:"db_name,omitempty"`
	CollectionName       string            `protobuf:"bytes,3,opt,name=collection_name,json=collectionName,proto3" json:"collection_name,omitempty"`
	PartitionName        string            `protobuf:"bytes,4,opt,name=partition_name,json=partitionName,proto3" json:"partition_name,omitempty"`
	DbID                 int64             `protobuf:"varint,5,opt,name=dbID,proto3" json:"dbID,omitempty"`
	CollectionID         int64             `protobuf:"varint,6,opt,name=collectionID,proto3" json:"collectionID,omitempty"`
	PartitionID          int64             `protobuf:"varint,7,opt,name=partitionID,proto3" json:"partitionID,omitempty"`
	XXX_NoUnkeyedLiteral struct{}          `json:"-"`
	XXX_unrecognized     []byte            `json:"-"`
	XXX_sizecache        int32             `json:"-"`
}

func (m *DropPartitionRequest) Reset()         { *m = DropPartitionRequest{} }
func (m *DropPartitionRequest) String() string { return proto.CompactTextString(m) }
func (*DropPartitionRequest) ProtoMessage()    {}

func (m *DropPartitionRequest) GetBase() *commonpb.MsgBase {
	if m != nil {
		return m.Base
	}
	return nil
}

func (m *DropPartitionRequest) GetDbName() string {
	if m != nil {
		return m.DbName
	}
	return ""
}

func (m *DropPartitionRequest) GetCollectionName() string {
	if m != nil {
		return m.CollectionName
	}
	return ""
}

func (m *DropPartitionRequest) GetPartitionName() string {
	if m != nil {
		return m.PartitionName
	}
	return ""
}

func (m *DropPartitionRequest) GetDbID() int64 {
	if m != nil {
		return m.DbID
	}
	return 0
}

func (m *DropPartitionRequest) GetCollectionID() int64 {
	if m != nil {
		return m.CollectionID
	}
	return 0
}

func (m *DropPartitionRequest) GetPartitionID() int64 {
	if m != nil {
		return m.PartitionID
	}
	return 0
}
