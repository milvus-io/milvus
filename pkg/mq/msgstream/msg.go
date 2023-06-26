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

package msgstream

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/golang/protobuf/proto"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"

	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/funcutil"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

// MsgType is an alias of commonpb.MsgType
type MsgType = commonpb.MsgType

// MarshalType is an empty interface
type MarshalType = interface{}

// TsMsg provides methods to get begin timestamp and end timestamp of a message pack
type TsMsg interface {
	TraceCtx() context.Context
	SetTraceCtx(ctx context.Context)
	ID() UniqueID
	SetID(id UniqueID)
	BeginTs() Timestamp
	EndTs() Timestamp
	Type() MsgType
	SourceID() int64
	HashKeys() []uint32
	Marshal(TsMsg) (MarshalType, error)
	Unmarshal(MarshalType) (TsMsg, error)
	Position() *MsgPosition
	SetPosition(*MsgPosition)
	Size() int
}

// BaseMsg is a basic structure that contains begin timestamp, end timestamp and the position of msgstream
type BaseMsg struct {
	Ctx            context.Context
	BeginTimestamp Timestamp
	EndTimestamp   Timestamp
	HashValues     []uint32
	MsgPosition    *MsgPosition
}

// TraceCtx returns the context of opentracing
func (bm *BaseMsg) TraceCtx() context.Context {
	return bm.Ctx
}

// SetTraceCtx is used to set context for opentracing
func (bm *BaseMsg) SetTraceCtx(ctx context.Context) {
	bm.Ctx = ctx
}

// BeginTs returns the begin timestamp of this message pack
func (bm *BaseMsg) BeginTs() Timestamp {
	return bm.BeginTimestamp
}

// EndTs returns the end timestamp of this message pack
func (bm *BaseMsg) EndTs() Timestamp {
	return bm.EndTimestamp
}

// HashKeys returns the end timestamp of this message pack
func (bm *BaseMsg) HashKeys() []uint32 {
	return bm.HashValues
}

// Position returns the position of this message pack in msgstream
func (bm *BaseMsg) Position() *MsgPosition {
	return bm.MsgPosition
}

// SetPosition is used to set position of this message in msgstream
func (bm *BaseMsg) SetPosition(position *MsgPosition) {
	bm.MsgPosition = position
}

func convertToByteArray(input interface{}) ([]byte, error) {
	switch output := input.(type) {
	case []byte:
		return output, nil
	default:
		return nil, errors.New("Cannot convert interface{} to []byte")
	}
}

/////////////////////////////////////////Insert//////////////////////////////////////////

// InsertMsg is a message pack that contains insert request
type InsertMsg struct {
	BaseMsg
	msgpb.InsertRequest
}

// interface implementation validation
var _ TsMsg = &InsertMsg{}

// ID returns the ID of this message pack
func (it *InsertMsg) ID() UniqueID {
	return it.Base.MsgID
}

// SetID set the ID of this message pack
func (it *InsertMsg) SetID(id UniqueID) {
	it.Base.MsgID = id
}

// Type returns the type of this message pack
func (it *InsertMsg) Type() MsgType {
	return it.Base.MsgType
}

// SourceID indicates which component generated this message
func (it *InsertMsg) SourceID() int64 {
	return it.Base.SourceID
}

// Marshal is used to serialize a message pack to byte array
func (it *InsertMsg) Marshal(input TsMsg) (MarshalType, error) {
	insertMsg := input.(*InsertMsg)
	insertRequest := &insertMsg.InsertRequest
	mb, err := proto.Marshal(insertRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserialize a message pack from byte array
func (it *InsertMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	insertRequest := msgpb.InsertRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &insertRequest)
	if err != nil {
		return nil, err
	}
	insertMsg := &InsertMsg{InsertRequest: insertRequest}
	for _, timestamp := range insertMsg.Timestamps {
		insertMsg.BeginTimestamp = timestamp
		insertMsg.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range insertMsg.Timestamps {
		if timestamp > insertMsg.EndTimestamp {
			insertMsg.EndTimestamp = timestamp
		}
		if timestamp < insertMsg.BeginTimestamp {
			insertMsg.BeginTimestamp = timestamp
		}
	}

	return insertMsg, nil
}

func (it *InsertMsg) IsRowBased() bool {
	return it.GetVersion() == msgpb.InsertDataVersion_RowBased
}

func (it *InsertMsg) IsColumnBased() bool {
	return it.GetVersion() == msgpb.InsertDataVersion_ColumnBased
}

func (it *InsertMsg) NRows() uint64 {
	if it.IsRowBased() {
		return uint64(len(it.RowData))
	}
	return it.InsertRequest.GetNumRows()
}

func (it *InsertMsg) CheckAligned() error {
	numRowsOfFieldDataMismatch := func(fieldName string, fieldNumRows, passedNumRows uint64) error {
		return fmt.Errorf("the num_rows(%d) of %sth field is not equal to passed NumRows(%d)", fieldNumRows, fieldName, passedNumRows)
	}
	rowNums := it.NRows()
	if it.IsColumnBased() {
		for _, field := range it.FieldsData {
			fieldNumRows, err := funcutil.GetNumRowOfFieldData(field)
			if err != nil {
				return err
			}
			if fieldNumRows != rowNums {
				return numRowsOfFieldDataMismatch(field.FieldName, fieldNumRows, rowNums)
			}
		}
	}

	if len(it.GetRowIDs()) != len(it.GetTimestamps()) {
		return fmt.Errorf("the num_rows(%d) of rowIDs  is not equal to the num_rows(%d) of timestamps", len(it.GetRowIDs()), len(it.GetTimestamps()))
	}

	if uint64(len(it.GetRowIDs())) != it.NRows() {
		return fmt.Errorf("the num_rows(%d) of rowIDs  is not equal to passed NumRows(%d)", len(it.GetRowIDs()), it.NRows())
	}

	return nil
}

func (it *InsertMsg) rowBasedIndexRequest(index int) msgpb.InsertRequest {
	return msgpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
			commonpbutil.WithMsgID(it.Base.MsgID),
			commonpbutil.WithTimeStamp(it.Timestamps[index]),
			commonpbutil.WithSourceID(it.Base.SourceID),
		),
		DbID:           it.DbID,
		CollectionID:   it.CollectionID,
		PartitionID:    it.PartitionID,
		CollectionName: it.CollectionName,
		PartitionName:  it.PartitionName,
		SegmentID:      it.SegmentID,
		ShardName:      it.ShardName,
		Timestamps:     []uint64{it.Timestamps[index]},
		RowIDs:         []int64{it.RowIDs[index]},
		RowData:        []*commonpb.Blob{it.RowData[index]},
		Version:        msgpb.InsertDataVersion_RowBased,
	}
}

func (it *InsertMsg) columnBasedIndexRequest(index int) msgpb.InsertRequest {
	colNum := len(it.GetFieldsData())
	fieldsData := make([]*schemapb.FieldData, colNum)
	typeutil.AppendFieldData(fieldsData, it.GetFieldsData(), int64(index))
	return msgpb.InsertRequest{
		Base: commonpbutil.NewMsgBase(
			commonpbutil.WithMsgType(commonpb.MsgType_Insert),
			commonpbutil.WithMsgID(it.Base.MsgID),
			commonpbutil.WithTimeStamp(it.Timestamps[index]),
			commonpbutil.WithSourceID(it.Base.SourceID),
		),
		DbID:           it.DbID,
		CollectionID:   it.CollectionID,
		PartitionID:    it.PartitionID,
		CollectionName: it.CollectionName,
		PartitionName:  it.PartitionName,
		SegmentID:      it.SegmentID,
		ShardName:      it.ShardName,
		Timestamps:     []uint64{it.Timestamps[index]},
		RowIDs:         []int64{it.RowIDs[index]},
		FieldsData:     fieldsData,
		NumRows:        1,
		Version:        msgpb.InsertDataVersion_ColumnBased,
	}
}

func (it *InsertMsg) IndexRequest(index int) msgpb.InsertRequest {
	if it.IsRowBased() {
		return it.rowBasedIndexRequest(index)
	}
	return it.columnBasedIndexRequest(index)
}

func (it *InsertMsg) IndexMsg(index int) *InsertMsg {
	return &InsertMsg{
		BaseMsg: BaseMsg{
			Ctx:            it.TraceCtx(),
			BeginTimestamp: it.BeginTimestamp,
			EndTimestamp:   it.EndTimestamp,
			HashValues:     it.HashValues,
			MsgPosition:    it.MsgPosition,
		},
		InsertRequest: it.IndexRequest(index),
	}
}

func (it *InsertMsg) Size() int {
	return proto.Size(&it.InsertRequest)
}

/////////////////////////////////////////Delete//////////////////////////////////////////

// DeleteMsg is a message pack that contains delete request
type DeleteMsg struct {
	BaseMsg
	msgpb.DeleteRequest
}

// interface implementation validation
var _ TsMsg = &DeleteMsg{}

// ID returns the ID of this message pack
func (dt *DeleteMsg) ID() UniqueID {
	return dt.Base.MsgID
}

// SetID set the ID of this message pack
func (dt *DeleteMsg) SetID(id UniqueID) {
	dt.Base.MsgID = id
}

// Type returns the type of this message pack
func (dt *DeleteMsg) Type() MsgType {
	return dt.Base.MsgType
}

// SourceID indicates which component generated this message
func (dt *DeleteMsg) SourceID() int64 {
	return dt.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (dt *DeleteMsg) Marshal(input TsMsg) (MarshalType, error) {
	deleteMsg := input.(*DeleteMsg)
	deleteRequest := &deleteMsg.DeleteRequest
	mb, err := proto.Marshal(deleteRequest)
	if err != nil {
		return nil, err
	}

	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (dt *DeleteMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	deleteRequest := msgpb.DeleteRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &deleteRequest)
	if err != nil {
		return nil, err
	}

	// Compatible with primary keys that only support int64 type
	if deleteRequest.PrimaryKeys == nil {
		deleteRequest.PrimaryKeys = &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: deleteRequest.Int64PrimaryKeys,
				},
			},
		}
		deleteRequest.NumRows = int64(len(deleteRequest.Int64PrimaryKeys))
	}

	deleteMsg := &DeleteMsg{DeleteRequest: deleteRequest}
	for _, timestamp := range deleteMsg.Timestamps {
		deleteMsg.BeginTimestamp = timestamp
		deleteMsg.EndTimestamp = timestamp
		break
	}
	for _, timestamp := range deleteMsg.Timestamps {
		if timestamp > deleteMsg.EndTimestamp {
			deleteMsg.EndTimestamp = timestamp
		}
		if timestamp < deleteMsg.BeginTimestamp {
			deleteMsg.BeginTimestamp = timestamp
		}
	}
	return deleteMsg, nil
}

func (dt *DeleteMsg) CheckAligned() error {
	numRows := dt.GetNumRows()

	if numRows != int64(len(dt.GetTimestamps())) {
		return fmt.Errorf("the num_rows(%d) of pks  is not equal to the num_rows(%d) of timestamps", numRows, len(dt.GetTimestamps()))
	}

	numPks := int64(typeutil.GetSizeOfIDs(dt.PrimaryKeys))
	if numRows != numPks {
		return fmt.Errorf("the num_rows(%d) of pks is not equal to passed NumRows(%d)", numPks, numRows)
	}

	return nil
}

func (dt *DeleteMsg) Size() int {
	return proto.Size(&dt.DeleteRequest)
}

// ///////////////////////////////////////Upsert//////////////////////////////////////////
type UpsertMsg struct {
	InsertMsg *InsertMsg
	DeleteMsg *DeleteMsg
}

/////////////////////////////////////////TimeTick//////////////////////////////////////////

// TimeTickMsg is a message pack that contains time tick only
type TimeTickMsg struct {
	BaseMsg
	msgpb.TimeTickMsg
}

// interface implementation validation
var _ TsMsg = &TimeTickMsg{}

// ID returns the ID of this message pack
func (tst *TimeTickMsg) ID() UniqueID {
	return tst.Base.MsgID
}

// SetID set the ID of this message pack
func (tst *TimeTickMsg) SetID(id UniqueID) {
	tst.Base.MsgID = id
}

// Type returns the type of this message pack
func (tst *TimeTickMsg) Type() MsgType {
	return tst.Base.MsgType
}

// SourceID indicates which component generated this message
func (tst *TimeTickMsg) SourceID() int64 {
	return tst.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (tst *TimeTickMsg) Marshal(input TsMsg) (MarshalType, error) {
	timeTickTask := input.(*TimeTickMsg)
	timeTick := &timeTickTask.TimeTickMsg
	mb, err := proto.Marshal(timeTick)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (tst *TimeTickMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	timeTickMsg := msgpb.TimeTickMsg{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &timeTickMsg)
	if err != nil {
		return nil, err
	}
	timeTick := &TimeTickMsg{TimeTickMsg: timeTickMsg}
	timeTick.BeginTimestamp = timeTick.Base.Timestamp
	timeTick.EndTimestamp = timeTick.Base.Timestamp

	return timeTick, nil
}

func (tst *TimeTickMsg) Size() int {
	return proto.Size(&tst.TimeTickMsg)
}

/////////////////////////////////////////CreateCollection//////////////////////////////////////////

// CreateCollectionMsg is a message pack that contains create collection request
type CreateCollectionMsg struct {
	BaseMsg
	msgpb.CreateCollectionRequest
}

// interface implementation validation
var _ TsMsg = &CreateCollectionMsg{}

// ID returns the ID of this message pack
func (cc *CreateCollectionMsg) ID() UniqueID {
	return cc.Base.MsgID
}

// SetID set the ID of this message pack
func (cc *CreateCollectionMsg) SetID(id UniqueID) {
	cc.Base.MsgID = id
}

// Type returns the type of this message pack
func (cc *CreateCollectionMsg) Type() MsgType {
	return cc.Base.MsgType
}

// SourceID indicates which component generated this message
func (cc *CreateCollectionMsg) SourceID() int64 {
	return cc.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (cc *CreateCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createCollectionMsg := input.(*CreateCollectionMsg)
	createCollectionRequest := &createCollectionMsg.CreateCollectionRequest
	mb, err := proto.Marshal(createCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (cc *CreateCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createCollectionRequest := msgpb.CreateCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createCollectionRequest)
	if err != nil {
		return nil, err
	}
	createCollectionMsg := &CreateCollectionMsg{CreateCollectionRequest: createCollectionRequest}
	createCollectionMsg.BeginTimestamp = createCollectionMsg.Base.Timestamp
	createCollectionMsg.EndTimestamp = createCollectionMsg.Base.Timestamp

	return createCollectionMsg, nil
}

func (cc *CreateCollectionMsg) Size() int {
	return proto.Size(&cc.CreateCollectionRequest)
}

/////////////////////////////////////////DropCollection//////////////////////////////////////////

// DropCollectionMsg is a message pack that contains drop collection request
type DropCollectionMsg struct {
	BaseMsg
	msgpb.DropCollectionRequest
}

// interface implementation validation
var _ TsMsg = &DropCollectionMsg{}

// ID returns the ID of this message pack
func (dc *DropCollectionMsg) ID() UniqueID {
	return dc.Base.MsgID
}

// SetID set the ID of this message pack
func (dc *DropCollectionMsg) SetID(id UniqueID) {
	dc.Base.MsgID = id
}

// Type returns the type of this message pack
func (dc *DropCollectionMsg) Type() MsgType {
	return dc.Base.MsgType
}

// SourceID indicates which component generated this message
func (dc *DropCollectionMsg) SourceID() int64 {
	return dc.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (dc *DropCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropCollectionMsg := input.(*DropCollectionMsg)
	dropCollectionRequest := &dropCollectionMsg.DropCollectionRequest
	mb, err := proto.Marshal(dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (dc *DropCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropCollectionRequest := msgpb.DropCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropCollectionRequest)
	if err != nil {
		return nil, err
	}
	dropCollectionMsg := &DropCollectionMsg{DropCollectionRequest: dropCollectionRequest}
	dropCollectionMsg.BeginTimestamp = dropCollectionMsg.Base.Timestamp
	dropCollectionMsg.EndTimestamp = dropCollectionMsg.Base.Timestamp

	return dropCollectionMsg, nil
}

func (dc *DropCollectionMsg) Size() int {
	return proto.Size(&dc.DropCollectionRequest)
}

/////////////////////////////////////////CreatePartition//////////////////////////////////////////

// CreatePartitionMsg is a message pack that contains create partition request
type CreatePartitionMsg struct {
	BaseMsg
	msgpb.CreatePartitionRequest
}

// interface implementation validation
var _ TsMsg = &CreatePartitionMsg{}

// ID returns the ID of this message pack
func (cp *CreatePartitionMsg) ID() UniqueID {
	return cp.Base.MsgID
}

// SetID set the ID of this message pack
func (cp *CreatePartitionMsg) SetID(id UniqueID) {
	cp.Base.MsgID = id
}

// Type returns the type of this message pack
func (cp *CreatePartitionMsg) Type() MsgType {
	return cp.Base.MsgType
}

// SourceID indicates which component generated this message
func (cp *CreatePartitionMsg) SourceID() int64 {
	return cp.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (cp *CreatePartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	createPartitionMsg := input.(*CreatePartitionMsg)
	createPartitionRequest := &createPartitionMsg.CreatePartitionRequest
	mb, err := proto.Marshal(createPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (cp *CreatePartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	createPartitionRequest := msgpb.CreatePartitionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &createPartitionRequest)
	if err != nil {
		return nil, err
	}
	createPartitionMsg := &CreatePartitionMsg{CreatePartitionRequest: createPartitionRequest}
	createPartitionMsg.BeginTimestamp = createPartitionMsg.Base.Timestamp
	createPartitionMsg.EndTimestamp = createPartitionMsg.Base.Timestamp

	return createPartitionMsg, nil
}

func (cp *CreatePartitionMsg) Size() int {
	return proto.Size(&cp.CreatePartitionRequest)
}

/////////////////////////////////////////DropPartition//////////////////////////////////////////

// DropPartitionMsg is a message pack that contains drop partition request
type DropPartitionMsg struct {
	BaseMsg
	msgpb.DropPartitionRequest
}

// interface implementation validation
var _ TsMsg = &DropPartitionMsg{}

// ID returns the ID of this message pack
func (dp *DropPartitionMsg) ID() UniqueID {
	return dp.Base.MsgID
}

// SetID set the ID of this message pack
func (dp *DropPartitionMsg) SetID(id UniqueID) {
	dp.Base.MsgID = id
}

// Type returns the type of this message pack
func (dp *DropPartitionMsg) Type() MsgType {
	return dp.Base.MsgType
}

// SourceID indicates which component generated this message
func (dp *DropPartitionMsg) SourceID() int64 {
	return dp.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (dp *DropPartitionMsg) Marshal(input TsMsg) (MarshalType, error) {
	dropPartitionMsg := input.(*DropPartitionMsg)
	dropPartitionRequest := &dropPartitionMsg.DropPartitionRequest
	mb, err := proto.Marshal(dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (dp *DropPartitionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	dropPartitionRequest := msgpb.DropPartitionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &dropPartitionRequest)
	if err != nil {
		return nil, err
	}
	dropPartitionMsg := &DropPartitionMsg{DropPartitionRequest: dropPartitionRequest}
	dropPartitionMsg.BeginTimestamp = dropPartitionMsg.Base.Timestamp
	dropPartitionMsg.EndTimestamp = dropPartitionMsg.Base.Timestamp

	return dropPartitionMsg, nil
}

func (dp *DropPartitionMsg) Size() int {
	return proto.Size(&dp.DropPartitionRequest)
}

/////////////////////////////////////////DataNodeTtMsg//////////////////////////////////////////

// DataNodeTtMsg is a message pack that contains datanode time tick
type DataNodeTtMsg struct {
	BaseMsg
	msgpb.DataNodeTtMsg
}

// interface implementation validation
var _ TsMsg = &DataNodeTtMsg{}

// ID returns the ID of this message pack
func (m *DataNodeTtMsg) ID() UniqueID {
	return m.Base.MsgID
}

// SetID set the ID of this message pack
func (m *DataNodeTtMsg) SetID(id UniqueID) {
	m.Base.MsgID = id
}

// Type returns the type of this message pack
func (m *DataNodeTtMsg) Type() MsgType {
	return m.Base.MsgType
}

// SourceID indicates which component generated this message
func (m *DataNodeTtMsg) SourceID() int64 {
	return m.Base.SourceID
}

// Marshal is used to serializing a message pack to byte array
func (m *DataNodeTtMsg) Marshal(input TsMsg) (MarshalType, error) {
	msg := input.(*DataNodeTtMsg)
	t, err := proto.Marshal(&msg.DataNodeTtMsg)
	if err != nil {
		return nil, err
	}
	return t, nil
}

// Unmarshal is used to deserializing a message pack from byte array
func (m *DataNodeTtMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	msg := msgpb.DataNodeTtMsg{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, &msg)
	if err != nil {
		return nil, err
	}
	return &DataNodeTtMsg{
		DataNodeTtMsg: msg,
	}, nil
}

func (m *DataNodeTtMsg) Size() int {
	return proto.Size(&m.DataNodeTtMsg)
}
