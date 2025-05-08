/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package msgstream

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
)

// LoadCollectionMsg is a message pack that contains load collection request
type LoadCollectionMsg struct {
	BaseMsg
	*milvuspb.LoadCollectionRequest
}

// interface implementation validation
var _ TsMsg = &LoadCollectionMsg{}

func (l *LoadCollectionMsg) ID() UniqueID {
	return l.Base.MsgID
}

func (l *LoadCollectionMsg) SetID(id UniqueID) {
	l.Base.MsgID = id
}

func (l *LoadCollectionMsg) Type() MsgType {
	return l.Base.MsgType
}

func (l *LoadCollectionMsg) SourceID() int64 {
	return l.Base.SourceID
}

func (l *LoadCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	loadCollectionMsg := input.(*LoadCollectionMsg)
	loadCollectionRequest := loadCollectionMsg.LoadCollectionRequest
	mb, err := proto.Marshal(loadCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (l *LoadCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	loadCollectionRequest := &milvuspb.LoadCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, loadCollectionRequest)
	if err != nil {
		return nil, err
	}
	loadCollectionMsg := &LoadCollectionMsg{LoadCollectionRequest: loadCollectionRequest}
	loadCollectionMsg.BeginTimestamp = loadCollectionMsg.GetBase().GetTimestamp()
	loadCollectionMsg.EndTimestamp = loadCollectionMsg.GetBase().GetTimestamp()

	return loadCollectionMsg, nil
}

func (l *LoadCollectionMsg) Size() int {
	return proto.Size(l.LoadCollectionRequest)
}

// ReleaseCollectionMsg is a message pack that contains release collection request
type ReleaseCollectionMsg struct {
	BaseMsg
	*milvuspb.ReleaseCollectionRequest
}

var _ TsMsg = &ReleaseCollectionMsg{}

func (r *ReleaseCollectionMsg) ID() UniqueID {
	return r.Base.MsgID
}

func (r *ReleaseCollectionMsg) SetID(id UniqueID) {
	r.Base.MsgID = id
}

func (r *ReleaseCollectionMsg) Type() MsgType {
	return r.Base.MsgType
}

func (r *ReleaseCollectionMsg) SourceID() int64 {
	return r.Base.SourceID
}

func (r *ReleaseCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	releaseCollectionMsg := input.(*ReleaseCollectionMsg)
	releaseCollectionRequest := releaseCollectionMsg.ReleaseCollectionRequest
	mb, err := proto.Marshal(releaseCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (r *ReleaseCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	releaseCollectionRequest := &milvuspb.ReleaseCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, releaseCollectionRequest)
	if err != nil {
		return nil, err
	}
	releaseCollectionMsg := &ReleaseCollectionMsg{ReleaseCollectionRequest: releaseCollectionRequest}
	releaseCollectionMsg.BeginTimestamp = releaseCollectionMsg.GetBase().GetTimestamp()
	releaseCollectionMsg.EndTimestamp = releaseCollectionMsg.GetBase().GetTimestamp()

	return releaseCollectionMsg, nil
}

func (r *ReleaseCollectionMsg) Size() int {
	return proto.Size(r.ReleaseCollectionRequest)
}

type FlushMsg struct {
	BaseMsg
	*milvuspb.FlushRequest
}

var _ TsMsg = &FlushMsg{}

func (f *FlushMsg) ID() UniqueID {
	return f.Base.MsgID
}

func (f *FlushMsg) SetID(id UniqueID) {
	f.Base.MsgID = id
}

func (f *FlushMsg) Type() MsgType {
	return f.Base.MsgType
}

func (f *FlushMsg) SourceID() int64 {
	return f.Base.SourceID
}

func (f *FlushMsg) Marshal(input TsMsg) (MarshalType, error) {
	flushMsg := input.(*FlushMsg)
	flushRequest := flushMsg.FlushRequest
	mb, err := proto.Marshal(flushRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (f *FlushMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	flushRequest := &milvuspb.FlushRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, flushRequest)
	if err != nil {
		return nil, err
	}
	flushMsg := &FlushMsg{FlushRequest: flushRequest}
	flushMsg.BeginTimestamp = flushMsg.GetBase().GetTimestamp()
	flushMsg.EndTimestamp = flushMsg.GetBase().GetTimestamp()

	return flushMsg, nil
}

func (f *FlushMsg) Size() int {
	return proto.Size(f.FlushRequest)
}

type AlterCollectionMsg struct {
	BaseMsg
	*milvuspb.AlterCollectionRequest
}

var _ TsMsg = &AlterCollectionMsg{}

func (a *AlterCollectionMsg) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterCollectionMsg) SetID(id UniqueID) {
	a.Base.MsgID = id
}

func (a *AlterCollectionMsg) Type() MsgType {
	return a.Base.MsgType
}

func (a *AlterCollectionMsg) SourceID() int64 {
	return a.Base.SourceID
}

func (a *AlterCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	alterCollectionMsg := input.(*AlterCollectionMsg)
	alterCollectionRequest := alterCollectionMsg.AlterCollectionRequest
	mb, err := proto.Marshal(alterCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (a *AlterCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	alterCollectionRequest := &milvuspb.AlterCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, alterCollectionRequest)
	if err != nil {
		return nil, err
	}
	alterCollectionMsg := &AlterCollectionMsg{AlterCollectionRequest: alterCollectionRequest}
	alterCollectionMsg.BeginTimestamp = alterCollectionMsg.GetBase().GetTimestamp()
	alterCollectionMsg.EndTimestamp = alterCollectionMsg.GetBase().GetTimestamp()

	return alterCollectionMsg, nil
}

func (a *AlterCollectionMsg) Size() int {
	return proto.Size(a.AlterCollectionRequest)
}

type AlterCollectionFieldMsg struct {
	BaseMsg
	*milvuspb.AlterCollectionFieldRequest
}

var _ TsMsg = &AlterCollectionFieldMsg{}

func (a *AlterCollectionFieldMsg) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterCollectionFieldMsg) SetID(id UniqueID) {
	a.Base.MsgID = id
}

func (a *AlterCollectionFieldMsg) Type() MsgType {
	return a.Base.MsgType
}

func (a *AlterCollectionFieldMsg) SourceID() int64 {
	return a.Base.SourceID
}

func (a *AlterCollectionFieldMsg) Marshal(input TsMsg) (MarshalType, error) {
	alterCollectionFieldMsg := input.(*AlterCollectionFieldMsg)
	alterCollectionFieldRequest := alterCollectionFieldMsg.AlterCollectionFieldRequest
	mb, err := proto.Marshal(alterCollectionFieldRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (a *AlterCollectionFieldMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	alterCollectionFieldRequest := &milvuspb.AlterCollectionFieldRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, alterCollectionFieldRequest)
	if err != nil {
		return nil, err
	}
	alterCollectionFieldMsg := &AlterCollectionFieldMsg{AlterCollectionFieldRequest: alterCollectionFieldRequest}
	alterCollectionFieldMsg.BeginTimestamp = alterCollectionFieldMsg.GetBase().GetTimestamp()
	alterCollectionFieldMsg.EndTimestamp = alterCollectionFieldMsg.GetBase().GetTimestamp()

	return alterCollectionFieldMsg, nil
}

func (a *AlterCollectionFieldMsg) Size() int {
	return proto.Size(a.AlterCollectionFieldRequest)
}

// TODO fubang maybe it will break the cdc replication
type RenameCollectionMsg struct {
	BaseMsg
	*milvuspb.RenameCollectionRequest
}

var _ TsMsg = &RenameCollectionMsg{}

func (r *RenameCollectionMsg) ID() UniqueID {
	return r.Base.MsgID
}

func (r *RenameCollectionMsg) SetID(id UniqueID) {
	r.Base.MsgID = id
}

func (r *RenameCollectionMsg) Type() MsgType {
	return r.Base.MsgType
}

func (r *RenameCollectionMsg) SourceID() int64 {
	return r.Base.SourceID
}

func (r *RenameCollectionMsg) Marshal(input TsMsg) (MarshalType, error) {
	renameCollectionMsg := input.(*RenameCollectionMsg)
	renameCollectionRequest := renameCollectionMsg.RenameCollectionRequest
	mb, err := proto.Marshal(renameCollectionRequest)
	if err != nil {
		return nil, err
	}
	return mb, nil
}

func (r *RenameCollectionMsg) Unmarshal(input MarshalType) (TsMsg, error) {
	renameCollectionRequest := &milvuspb.RenameCollectionRequest{}
	in, err := convertToByteArray(input)
	if err != nil {
		return nil, err
	}
	err = proto.Unmarshal(in, renameCollectionRequest)
	if err != nil {
		return nil, err
	}
	renameCollectionMsg := &RenameCollectionMsg{RenameCollectionRequest: renameCollectionRequest}
	renameCollectionMsg.BeginTimestamp = renameCollectionMsg.GetBase().GetTimestamp()
	renameCollectionMsg.EndTimestamp = renameCollectionMsg.GetBase().GetTimestamp()

	return renameCollectionMsg, nil
}

func (r *RenameCollectionMsg) Size() int {
	return proto.Size(r.RenameCollectionRequest)
}
