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

package proxy

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// CreateAliasTask contains task information of CreateAlias
type CreateAliasTask struct {
	baseTask
	Condition
	*milvuspb.CreateAliasRequest

	ctx                context.Context
	rootCoord          types.RootCoordClient
	replicateMsgStream msgstream.MsgStream
	result             *commonpb.Status
}

// TraceCtx returns the trace context of the task.
func (t *CreateAliasTask) TraceCtx() context.Context {
	return t.ctx
}

// ID return the id of the task
func (t *CreateAliasTask) ID() UniqueID {
	return t.Base.MsgID
}

// SetID sets the id of the task
func (t *CreateAliasTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

// Name returns the name of the task
func (t *CreateAliasTask) Name() string {
	return CreateAliasTaskName
}

// Type returns the type of the task
func (t *CreateAliasTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

// BeginTs returns the ts
func (t *CreateAliasTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

// EndTs returns the ts
func (t *CreateAliasTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

// SetTs sets the ts
func (t *CreateAliasTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

// OnEnqueue defines the behavior task enqueued
func (t *CreateAliasTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_CreateAlias
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

// PreExecute defines the tion before task execution
func (t *CreateAliasTask) PreExecute(ctx context.Context) error {
	collAlias := t.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := t.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}
	return nil
}

// Execute defines the tual execution of create alias
func (t *CreateAliasTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.CreateAlias(ctx, t.CreateAliasRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.CreateAliasRequest)
	return nil
}

// PostExecute defines the post execution, do nothing for create alias
func (t *CreateAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DropAliasTask is the task to drop alias
type DropAliasTask struct {
	baseTask
	Condition
	*milvuspb.DropAliasRequest

	ctx                context.Context
	rootCoord          types.RootCoordClient
	replicateMsgStream msgstream.MsgStream
	result             *commonpb.Status
}

// TraceCtx returns the context for trace
func (t *DropAliasTask) TraceCtx() context.Context {
	return t.ctx
}

// ID returns the MsgID
func (t *DropAliasTask) ID() UniqueID {
	return t.Base.MsgID
}

// SetID sets the MsgID
func (t *DropAliasTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

// Name returns the name of the task
func (t *DropAliasTask) Name() string {
	return DropAliasTaskName
}

func (t *DropAliasTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *DropAliasTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DropAliasTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *DropAliasTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *DropAliasTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_DropAlias
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *DropAliasTask) PreExecute(ctx context.Context) error {
	collAlias := t.Alias
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}
	return nil
}

func (t *DropAliasTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.DropAlias(ctx, t.DropAliasRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.DropAliasRequest)
	return nil
}

func (t *DropAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// AlterAliasTask is the task to alter alias
type AlterAliasTask struct {
	baseTask
	Condition
	*milvuspb.AlterAliasRequest

	ctx                context.Context
	rootCoord          types.RootCoordClient
	replicateMsgStream msgstream.MsgStream
	result             *commonpb.Status
}

func (t *AlterAliasTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *AlterAliasTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *AlterAliasTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *AlterAliasTask) Name() string {
	return AlterAliasTaskName
}

func (t *AlterAliasTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *AlterAliasTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *AlterAliasTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *AlterAliasTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *AlterAliasTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_AlterAlias
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *AlterAliasTask) PreExecute(ctx context.Context) error {
	collAlias := t.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := t.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (t *AlterAliasTask) Execute(ctx context.Context) error {
	var err error
	t.result, err = t.rootCoord.AlterAlias(ctx, t.AlterAliasRequest)
	if err = merr.CheckRPCCall(t.result, err); err != nil {
		return err
	}
	SendReplicateMessagePack(ctx, t.replicateMsgStream, t.AlterAliasRequest)
	return nil
}

func (t *AlterAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DescribeAliasTask is the task to describe alias
type DescribeAliasTask struct {
	baseTask
	Condition
	nodeID UniqueID
	*milvuspb.DescribeAliasRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.DescribeAliasResponse
}

func (a *DescribeAliasTask) TraceCtx() context.Context {
	return a.ctx
}

func (a *DescribeAliasTask) ID() UniqueID {
	return a.Base.MsgID
}

func (a *DescribeAliasTask) SetID(uid UniqueID) {
	a.Base.MsgID = uid
}

func (a *DescribeAliasTask) Name() string {
	return DescribeAliasTaskName
}

func (a *DescribeAliasTask) Type() commonpb.MsgType {
	return a.Base.MsgType
}

func (a *DescribeAliasTask) BeginTs() Timestamp {
	return a.Base.Timestamp
}

func (a *DescribeAliasTask) EndTs() Timestamp {
	return a.Base.Timestamp
}

func (a *DescribeAliasTask) SetTs(ts Timestamp) {
	a.Base.Timestamp = ts
}

func (a *DescribeAliasTask) OnEnqueue() error {
	a.Base = commonpbutil.NewMsgBase()
	a.Base.MsgType = commonpb.MsgType_DescribeAlias
	a.Base.SourceID = a.nodeID
	return nil
}

func (a *DescribeAliasTask) PreExecute(ctx context.Context) error {
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(a.GetAlias()); err != nil {
		return err
	}
	return nil
}

func (a *DescribeAliasTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.DescribeAlias(ctx, a.DescribeAliasRequest)
	return merr.CheckRPCCall(a.result, err)
}

func (a *DescribeAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// ListAliasesTask is the task to list aliases
type ListAliasesTask struct {
	baseTask
	Condition
	nodeID UniqueID
	*milvuspb.ListAliasesRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	result    *milvuspb.ListAliasesResponse
}

func (a *ListAliasesTask) TraceCtx() context.Context {
	return a.ctx
}

func (a *ListAliasesTask) ID() UniqueID {
	return a.Base.MsgID
}

func (a *ListAliasesTask) SetID(uid UniqueID) {
	a.Base.MsgID = uid
}

func (a *ListAliasesTask) Name() string {
	return ListAliasesTaskName
}

func (a *ListAliasesTask) Type() commonpb.MsgType {
	return a.Base.MsgType
}

func (a *ListAliasesTask) BeginTs() Timestamp {
	return a.Base.Timestamp
}

func (a *ListAliasesTask) EndTs() Timestamp {
	return a.Base.Timestamp
}

func (a *ListAliasesTask) SetTs(ts Timestamp) {
	a.Base.Timestamp = ts
}

func (a *ListAliasesTask) OnEnqueue() error {
	a.Base = commonpbutil.NewMsgBase()
	a.Base.MsgType = commonpb.MsgType_ListAliases
	a.Base.SourceID = a.nodeID
	return nil
}

func (a *ListAliasesTask) PreExecute(ctx context.Context) error {
	if len(a.GetCollectionName()) > 0 {
		if err := validateCollectionName(a.GetCollectionName()); err != nil {
			return err
		}
	}
	return nil
}

func (a *ListAliasesTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.ListAliases(ctx, a.ListAliasesRequest)
	return merr.CheckRPCCall(a.result, err)
}

func (a *ListAliasesTask) PostExecute(ctx context.Context) error {
	return nil
}
