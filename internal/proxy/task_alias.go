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
	"github.com/milvus-io/milvus/pkg/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// CreateAliasTask contains task information of CreateAlias
type CreateAliasTask struct {
	Condition
	*milvuspb.CreateAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

// TraceCtx returns the trace context of the task.
func (c *CreateAliasTask) TraceCtx() context.Context {
	return c.ctx
}

// ID return the id of the task
func (c *CreateAliasTask) ID() UniqueID {
	return c.Base.MsgID
}

// SetID sets the id of the task
func (c *CreateAliasTask) SetID(uid UniqueID) {
	c.Base.MsgID = uid
}

// Name returns the name of the task
func (c *CreateAliasTask) Name() string {
	return CreateAliasTaskName
}

// Type returns the type of the task
func (c *CreateAliasTask) Type() commonpb.MsgType {
	return c.Base.MsgType
}

// BeginTs returns the ts
func (c *CreateAliasTask) BeginTs() Timestamp {
	return c.Base.Timestamp
}

// EndTs returns the ts
func (c *CreateAliasTask) EndTs() Timestamp {
	return c.Base.Timestamp
}

// SetTs sets the ts
func (c *CreateAliasTask) SetTs(ts Timestamp) {
	c.Base.Timestamp = ts
}

// OnEnqueue defines the behavior task enqueued
func (c *CreateAliasTask) OnEnqueue() error {
	c.Base = commonpbutil.NewMsgBase()
	return nil
}

// PreExecute defines the action before task execution
func (c *CreateAliasTask) PreExecute(ctx context.Context) error {
	c.Base.MsgType = commonpb.MsgType_CreateAlias
	c.Base.SourceID = paramtable.GetNodeID()

	collAlias := c.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := c.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}
	return nil
}

// Execute defines the actual execution of create alias
func (c *CreateAliasTask) Execute(ctx context.Context) error {
	var err error
	c.result, err = c.rootCoord.CreateAlias(ctx, c.CreateAliasRequest)
	return err
}

// PostExecute defines the post execution, do nothing for create alias
func (c *CreateAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DropAliasTask is the task to drop alias
type DropAliasTask struct {
	Condition
	*milvuspb.DropAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

// TraceCtx returns the context for trace
func (d *DropAliasTask) TraceCtx() context.Context {
	return d.ctx
}

// ID returns the MsgID
func (d *DropAliasTask) ID() UniqueID {
	return d.Base.MsgID
}

// SetID sets the MsgID
func (d *DropAliasTask) SetID(uid UniqueID) {
	d.Base.MsgID = uid
}

// Name returns the name of the task
func (d *DropAliasTask) Name() string {
	return DropAliasTaskName
}

func (d *DropAliasTask) Type() commonpb.MsgType {
	return d.Base.MsgType
}

func (d *DropAliasTask) BeginTs() Timestamp {
	return d.Base.Timestamp
}

func (d *DropAliasTask) EndTs() Timestamp {
	return d.Base.Timestamp
}

func (d *DropAliasTask) SetTs(ts Timestamp) {
	d.Base.Timestamp = ts
}

func (d *DropAliasTask) OnEnqueue() error {
	d.Base = commonpbutil.NewMsgBase()
	return nil
}

func (d *DropAliasTask) PreExecute(ctx context.Context) error {
	d.Base.MsgType = commonpb.MsgType_DropAlias
	d.Base.SourceID = paramtable.GetNodeID()
	collAlias := d.Alias
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}
	return nil
}

func (d *DropAliasTask) Execute(ctx context.Context) error {
	var err error
	d.result, err = d.rootCoord.DropAlias(ctx, d.DropAliasRequest)
	return err
}

func (d *DropAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// AlterAliasTask is the task to alter alias
type AlterAliasTask struct {
	Condition
	*milvuspb.AlterAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
	result    *commonpb.Status
}

func (a *AlterAliasTask) TraceCtx() context.Context {
	return a.ctx
}

func (a *AlterAliasTask) ID() UniqueID {
	return a.Base.MsgID
}

func (a *AlterAliasTask) SetID(uid UniqueID) {
	a.Base.MsgID = uid
}

func (a *AlterAliasTask) Name() string {
	return AlterAliasTaskName
}

func (a *AlterAliasTask) Type() commonpb.MsgType {
	return a.Base.MsgType
}

func (a *AlterAliasTask) BeginTs() Timestamp {
	return a.Base.Timestamp
}

func (a *AlterAliasTask) EndTs() Timestamp {
	return a.Base.Timestamp
}

func (a *AlterAliasTask) SetTs(ts Timestamp) {
	a.Base.Timestamp = ts
}

func (a *AlterAliasTask) OnEnqueue() error {
	a.Base = commonpbutil.NewMsgBase()
	return nil
}

func (a *AlterAliasTask) PreExecute(ctx context.Context) error {
	a.Base.MsgType = commonpb.MsgType_AlterAlias
	a.Base.SourceID = paramtable.GetNodeID()

	collAlias := a.Alias
	// collection alias uses the same format as collection name
	if err := ValidateCollectionAlias(collAlias); err != nil {
		return err
	}

	collName := a.CollectionName
	if err := validateCollectionName(collName); err != nil {
		return err
	}

	return nil
}

func (a *AlterAliasTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.AlterAlias(ctx, a.AlterAliasRequest)
	return err
}

func (a *AlterAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// DescribeAliasTask is the task to describe alias
type DescribeAliasTask struct {
	Condition
	nodeID UniqueID
	*milvuspb.DescribeAliasRequest
	ctx       context.Context
	rootCoord types.RootCoord
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
	return nil
}

func (a *DescribeAliasTask) PreExecute(ctx context.Context) error {
	a.Base.MsgType = commonpb.MsgType_DescribeAlias
	a.Base.SourceID = a.nodeID
	return nil
}

func (a *DescribeAliasTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.DescribeAlias(ctx, a.DescribeAliasRequest)
	return err
}

func (a *DescribeAliasTask) PostExecute(ctx context.Context) error {
	return nil
}

// ListAliasesTask is the task to list aliases
type ListAliasesTask struct {
	Condition
	nodeID UniqueID
	*milvuspb.ListAliasesRequest
	ctx       context.Context
	rootCoord types.RootCoord
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
	return nil
}

func (a *ListAliasesTask) PreExecute(ctx context.Context) error {
	a.Base.MsgType = commonpb.MsgType_ListAliases
	a.Base.SourceID = a.nodeID
	return nil
}

func (a *ListAliasesTask) Execute(ctx context.Context) error {
	var err error
	a.result, err = a.rootCoord.ListAliases(ctx, a.ListAliasesRequest)
	return err
}

func (a *ListAliasesTask) PostExecute(ctx context.Context) error {
	return nil
}
