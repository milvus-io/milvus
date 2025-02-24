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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/uniquegenerator"
)

func TestCreateAlias_all(t *testing.T) {
	rc := NewRootCoordMock()

	defer rc.Close()
	ctx := context.Background()
	prefix := "TestCreateAlias_all"
	collectionName := prefix + funcutil.GenRandomStr()
	task := &CreateAliasTask{
		Condition: NewTaskCondition(ctx),
		CreateAliasRequest: &milvuspb.CreateAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias1",
		},
		ctx:       ctx,
		result:    merr.Success(),
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_CreateAlias
	assert.Equal(t, commonpb.MsgType_CreateAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	task.CreateAliasRequest.Alias = "illgal-alias:!"
	assert.Error(t, task.PreExecute(ctx))
	task.CreateAliasRequest.Alias = "alias1"
	task.CreateAliasRequest.CollectionName = "illgal-collection:!"
	assert.Error(t, task.PreExecute(ctx))
	task.CreateAliasRequest.CollectionName = collectionName

	assert.NoError(t, task.PreExecute(ctx))
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func TestDropAlias_all(t *testing.T) {
	rc := NewRootCoordMock()

	defer rc.Close()
	ctx := context.Background()
	task := &DropAliasTask{
		Condition: NewTaskCondition(ctx),
		DropAliasRequest: &milvuspb.DropAliasRequest{
			Base:  nil,
			Alias: "alias1",
		},
		ctx:       ctx,
		result:    merr.Success(),
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())
	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_DropAlias
	assert.Equal(t, commonpb.MsgType_DropAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func TestAlterAlias_all(t *testing.T) {
	rc := NewRootCoordMock()

	defer rc.Close()
	ctx := context.Background()
	prefix := "TestAlterAlias_all"
	collectionName := prefix + funcutil.GenRandomStr()
	task := &AlterAliasTask{
		Condition: NewTaskCondition(ctx),
		AlterAliasRequest: &milvuspb.AlterAliasRequest{
			Base:           nil,
			CollectionName: collectionName,
			Alias:          "alias1",
		},
		ctx:       ctx,
		result:    merr.Success(),
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_AlterAlias
	assert.Equal(t, commonpb.MsgType_AlterAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	task.AlterAliasRequest.Alias = "illgal-alias:!"
	assert.Error(t, task.PreExecute(ctx))
	task.AlterAliasRequest.Alias = "alias1"
	task.AlterAliasRequest.CollectionName = "illgal-collection:!"
	assert.Error(t, task.PreExecute(ctx))
	task.AlterAliasRequest.CollectionName = collectionName

	assert.NoError(t, task.PreExecute(ctx))
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func TestDescribeAlias_all(t *testing.T) {
	rc := NewRootCoordMock()

	defer rc.Close()
	ctx := context.Background()
	task := &DescribeAliasTask{
		Condition: NewTaskCondition(ctx),
		DescribeAliasRequest: &milvuspb.DescribeAliasRequest{
			Base:  nil,
			Alias: "alias1",
		},
		ctx: ctx,
		result: &milvuspb.DescribeAliasResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_DescribeAlias
	assert.Equal(t, commonpb.MsgType_DescribeAlias, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.Error(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}

func TestListAliases_all(t *testing.T) {
	rc := NewRootCoordMock()

	defer rc.Close()
	ctx := context.Background()
	task := &ListAliasesTask{
		Condition: NewTaskCondition(ctx),
		ListAliasesRequest: &milvuspb.ListAliasesRequest{
			Base: nil,
		},
		ctx: ctx,
		result: &milvuspb.ListAliasesResponse{
			Status: &commonpb.Status{
				ErrorCode: commonpb.ErrorCode_Success,
			},
		},
		rootCoord: rc,
	}

	assert.NoError(t, task.OnEnqueue())

	assert.NotNil(t, task.TraceCtx())

	id := UniqueID(uniquegenerator.GetUniqueIntGeneratorIns().GetInt())
	task.SetID(id)
	assert.Equal(t, id, task.ID())

	task.Base.MsgType = commonpb.MsgType_ListAliases
	assert.Equal(t, commonpb.MsgType_ListAliases, task.Type())
	ts := Timestamp(time.Now().UnixNano())
	task.SetTs(ts)
	assert.Equal(t, ts, task.BeginTs())
	assert.Equal(t, ts, task.EndTs())

	assert.NoError(t, task.PreExecute(ctx))
	assert.NoError(t, task.Execute(ctx))
	assert.NoError(t, task.PostExecute(ctx))
}
