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
	"fmt"

	"github.com/samber/lo"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type flushAllTask struct {
	baseTask
	Condition
	*milvuspb.FlushAllRequest
	ctx       context.Context
	rootCoord types.RootCoordClient
	dataCoord types.DataCoordClient
	result    *milvuspb.FlushAllResponse

	replicateMsgStream msgstream.MsgStream
}

func (t *flushAllTask) TraceCtx() context.Context {
	return t.ctx
}

func (t *flushAllTask) ID() UniqueID {
	return t.Base.MsgID
}

func (t *flushAllTask) SetID(uid UniqueID) {
	t.Base.MsgID = uid
}

func (t *flushAllTask) Name() string {
	return FlushAllTaskName
}

func (t *flushAllTask) Type() commonpb.MsgType {
	return t.Base.MsgType
}

func (t *flushAllTask) BeginTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushAllTask) EndTs() Timestamp {
	return t.Base.Timestamp
}

func (t *flushAllTask) SetTs(ts Timestamp) {
	t.Base.Timestamp = ts
}

func (t *flushAllTask) OnEnqueue() error {
	if t.Base == nil {
		t.Base = commonpbutil.NewMsgBase()
	}
	t.Base.MsgType = commonpb.MsgType_Flush
	t.Base.SourceID = paramtable.GetNodeID()
	return nil
}

func (t *flushAllTask) PreExecute(ctx context.Context) error {
	return nil
}

func (t *flushAllTask) Execute(ctx context.Context) error {
	targets, err := t.expandFlushCollectionNames(ctx)
	if err != nil {
		return err
	}

	// get flush detail info from datacoord
	resp, err := t.dataCoord.FlushAll(ctx, &datapb.FlushAllRequest{
		Base:         commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_Flush)),
		DbName:       t.GetDbName(),
		FlushTargets: targets,
	})
	if err = merr.CheckRPCCall(resp, err); err != nil {
		return fmt.Errorf("failed to call flush all to data coordinator: %s", err.Error())
	}

	dbResultsMap := lo.GroupBy(resp.GetFlushResults(), func(result *datapb.FlushResult) string {
		return result.GetDbName()
	})
	results := make([]*milvuspb.FlushAllResult, 0)
	for dbName, dbResults := range dbResultsMap {
		results = append(results, &milvuspb.FlushAllResult{
			DbName: dbName,
			CollectionResults: lo.Map(dbResults, func(result *datapb.FlushResult, _ int) *milvuspb.FlushCollectionResult {
				return &milvuspb.FlushCollectionResult{
					CollectionName:  result.GetCollectionName(),
					SegmentIds:      &schemapb.LongArray{Data: result.GetSegmentIDs()},
					FlushSegmentIds: &schemapb.LongArray{Data: result.GetFlushSegmentIDs()},
					SealTime:        result.GetTimeOfSeal(),
					FlushTs:         result.GetFlushTs(),
					ChannelCps:      result.GetChannelCps(),
				}
			}),
		})
	}

	t.result = &milvuspb.FlushAllResponse{
		Status:       merr.Success(),
		FlushAllTs:   resp.GetFlushTs(),
		FlushResults: results,
	}
	return nil
}

func (t *flushAllTask) PostExecute(ctx context.Context) error {
	return nil
}

func (t *flushAllTask) expandFlushCollectionNames(ctx context.Context) ([]*datapb.FlushAllTarget, error) {
	// Determine which databases and collections to flush
	targets := make([]*datapb.FlushAllTarget, 0)
	if len(t.GetFlushTargets()) > 0 {
		// Use flush_targets from request
		for _, target := range t.GetFlushTargets() {
			collectionIDs := make([]int64, 0)
			for _, collectionName := range target.GetCollectionNames() {
				collectionID, err := globalMetaCache.GetCollectionID(ctx, target.GetDbName(), collectionName)
				if err != nil {
					return nil, err
				}
				collectionIDs = append(collectionIDs, collectionID)
			}
			targets = append(targets, &datapb.FlushAllTarget{
				DbName:        target.GetDbName(),
				CollectionIds: collectionIDs,
			})
		}
	} else if t.GetDbName() != "" {
		// Backward compatibility: use deprecated db_name field
		targets = append(targets, &datapb.FlushAllTarget{
			DbName:        t.GetDbName(),
			CollectionIds: []int64{},
		})
	} else {
		// Flush all databases
		listResp, err := t.rootCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
			Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
		})
		if err != nil {
			log.Info("flush all task by streaming service failed, list databases failed", zap.Error(err))
			return nil, err
		}
		for _, dbName := range listResp.GetDbNames() {
			targets = append(targets, &datapb.FlushAllTarget{
				DbName:        dbName,
				CollectionIds: []int64{},
			})
		}
	}

	// If CollectionNames is empty, it means flush all collections in this database
	for _, target := range targets {
		collectionNames := target.GetCollectionIds()
		if len(collectionNames) == 0 {
			showColRsp, err := t.rootCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
				Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections)),
				DbName: target.GetDbName(),
			})
			if err != nil {
				return nil, err
			}
			target.CollectionIds = showColRsp.GetCollectionIds()
		}
	}
	return targets, nil
}
