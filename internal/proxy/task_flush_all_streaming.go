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
	"sync"

	"github.com/samber/lo"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func (t *flushAllTask) Execute(ctx context.Context) error {
	flushTs := t.BeginTs()
	timeOfSeal, _ := tsoutil.ParseTS(flushTs)

	// Note: for now, flush will send flush signal to wal on streamnode, then get flush segment list from datacoord
	// so we need to expand flush collection names to make sure that flushed collection list is same as each other
	targets, err := t.expandFlushCollectionNames(ctx)
	if err != nil {
		return err
	}

	// send flush signal to wal on streamnode
	onFlushSegmentMap, err := t.sendManualFlushAllToWal(ctx, targets, flushTs)
	if err != nil {
		return err
	}

	// get flush detail info from datacoord
	resp, err := t.mixCoord.FlushAll(ctx, &datapb.FlushAllRequest{
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
				onFlushSegmentIDs := onFlushSegmentMap[result.GetCollectionID()]
				// Remove the flushed segments from onFlushSegmentIDs
				flushedSegmentSet := typeutil.NewUniqueSet(result.GetFlushSegmentIDs()...)
				filteredSegments := make([]int64, 0, len(onFlushSegmentIDs))
				for _, id := range onFlushSegmentIDs {
					if !flushedSegmentSet.Contain(id) {
						filteredSegments = append(filteredSegments, id)
					}
				}
				onFlushSegmentIDs = filteredSegments
				return &milvuspb.FlushCollectionResult{
					CollectionName:  result.GetCollectionName(),
					SegmentIds:      &schemapb.LongArray{Data: onFlushSegmentIDs},
					FlushSegmentIds: &schemapb.LongArray{Data: result.GetFlushSegmentIDs()},
					SealTime:        timeOfSeal.Unix(),
					FlushTs:         flushTs,
					ChannelCps:      result.GetChannelCps(),
				}
			}),
		})
	}

	t.result = &milvuspb.FlushAllResponse{
		Status:       merr.Success(),
		FlushAllTs:   flushTs,
		FlushResults: results,
	}
	return nil
}

// todo: refine this by sending a single FlushAll message to wal
func (t *flushAllTask) sendManualFlushAllToWal(ctx context.Context, flushTargets []*datapb.FlushAllTarget, flushTs Timestamp) (map[int64][]int64, error) {
	wg := errgroup.Group{}
	// limit goroutine number to 100
	wg.SetLimit(100)

	var mu sync.Mutex
	results := make(map[int64][]int64)

	for _, target := range flushTargets {
		for _, coll := range target.CollectionIds {
			collID := coll
			wg.Go(func() error {
				vchannels, err := t.chMgr.getVChannels(collID)
				if err != nil {
					return err
				}

				onFlushSegmentIDs := make([]int64, 0)
				// Ask the streamingnode to flush segments.
				for _, vchannel := range vchannels {
					segmentIDs, err := sendManualFlushToWAL(ctx, collID, vchannel, flushTs)
					if err != nil {
						return err
					}
					onFlushSegmentIDs = append(onFlushSegmentIDs, segmentIDs...)
				}
				mu.Lock()
				results[collID] = onFlushSegmentIDs
				mu.Unlock()
				return nil
			})
		}
	}

	err := wg.Wait()
	if err != nil {
		return nil, err
	}

	return results, nil
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
		listResp, err := t.mixCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
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
			showColRsp, err := t.mixCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
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
