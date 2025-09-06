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

	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func (t *flushAllTask) Execute(ctx context.Context) error {
	// Determine which databases and collections to flush
	type flushTarget struct {
		dbName          string
		collectionNames []string // empty means all collections
	}
	targets := make([]flushTarget, 0)

	if len(t.GetFlushTargets()) > 0 {
		// Use flush_targets from request
		for _, target := range t.GetFlushTargets() {
			targets = append(targets, flushTarget{
				dbName:          target.GetDbName(),
				collectionNames: target.GetCollectionNames(),
			})
		}
	} else if t.GetDbName() != "" {
		// Backward compatibility: use deprecated db_name field
		targets = append(targets, flushTarget{
			dbName:          t.GetDbName(),
			collectionNames: []string{}, // flush all collections
		})
	} else {
		// Flush all databases
		listResp, err := t.mixCoord.ListDatabases(ctx, &milvuspb.ListDatabasesRequest{
			Base: commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ListDatabases)),
		})
		if err != nil {
			log.Info("flush all task by streaming service failed, list databases failed", zap.Error(err))
			return err
		}
		for _, dbName := range listResp.GetDbNames() {
			targets = append(targets, flushTarget{
				dbName:          dbName,
				collectionNames: []string{}, // flush all collections
			})
		}
	}

	flushTs := t.BeginTs()
	timeOfSeal, _ := tsoutil.ParseTS(flushTs)

	// Results aggregation by database
	dbResultsMap := make(map[string]*milvuspb.FlushAllResult)

	wg := errgroup.Group{}
	// limit goroutine number to 100
	wg.SetLimit(100)

	// Channel for collecting results from goroutines
	type collectionFlushResult struct {
		dbName          string
		collName        string
		collID          UniqueID
		segmentIDs      []int64
		flushSegmentIDs []int64
		channelCps      map[string]*msgpb.MsgPosition
	}
	resultChan := make(chan collectionFlushResult, 1000)

	for _, target := range targets {
		dbName := target.dbName
		targetCollections := target.collectionNames

		// Get all collections in this database
		showColRsp, err := t.mixCoord.ShowCollections(ctx, &milvuspb.ShowCollectionsRequest{
			Base:   commonpbutil.NewMsgBase(commonpbutil.WithMsgType(commonpb.MsgType_ShowCollections)),
			DbName: dbName,
		})
		if err := merr.CheckRPCCall(showColRsp, err); err != nil {
			log.Info("flush all task by streaming service failed, show collections failed", zap.String("dbName", dbName), zap.Error(err))
			return err
		}

		collections := showColRsp.GetCollectionNames()
		// Filter collections if specific ones are requested
		if len(targetCollections) > 0 {
			filteredCollections := make([]string, 0)
			for _, coll := range collections {
				for _, targetColl := range targetCollections {
					if coll == targetColl {
						filteredCollections = append(filteredCollections, coll)
						break
					}
				}
			}
			collections = filteredCollections
		}

		for _, collName := range collections {
			coll := collName
			db := dbName
			wg.Go(func() error {
				collID, err := globalMetaCache.GetCollectionID(t.ctx, db, coll)
				if err != nil {
					return merr.WrapErrAsInputErrorWhen(err, merr.ErrCollectionNotFound, merr.ErrDatabaseNotFound)
				}
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

				// Ask datacoord to get flushed segment infos.
				flushReq := &datapb.FlushRequest{
					Base: commonpbutil.NewMsgBase(
						commonpbutil.WithMsgType(commonpb.MsgType_Flush),
					),
					CollectionID: collID,
				}
				resp, err := t.mixCoord.Flush(ctx, flushReq)
				if err = merr.CheckRPCCall(resp, err); err != nil {
					return fmt.Errorf("failed to call flush to data coordinator: %s", err.Error())
				}

				// Remove the flushed segments from onFlushSegmentIDs
				for _, segID := range resp.GetFlushSegmentIDs() {
					for i, id := range onFlushSegmentIDs {
						if id == segID {
							onFlushSegmentIDs = append(onFlushSegmentIDs[:i], onFlushSegmentIDs[i+1:]...)
							break
						}
					}
				}

				// Send result to channel
				resultChan <- collectionFlushResult{
					dbName:          db,
					collName:        coll,
					collID:          collID,
					segmentIDs:      onFlushSegmentIDs,
					flushSegmentIDs: resp.GetFlushSegmentIDs(),
					channelCps:      resp.GetChannelCps(),
				}
				return nil
			})
		}
	}

	// Close result channel when all goroutines finish
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Wait for goroutines and collect results
	for result := range resultChan {
		// Get or create result for this database
		dbResult, exists := dbResultsMap[result.dbName]
		if !exists {
			dbResult = &milvuspb.FlushAllResult{
				DbName:            result.dbName,
				TargetCollections: make([]string, 0),
				CollSegIDs:        make(map[string]*schemapb.LongArray),
				FlushCollSegIDs:   make(map[string]*schemapb.LongArray),
				CollSealTimes:     make(map[string]int64),
				CollFlushTs:       make(map[string]uint64),
				ChannelCps:        make(map[string]*msgpb.MsgPosition),
			}
			dbResultsMap[result.dbName] = dbResult
		}

		// Add collection to target collections if not already there
		found := false
		for _, name := range dbResult.TargetCollections {
			if name == result.collName {
				found = true
				break
			}
		}
		if !found {
			dbResult.TargetCollections = append(dbResult.TargetCollections, result.collName)
		}

		// Add collection results
		if len(result.segmentIDs) > 0 {
			dbResult.CollSegIDs[result.collName] = &schemapb.LongArray{Data: result.segmentIDs}
		}
		if len(result.flushSegmentIDs) > 0 {
			dbResult.FlushCollSegIDs[result.collName] = &schemapb.LongArray{Data: result.flushSegmentIDs}
		}
		dbResult.CollSealTimes[result.collName] = timeOfSeal.Unix()
		dbResult.CollFlushTs[result.collName] = flushTs

		// Merge channel checkpoints
		for channel, cp := range result.channelCps {
			dbResult.ChannelCps[channel] = cp
		}
	}

	// Wait for all goroutines to complete
	if err := wg.Wait(); err != nil {
		return err
	}

	// Convert map to slice
	flushResults := make([]*milvuspb.FlushAllResult, 0, len(dbResultsMap))
	for _, dbResult := range dbResultsMap {
		flushResults = append(flushResults, dbResult)
	}

	t.result = &milvuspb.FlushAllResponse{
		Status:       merr.Success(),
		FlushAllTs:   flushTs,
		FlushResults: flushResults,
	}
	return nil
}
