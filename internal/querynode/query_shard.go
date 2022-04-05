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

package querynode

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/milvus-io/milvus/internal/proto/querypb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

type queryShard struct {
	ctx    context.Context
	cancel context.CancelFunc

	collectionID UniqueID
	channel      Channel
	replicaID    int64

	cluster    *ShardCluster
	historical *historical
	streaming  *streaming

	localChunkManager  storage.ChunkManager
	remoteChunkManager storage.ChunkManager
	vectorChunkManager *storage.VectorChunkManager
	localCacheEnabled  bool
	localCacheSize     int64
}

func newQueryShard(
	ctx context.Context,
	collectionID UniqueID,
	channel Channel,
	replicaID int64,
	cluster *ShardCluster,
	historical *historical,
	streaming *streaming,
	localChunkManager storage.ChunkManager,
	remoteChunkManager storage.ChunkManager,
	localCacheEnabled bool,
) *queryShard {
	ctx, cancel := context.WithCancel(ctx)
	qs := &queryShard{
		ctx:                ctx,
		cancel:             cancel,
		collectionID:       collectionID,
		channel:            channel,
		replicaID:          replicaID,
		cluster:            cluster,
		historical:         historical,
		streaming:          streaming,
		localChunkManager:  localChunkManager,
		remoteChunkManager: remoteChunkManager,
		localCacheEnabled:  localCacheEnabled,
		localCacheSize:     Params.QueryNodeCfg.LocalFileCacheLimit,
	}
	return qs
}

func (q *queryShard) search(ctx context.Context, req *querypb.SearchRequest) (*milvuspb.SearchResults, error) {
	return nil, errors.New("not implemented")
}

func (q *queryShard) query(ctx context.Context, req *querypb.QueryRequest) (*internalpb.RetrieveResults, error) {
	collectionID := req.Req.CollectionID
	segmentIDs := req.SegmentIDs
	partitionIDs := req.Req.PartitionIDs
	expr := req.Req.SerializedExprPlan
	timestamp := req.Req.TravelTimestamp
	// TODO: hold request until guarantee timestamp >= service timestamp

	// deserialize query plan
	collection, err := q.streaming.replica.getCollectionByID(collectionID)
	if err != nil {
		return nil, err
	}
	plan, err := createRetrievePlanByExpr(collection, expr, timestamp)
	if err != nil {
		return nil, err
	}
	defer plan.delete()

	// TODO: init vector chunk manager at most once
	if q.vectorChunkManager == nil {
		if q.localChunkManager == nil {
			return nil, fmt.Errorf("can not create vector chunk manager for local chunk manager is nil")
		}
		if q.remoteChunkManager == nil {
			return nil, fmt.Errorf("can not create vector chunk manager for remote chunk manager is nil")
		}
		q.vectorChunkManager, err = storage.NewVectorChunkManager(q.localChunkManager, q.remoteChunkManager,
			&etcdpb.CollectionMeta{
				ID:     collection.id,
				Schema: collection.schema,
			}, q.localCacheSize, q.localCacheEnabled)
		if err != nil {
			return nil, err
		}
	}

	// check if shard leader b.c only leader receives request with non-empty dml channel
	if req.DmlChannel != "" {
		if q.cluster == nil {
			return nil, errors.New("shard cluster is nil, but received non-empty dml channel")
		}
		// shard leader dispatches request to its shard cluster
		results, err := q.cluster.Query(ctx, req)
		if err != nil {
			return nil, err
		}
		// shard leader queries its own streaming data
		// TODO: filter stream retrieve results by channel
		streamingResults, _, _, err := q.streaming.retrieve(collectionID, partitionIDs, plan)
		if err != nil {
			return nil, err
		}
		streamingResult, err := mergeRetrieveResults(streamingResults)
		if err != nil {
			return nil, err
		}
		// complete results with merged streaming result
		results = append(results, &internalpb.RetrieveResults{
			Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
			Ids:        streamingResult.Ids,
			FieldsData: streamingResult.FieldsData,
		})
		// merge shard query results
		return mergeInternalRetrieveResults(results)
	}

	// shard follower considers solely historical segments
	retrieveResults, err := q.historical.retrieveBySegmentIDs(collectionID, segmentIDs, q.vectorChunkManager, plan)
	if err != nil {
		return nil, err
	}
	mergedResult, err := mergeRetrieveResults(retrieveResults)
	if err != nil {
		return nil, err
	}

	log.Debug("retrieve result", zap.String("ids", mergedResult.Ids.String()))
	RetrieveResults := &internalpb.RetrieveResults{
		Status:     &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		Ids:        mergedResult.Ids,
		FieldsData: mergedResult.FieldsData,
	}
	return RetrieveResults, nil
}

// TODO: largely based on function mergeRetrieveResults, need rewriting
func mergeInternalRetrieveResults(retrieveResults []*internalpb.RetrieveResults) (*internalpb.RetrieveResults, error) {
	var ret *internalpb.RetrieveResults
	var skipDupCnt int64
	var idSet = make(map[int64]struct{})

	// merge results and remove duplicates
	for _, rr := range retrieveResults {

		if ret == nil {
			ret = &internalpb.RetrieveResults{
				Ids: &schemapb.IDs{
					IdField: &schemapb.IDs_IntId{
						IntId: &schemapb.LongArray{
							Data: []int64{},
						},
					},
				},
				FieldsData: make([]*schemapb.FieldData, len(rr.FieldsData)),
			}
		}

		if len(ret.FieldsData) != len(rr.FieldsData) {
			return nil, fmt.Errorf("mismatch FieldData in RetrieveResults")
		}

		dstIds := ret.Ids.GetIntId()
		for i, id := range rr.Ids.GetIntId().GetData() {
			if _, ok := idSet[id]; !ok {
				dstIds.Data = append(dstIds.Data, id)
				typeutil.AppendFieldData(ret.FieldsData, rr.FieldsData, int64(i))
				idSet[id] = struct{}{}
			} else {
				// primary keys duplicate
				skipDupCnt++
			}
		}
	}

	// not found, return default values indicating not result found
	if ret == nil {
		ret = &internalpb.RetrieveResults{
			Ids:        &schemapb.IDs{},
			FieldsData: []*schemapb.FieldData{},
		}
	}

	return ret, nil
}
