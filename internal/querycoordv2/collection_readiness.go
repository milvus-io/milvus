// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querycoordv2

import (
	"context"
	"strconv"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/internal/views/coord/loadstatus"
	"github.com/milvus-io/milvus/internal/views/coord/readiness"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// EnsureCollectionReady loads an absent collection and waits until all its shards are Up.
func (s *Server) EnsureCollectionReady(ctx context.Context, req *querypb.EnsureCollectionReadyRequest) (*commonpb.Status, error) {
	return merr.Status(s.ensureCollectionReady(ctx, req)), nil
}

func (s *Server) ensureCollectionReady(ctx context.Context, req *querypb.EnsureCollectionReadyRequest) error {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return err
	}
	if ctx.Err() != nil {
		return context.Cause(ctx)
	}
	collectionID := req.GetCollectionID()
	runtime := s.qviewsRuntime
	s.collectionUsage.touch(collectionID)
	progress := loadstatus.Get(runtime.loadConfigStore, runtime.shardViewRegistry, collectionID, req.GetExpectedVchannels())
	if progress.Ready() {
		return nil
	}

	result := s.autoLoadCollectionGroup.DoChan(strconv.FormatInt(collectionID, 10), func() (any, error) {
		// The shared operation belongs to Coord, not to the first waiting Proxy.
		loadCtx, cancelLifecycle := contextutil.MergeContext(context.WithoutCancel(ctx), s.ctx)
		defer cancelLifecycle()
		loadCtx, cancelTimeout := context.WithTimeout(loadCtx, paramtable.Get().QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
		defer cancelTimeout()
		if progress.Config == nil {
			coll, err := s.broker.DescribeCollection(loadCtx, collectionID)
			if err != nil {
				return nil, err
			}
			loadReq, err := s.prepareAutoLoadRequest(loadCtx, coll)
			if err != nil {
				return nil, err
			}
			status, err := s.LoadCollection(loadCtx, loadReq)
			if err := merr.CheckRPCCall(status, err); err != nil {
				return nil, err
			}
		}
		return nil, s.waitCollectionReady(loadCtx, req)
	})
	select {
	case r := <-result:
		if ctx.Err() != nil {
			return context.Cause(ctx)
		}
		if r.Err == nil {
			// A long load gets a fresh idle TTL immediately before query execution.
			s.collectionUsage.touch(collectionID)
		}
		return r.Err
	case <-ctx.Done():
		return context.Cause(ctx)
	case <-runtime.readyChanges.Done():
		return merr.WrapErrServiceUnavailableMsg("querycoord stopped while waiting for collection %d", collectionID)
	}
}

// prepareAutoLoadRequest applies the same default field and index requirements
// as a Proxy LoadCollection task, using Coord's collection metadata.
func (s *Server) prepareAutoLoadRequest(ctx context.Context, coll *milvuspb.DescribeCollectionResponse) (*querypb.LoadCollectionRequest, error) {
	schema := coll.GetSchema()
	if err := typeutil.ValidateTextRequiresStorageV3(schema, paramtable.Get().CommonCfg.UseLoonFFI.GetAsBool()); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("%s", err.Error())
	}
	indexes, err := s.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{CollectionID: coll.GetCollectionID()})
	if err = merr.CheckRPCCall(indexes, err); err != nil {
		if errors.Is(err, merr.ErrIndexNotFound) {
			return nil, merr.WrapErrIndexNotFoundForCollection(coll.GetCollectionName())
		}
		return nil, err
	}
	fieldIndexes := make(map[int64]int64)
	for _, index := range indexes.GetIndexInfos() {
		fieldIndexes[index.GetFieldID()] = index.GetIndexID()
	}
	// QueryView load configs carry explicit field IDs, including the default
	// all-fields case; do not use the empty-list shorthand for all fields.
	loadFields := make([]int64, 0)
	unindexed := make([]string, 0)
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		if common.IsSystemField(field.GetFieldID()) {
			continue
		}
		enabled, err := common.ShouldFieldBeLoaded(field.GetTypeParams())
		if err == nil && !enabled {
			continue
		}
		loadFields = append(loadFields, field.GetFieldID())
		if typeutil.IsVectorType(field.GetDataType()) {
			if _, ok := fieldIndexes[field.GetFieldID()]; !ok {
				unindexed = append(unindexed, field.GetName())
			}
		}
	}
	if len(unindexed) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("there is no vector index on field: %v, please create index firstly", unindexed)
	}
	return &querypb.LoadCollectionRequest{
		DbID:         coll.GetDbId(),
		CollectionID: coll.GetCollectionID(),
		Schema:       schema,
		LoadFields:   loadFields,
		FieldIndexID: fieldIndexes,
		Priority:     commonpb.LoadPriority_HIGH,
	}, nil
}

func (s *Server) waitCollectionReady(ctx context.Context, req *querypb.EnsureCollectionReadyRequest) error {
	collectionID := req.GetCollectionID()
	runtime := s.qviewsRuntime
	// Subscribe before checking so state changes cannot be lost before blocking.
	subscription := runtime.readyChanges.Subscribe(collectionID)
	defer subscription.Close()
	for {
		if err := ctx.Err(); err != nil {
			return context.Cause(ctx)
		}
		changed, released := subscription.Observe()
		if released {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		// The same configuration-version-checked result drives public loading
		// progress and AutoLoad readiness. Observe release again after the read
		// so a coalesced release/reload cannot satisfy an older subscriber.
		progress := loadstatus.Get(runtime.loadConfigStore, runtime.shardViewRegistry, collectionID, req.GetExpectedVchannels())
		if progress.Config == nil {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		if _, released := subscription.Observe(); released {
			return merr.WrapErrCollectionNotLoaded(collectionID)
		}
		if progress.Ready() {
			return nil
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-runtime.readyChanges.Done():
			return merr.WrapErrServiceUnavailableMsg("querycoord stopped while waiting for collection %d", collectionID)
		case <-changed:
		}
	}
}

func newCollectionReadiness(loadConfigStore *loadmgr.LoadConfigStore, shardViewRegistry *coordview.ShardViewRegistry) *readiness.Notifications {
	readyChanges := readiness.NewNotifications()
	loadConfigStore.RegisterObserver(func(collectionID int64, released bool) {
		if released {
			readyChanges.Release(collectionID)
		} else {
			readyChanges.Notify(collectionID)
		}
	})
	shardViewRegistry.RegisterStatsObserver(func(shardID qviews.ShardID, _ *coordview.ShardStats) {
		channel, err := metautil.ParseChannel(shardID.VChannel, metautil.NewDynChannelMapper())
		if err == nil {
			readyChanges.Notify(channel.CollectionID())
		}
	})
	return readyChanges
}
