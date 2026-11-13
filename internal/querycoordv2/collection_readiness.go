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
	"github.com/milvus-io/milvus/pkg/v3/util/stage"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// EnsureCollectionReady loads an absent collection and waits until all its shards are Up.
func (s *Server) EnsureCollectionReady(ctx context.Context, req *querypb.EnsureCollectionReadyRequest) (*commonpb.Status, error) {
	path, err := s.ensureCollectionReadyWithPath(ctx, req)
	status := merr.Status(err)
	if path != "" {
		// Telemetry only: Proxy labels request latency with the readiness path
		// this caller observed. Older Proxies ignore the entry.
		if status.ExtraInfo == nil {
			status.ExtraInfo = make(map[string]string)
		}
		status.ExtraInfo[stage.ReadinessPathKey] = path
	}
	return status, nil
}

var (
	collectionReadyCheck = stage.New("coord", "readiness", "check")
	autoLoadLifecycle    = stage.New("coord", "auto_load", "shared_lifecycle")
	autoLoadSubmit       = stage.New("coord", "auto_load", "load_submit")
	autoLoadReadyWait    = stage.New("coord", "auto_load", "ready_wait")
	autoLoadCallerWait   = stage.New("coord", "auto_load", "caller_wait")
)

func (s *Server) ensureCollectionReady(ctx context.Context, req *querypb.EnsureCollectionReadyRequest) error {
	_, err := s.ensureCollectionReadyWithPath(ctx, req)
	return err
}

// ensureCollectionReadyWithPath also returns the readiness path this caller
// observed before joining any shared load; it is empty when no check ran.
func (s *Server) ensureCollectionReadyWithPath(ctx context.Context, req *querypb.EnsureCollectionReadyRequest) (string, error) {
	if err := merr.CheckHealthy(s.State()); err != nil {
		return "", err
	}
	if ctx.Err() != nil {
		return "", context.Cause(ctx)
	}
	collectionID := req.GetCollectionID()
	runtime := s.qviewsRuntime
	s.collectionUsage.touch(collectionID)
	checkStartedAt := time.Now()
	progress := loadstatus.Get(runtime.loadConfigStore, runtime.shardViewRegistry, collectionID, req.GetExpectedVchannels())
	if progress.Ready() {
		collectionReadyCheck.Observe(time.Since(checkStartedAt), stage.Success)
		return stage.PathReady, nil
	}
	collectionReadyCheck.Observe(time.Since(checkStartedAt), stage.NotReady)
	path := stage.PathLoading
	if progress.Config == nil {
		path = stage.PathUnloaded
	}

	result := s.autoLoadCollectionGroup.DoChan(strconv.FormatInt(collectionID, 10), func() (_ any, retErr error) {
		// The shared operation belongs to Coord, not to the first waiting Proxy.
		loadCtx, cancelLifecycle := contextutil.MergeContext(context.WithoutCancel(ctx), s.ctx)
		defer cancelLifecycle()
		loadCtx, cancelTimeout := context.WithTimeout(loadCtx, paramtable.Get().QueryCoordCfg.LoadTimeoutSeconds.GetAsDuration(time.Second))
		defer cancelTimeout()
		lifecycleTimer := autoLoadLifecycle.Begin()
		defer lifecycleTimer.EndError(&retErr)
		if progress.Config == nil {
			submitTimer := autoLoadSubmit.Begin()
			err := s.submitAutoLoad(loadCtx, collectionID)
			submitTimer.End(err)
			if err != nil {
				return nil, err
			}
		}
		waitTimer := autoLoadReadyWait.Begin()
		err := s.waitCollectionReady(loadCtx, req)
		waitTimer.End(err)
		return nil, err
	})
	callerTimer := autoLoadCallerWait.Begin()
	select {
	case r := <-result:
		if ctx.Err() != nil {
			callerTimer.End(context.Cause(ctx))
			return path, context.Cause(ctx)
		}
		callerTimer.End(r.Err)
		if r.Err == nil {
			// A long load gets a fresh idle TTL immediately before query execution.
			s.collectionUsage.touch(collectionID)
		}
		return path, r.Err
	case <-ctx.Done():
		callerTimer.End(context.Cause(ctx))
		return path, context.Cause(ctx)
	case <-runtime.readyChanges.Done():
		err := merr.WrapErrServiceUnavailableMsg("querycoord stopped while waiting for collection %d", collectionID)
		callerTimer.End(err)
		return path, err
	}
}

// submitAutoLoad submits a load for an absent collection with Proxy defaults.
func (s *Server) submitAutoLoad(ctx context.Context, collectionID int64) error {
	coll, err := s.broker.DescribeCollection(ctx, collectionID)
	if err != nil {
		return err
	}
	loadReq, err := s.prepareAutoLoadRequest(ctx, coll)
	if err != nil {
		return err
	}
	status, err := s.LoadCollection(ctx, loadReq)
	return merr.CheckRPCCall(status, err)
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
	return s.waitCollectionReadyAtSchemaVersion(ctx, req, 0)
}

func (s *Server) waitCollectionReadyAtSchemaVersion(ctx context.Context, req *querypb.EnsureCollectionReadyRequest, schemaVersion int32) error {
	return s.waitCollectionReadyAtVersions(ctx, req, schemaVersion, qviews.DataVersion{})
}

func (s *Server) waitCollectionReadyAtVersions(ctx context.Context, req *querypb.EnsureCollectionReadyRequest, schemaVersion int32, dataVersion qviews.DataVersion) error {
	r := s.qviewsRuntime
	return loadstatus.Wait(ctx, r.loadConfigStore, r.shardViewRegistry, r.readyChanges, req.GetCollectionID(), req.GetExpectedVchannels(), schemaVersion, dataVersion)
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
