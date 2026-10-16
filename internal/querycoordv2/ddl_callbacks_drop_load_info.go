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

package querycoordv2

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/coordview"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

var errReleaseCollectionNotLoaded = errors.New("release collection not loaded")

// broadcastDropLoadConfigCollectionV2ForReleaseCollection broadcasts the drop load config message for release collection.
func (s *Server) broadcastDropLoadConfigCollectionV2ForReleaseCollection(ctx context.Context, req *querypb.ReleaseCollectionRequest) error {
	broadcaster, err := s.startBroadcastWithCollectionIDLock(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}
	defer broadcaster.Close()

	// double check if the collection is already dropped.
	coll, err := s.broker.DescribeCollection(ctx, req.GetCollectionID())
	if err != nil {
		return err
	}

	isLoaded := s.qviewsRuntime.loadConfigStore.Contains(req.GetCollectionID())
	if !isLoaded {
		// A previous release may have removed desired state before cleanup
		// completed. Keep the collection lock until its views are gone.
		if err := s.waitQueryViewsReleased(ctx, req.GetCollectionID()); err != nil {
			return err
		}
		return errReleaseCollectionNotLoaded
	}
	msg := message.NewDropLoadConfigMessageBuilderV2().
		WithHeader(&message.DropLoadConfigMessageHeader{
			DbId:         coll.GetDbId(),
			CollectionId: coll.GetCollectionID(),
		}).
		WithBody(&message.DropLoadConfigMessageBody{}).
		WithControlChannelBroadcast().
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (s *Server) dropLoadConfigV2AckCallback(ctx context.Context, result message.BroadcastResultDropLoadConfigMessageV2) error {
	if err := s.qviewsRuntime.loadManager.ReleaseCollection(ctx, result.Message.Header()); err != nil {
		return err
	}
	meta.GlobalFailedLoadCache.Remove(result.Message.Header().GetCollectionId())
	// Broadcast owns the collection lock until this callback completes. Waiting
	// here also serializes a subsequent load with the old replica's cleanup.
	return s.waitQueryViewsReleased(ctx, result.Message.Header().GetCollectionId())
}

func (s *Server) waitQueryViewsReleased(ctx context.Context, collectionID int64) error {
	// Drain any pass that captured the old desired state before checking the
	// registry. Otherwise an in-flight prepare could create a shard after an
	// empty-registry check. Reconcile serializes planning and application.
	if err := s.qviewsRuntime.balancer.Reconcile(ctx); err != nil {
		return err
	}
	registry := s.qviewsRuntime.shardViewRegistry
	changed := make(chan struct{}, 1)
	// Subscribe before checking to avoid losing a final-removal notification.
	unregister := registry.RegisterPublicationListener(func(shardID qviews.ShardID, stats *coordview.ShardStats) {
		_, id, _, err := funcutil.ParseVChannel(shardID.VChannel)
		if err != nil || id != collectionID || stats != nil {
			return
		}
		select {
		case changed <- struct{}{}:
		default:
		}
	})
	defer unregister()
	for {
		if err := ctx.Err(); err != nil {
			return merr.Wrapf(err, "waiting for QueryView release, collection=%d", collectionID)
		}
		// Dropping views can have neither Up nor segment stats. Only final
		// registry removal proves node cleanup and durable deletion completed.
		if len(registry.CollectionShards(collectionID)) == 0 {
			return nil
		}
		select {
		case <-ctx.Done():
		case <-changed:
		}
	}
}
