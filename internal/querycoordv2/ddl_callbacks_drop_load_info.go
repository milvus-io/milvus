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

	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
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

	if !s.meta.CollectionManager.Exist(ctx, req.GetCollectionID()) {
		return errReleaseCollectionNotLoaded
	}
	msg := message.NewDropLoadConfigMessageBuilderV2().
		WithHeader(&message.DropLoadConfigMessageHeader{
			DbId:         coll.GetDbId(),
			CollectionId: coll.GetCollectionID(),
		}).
		WithBody(&message.DropLoadConfigMessageBody{}).
		WithBroadcast([]string{streaming.WAL().ControlChannel()}). // TODO: after we support query view in 3.0, we should broadcast the drop load config message to all vchannels.
		MustBuildBroadcast()

	_, err = broadcaster.Broadcast(ctx, msg)
	return err
}

func (s *Server) dropLoadConfigV2AckCallback(ctx context.Context, result message.BroadcastResultDropLoadConfigMessageV2) error {
	releaseJob := job.NewReleaseCollectionJob(ctx,
		result,
		s.dist,
		s.meta,
		s.broker,
		s.targetMgr,
		s.targetObserver,
		s.checkerController,
		s.proxyClientManager,
	)
	if err := releaseJob.Execute(); err != nil {
		return err
	}
	meta.GlobalFailedLoadCache.Remove(result.Message.Header().GetCollectionId())
	return nil
}
