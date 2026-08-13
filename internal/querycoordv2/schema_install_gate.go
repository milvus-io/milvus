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

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/schemaevolution"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/commonpbutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func (s *Server) gateManager() *schemaevolution.GateManager {
	return s.installGate
}

func (s *Server) acquireTopologyLease(ctx context.Context, collectionID int64) (func(), error) {
	if s.gateManager() == nil {
		return func() {}, nil
	}
	return s.gateManager().Acquire(ctx, collectionID)
}

func (s *Server) checkTopologyAdmission(collectionID int64) error {
	if s.gateManager() == nil {
		return nil
	}
	return s.gateManager().Check(collectionID)
}

// PrepareSchemaInstall closes collection topology admission and drains every
// operation admitted before the close. The admission manager, task scheduler,
// and job scheduler are all covered: a task/job cannot be created outside an
// already-counted API/checker lease and cross the returned boundary.
func (s *Server) PrepareSchemaInstall(ctx context.Context, collectionID int64) error {
	gate := s.gateManager()
	if gate == nil {
		return nil
	}
	gate.Close(collectionID)
	if s.jobScheduler != nil {
		if err := s.jobScheduler.WaitCollectionIdle(ctx, collectionID); err != nil {
			return merr.Wrap(err, "failed to drain schema install jobs")
		}
	}
	if s.taskScheduler != nil {
		if err := s.taskScheduler.WaitCollectionIdle(ctx, collectionID); err != nil {
			return merr.Wrap(err, "failed to drain schema install tasks")
		}
	}
	if err := gate.WaitIdle(ctx, collectionID); err != nil {
		return merr.Wrap(err, "failed to drain schema install topology leases")
	}
	return nil
}

// CompleteSchemaInstall actively applies the target schema/barrier to every
// QueryNode currently reporting a segment or channel for the collection.
// QueryNode delegators propagate the update to their workers. Restricting the
// direct participants to actual holders avoids permanently retrying against a
// replica member that has not loaded any collection state yet.
func (s *Server) CompleteSchemaInstall(ctx context.Context, collectionID int64, schema *schemapb.CollectionSchema, schemaBarrierTs uint64) error {
	if s.gateManager() == nil {
		return nil
	}

	if err := merr.CheckHealthy(s.State()); err != nil {
		return merr.Wrap(err, "querycoord is not ready to complete schema installation")
	}
	provider := s.schemaInstallVersionProvider
	if provider == nil {
		provider = s.session
	}
	if err := schemaevolution.CheckPhase0VersionContract(ctx, provider); err != nil {
		return merr.Wrap(err, "schema install version contract is not ready")
	}
	if s.meta == nil || s.dist == nil || s.cluster == nil {
		return merr.WrapErrServiceNotReadyMsg("querycoord schema installation dependencies are not initialized")
	}
	if !s.meta.Exist(ctx, collectionID) {
		// No loaded topology remains for this collection. Its deletion is the
		// terminal state, so there is no participant on which an old joining node
		// could publish stale collection state.
		s.gateManager().Open(collectionID)
		return nil
	}

	nodes := typeutil.NewUniqueSet()
	if s.dist != nil {
		for _, segment := range s.dist.SegmentDistManager.GetByFilter(meta.WithCollectionID(collectionID)) {
			nodes.Insert(segment.Node)
		}
		for _, channel := range s.dist.ChannelDistManager.GetByFilter(meta.WithCollectionID2Channel(collectionID)) {
			nodes.Insert(channel.Node)
			if channel.View == nil {
				continue
			}
			for _, segment := range channel.View.Segments {
				nodes.Insert(segment.GetNodeID())
			}
			for _, segment := range channel.View.GrowingSegments {
				nodes.Insert(segment.Node)
			}
		}
	}

	var combined error
	for _, nodeID := range nodes.Collect() {
		req := &querypb.UpdateSchemaRequest{
			Base: commonpbutil.NewMsgBase(
				commonpbutil.WithMsgType(commonpb.MsgType_AlterCollectionSchema),
				commonpbutil.WithSourceID(paramtable.GetNodeID()),
			),
			CollectionID:    collectionID,
			Schema:          schema,
			SchemaBarrierTs: schemaBarrierTs,
		}
		status, err := s.cluster.UpdateSchema(ctx, nodeID, req)
		if err = merr.CheckRPCCall(status, err); err != nil {
			combined = merr.Combine(combined, merr.Wrapf(err,
				"failed to apply schema install barrier on querynode %d", nodeID))
		}
	}
	if combined != nil {
		return combined
	}
	s.gateManager().Open(collectionID)

	mlog.Info(ctx, "schema install gate completed",
		mlog.FieldCollectionID(collectionID),
		mlog.Int64s("queryNodeIDs", lo.Uniq(nodes.Collect())),
		mlog.Int32("schemaVersion", schema.GetVersion()),
		mlog.Uint64("schemaBarrierTs", schemaBarrierTs))
	return nil
}

func (s *Server) CompleteSchemaInstallRequest(ctx context.Context, collectionID int64, req *querypb.UpdateSchemaRequest) error {
	return s.CompleteSchemaInstall(ctx, collectionID, req.GetSchema(), req.GetSchemaBarrierTs())
}

func (s *Server) AbortSchemaInstall(_ context.Context, collectionID int64) {
	if s.gateManager() != nil {
		s.gateManager().Open(collectionID)
	}
}
