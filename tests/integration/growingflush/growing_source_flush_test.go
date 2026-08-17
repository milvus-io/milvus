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

package growingflush

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metric"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

// GrowingSourceFlushSuite covers the one property that unit tests cannot reach:
// that rows which live ONLY in a querynode growing segment survive the channel
// being unsubscribed out from under them.
//
// In growing-source mode the write buffer keeps no copy of those rows — that is
// the whole point of the feature — so the release path has to drain the flushes
// it owes (WaitGrowingFlushDrained) and fence further growing-source admission
// (FenceGrowingSourceAdmission) before the querynode drops its growing
// segments. If either is wrong the flush loses its source, the channel
// checkpoint stays pinned behind it, and those rows never reach storage. That
// failure is invisible to every unit test, because it needs a real channel
// balance against a real WAL.
type GrowingSourceFlushSuite struct {
	integration.MiniClusterSuite
}

func (s *GrowingSourceFlushSuite) SetupSuite() {
	// AllowGrowingSourceFlush needs BOTH: storage v3, and the growing-source
	// switch. With either off the path degrades to the ordinary write-buffer
	// flush and this suite would pass without testing anything, which is why
	// the storage version is asserted below rather than assumed.
	s.WithMilvusConfig(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	s.WithMilvusConfig(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "true")

	// Make the channel actually move once a second streaming node appears.
	s.WithMilvusConfig(paramtable.Get().QueryCoordCfg.BalanceCheckInterval.Key, "1000")
	s.WithMilvusConfig(paramtable.Get().StreamingCfg.WALBalancerPolicyMinRebalanceIntervalThreshold.Key, "1ms")
	// Compaction would rewrite segments underneath the row-count assertion.
	s.WithMilvusConfig(paramtable.Get().DataCoordCfg.EnableCompaction.Key, "false")
	s.MiniClusterSuite.SetupSuite()
}

const (
	dim    = 128
	dbName = ""
)

// TestChannelReleaseKeepsGrowingOnlyRows drives a channel release while rows
// are still only in growing segments, then proves nothing was lost.
func (s *GrowingSourceFlushSuite) TestChannelReleaseKeepsGrowingOnlyRows() {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Minute)
	defer cancel()
	c := s.Cluster

	collectionName := "TestGrowingSourceFlush" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(collectionName, dim, true)
	marshaledSchema, err := proto.Marshal(schema)
	s.Require().NoError(err)

	createStatus, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Schema:         marshaledSchema,
		ShardsNum:      2,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, createStatus.GetErrorCode())

	// Index and load first: the delegator — and with it the growing-source
	// provider — only exists once the channel is watched.
	createIndexStatus, err := c.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: collectionName,
		FieldName:      integration.FloatVecField,
		IndexName:      "_default",
		ExtraParams:    integration.ConstructIndexParam(dim, integration.IndexFaissIvfFlat, metric.L2),
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, createIndexStatus.GetErrorCode())

	loadStatus, err := c.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{
		DbName:         dbName,
		CollectionName: collectionName,
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, loadStatus.GetErrorCode())
	s.WaitForLoad(ctx, collectionName)

	const (
		batchRows  = 2000
		preBatches = 3
	)
	insert := func(rows int) {
		vec := integration.NewFloatVectorFieldData(integration.FloatVecField, rows, dim)
		result, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
			DbName:         dbName,
			CollectionName: collectionName,
			FieldsData:     []*schemapb.FieldData{vec},
			HashKeys:       integration.GenerateHashKeys(rows),
			NumRows:        uint32(rows),
		})
		s.Require().NoError(err)
		s.Require().Equal(commonpb.ErrorCode_Success, result.GetStatus().GetErrorCode())
	}

	// Rows that are NOT flushed: they exist only in the querynode growing
	// segments, which is the state the release path has to survive.
	inserted := 0
	for i := 0; i < preBatches; i++ {
		insert(batchRows)
		inserted += batchRows
	}

	channelsBefore := s.channelOwners(ctx)
	s.Require().NotEmpty(channelsBefore, "expected the loaded channels to be served somewhere")
	mlog.Info(ctx, "channel owners before release", mlog.Any("owners", channelsBefore))

	// A new streaming node makes the WAL rebalance, which moves a vchannel and
	// makes querycoord issue UnsubDmChannel against the node that held it — the
	// release path under test.
	c.AddStreamingNode()

	// Keep writing across the move. This is what puts an insert in the window
	// between the release fence and the unsubscribe, the case the admission
	// fence exists for.
	moved := false
	for i := 0; i < 20 && !moved; i++ {
		insert(batchRows)
		inserted += batchRows
		owners := s.channelOwners(ctx)
		for channel, owner := range owners {
			if before, ok := channelsBefore[channel]; ok && before != owner {
				moved = true
				mlog.Info(ctx, "channel moved",
					mlog.String("channel", channel),
					mlog.Int64("from", before),
					mlog.Int64("to", owner))
				break
			}
		}
	}
	s.Require().True(moved, "no channel changed owner; the release path was never exercised")

	// The release must have completed rather than been retried forever: every
	// channel that was served before is served again, by exactly one owner.
	// Comparing only map sizes could pass with a lost channel plus a spurious
	// one, so check the actual channel->owner assignment.
	s.Eventually(func() bool {
		owners := s.channelOwners(ctx)
		if len(owners) != len(channelsBefore) {
			return false
		}
		for channel := range channelsBefore {
			if _, ok := owners[channel]; !ok {
				return false
			}
		}
		return true
	}, time.Minute, time.Second, "channels did not settle after the release")

	// Everything must still be there. A stranded growing-source flush shows up
	// here as a short count, because its rows never reached storage and its
	// checkpoint never advanced.
	flushResp, err := c.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{
		DbName:          dbName,
		CollectionNames: []string{collectionName},
	})
	s.Require().NoError(err)
	segIDs, ok := flushResp.GetCollSegIDs()[collectionName]
	s.Require().True(ok)
	flushTs, ok := flushResp.GetCollFlushTs()[collectionName]
	s.Require().True(ok)
	s.WaitForFlush(ctx, segIDs.GetData(), flushTs, dbName, collectionName)

	// Proves the feature was actually on: growing-source flush is gated on
	// storage v3, so a v2 segment here would mean the run silently fell back to
	// the write-buffer path and asserted nothing.
	segments, err := c.ShowSegments(collectionName)
	s.Require().NoError(err)
	s.Require().NotEmpty(segments)
	for _, segment := range segments {
		s.Equal(storage.StorageV3, segment.GetStorageVersion(),
			"segment %d is not storage v3; growing-source flush cannot have engaged", segment.GetID())
	}

	// TODO(growing-source observability): storage v3 is a NECESSARY condition
	// for the growing-source path but not proof it engaged — with UseLoonFFI
	// every segment is v3 even when AllowGrowingSourceFlush returns false and
	// the run silently degrades to the ordinary write-buffer flush. No
	// production-observable positive signal exists today: the only
	// growing-source-specific metric (datanode growing_source_sync_failure_count)
	// reports failures, the success-path metrics (write_data_count etc.) share
	// the "streaming" data-source label with the write-buffer path, the mini
	// cluster runs each node as a separate process (so in-process metric reads
	// are impossible), and neither ShowSegments nor GetDataDistribution carries
	// a flush-source marker. Until a positive signal exists (e.g. a distinct
	// data-source label or a per-channel growing-flush counter), this suite
	// proves durability across release but cannot prove which flush path
	// provided it.
	s.T().Log("WARNING: cannot assert the growing-source flush branch actually ran; " +
		"storage v3 check above only rules the fallback IN, not the growing path OUT")

	s.Equal(inserted, s.count(ctx, collectionName),
		"rows were lost across the channel release")
}

// channelOwners maps each served vchannel to the node id serving it.
func (s *GrowingSourceFlushSuite) channelOwners(ctx context.Context) map[string]int64 {
	owners := make(map[string]int64)
	for _, node := range s.Cluster.GetAllStreamingAndQueryNodesClient() {
		resp, err := node.GetDataDistribution(ctx, &querypb.GetDataDistributionRequest{})
		if err != nil || !merr.Ok(resp.GetStatus()) {
			continue
		}
		for _, view := range resp.GetLeaderViews() {
			owners[view.GetChannel()] = resp.GetNodeID()
		}
	}
	return owners
}

func (s *GrowingSourceFlushSuite) count(ctx context.Context, collectionName string) int {
	resp, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		DbName:         dbName,
		CollectionName: collectionName,
		Expr:           "",
		OutputFields:   []string{"count(*)"},
	})
	s.Require().NoError(err)
	s.Require().Equal(commonpb.ErrorCode_Success, resp.GetStatus().GetErrorCode())
	s.Require().NotEmpty(resp.GetFieldsData())

	total := resp.GetFieldsData()[0].GetScalars().GetLongData().GetData()
	s.Require().NotEmpty(total)
	return int(total[0])
}

func TestGrowingSourceFlush(t *testing.T) {
	suite.Run(t, new(GrowingSourceFlushSuite))
}
