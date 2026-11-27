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

package flushall

import (
	"context"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	etcdkv "github.com/milvus-io/milvus/internal/kv/etcd"
	streamingcatalog "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

type flushAllCheckpointSuite struct {
	integration.MiniClusterSuite
}

func (s *flushAllCheckpointSuite) SetupSuite() {
	params := paramtable.Get()
	s.WithMilvusConfig(params.RootCoordCfg.DmlChannelNum.Key, "2")
	s.WithMilvusConfig(params.StreamingCfg.WALRecoveryPersistInterval.Key, "1s")
	s.WithClusterOptions(cluster.WithMetricsPorts())
	s.MiniClusterSuite.SetupSuite()
}

func (s *flushAllCheckpointSuite) TestIdleChannels() {
	ctx, cancel := context.WithTimeout(s.T().Context(), 2*time.Minute)
	defer cancel()
	c := s.Cluster
	const (
		name = "flush_all_checkpoint"
		dim  = 8
		rows = 4
	)
	schema, err := proto.Marshal(integration.ConstructSchema(name, dim, false))
	s.Require().NoError(err)
	status, err := c.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: schema, ShardsNum: 1,
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	inserted, err := c.MilvusClient.Insert(ctx, &milvuspb.InsertRequest{
		CollectionName: name, NumRows: rows,
		FieldsData: []*schemapb.FieldData{
			integration.NewInt64FieldDataWithStart(integration.Int64Field, rows, 0),
			integration.NewFloatVectorFieldData(integration.FloatVecField, rows, dim),
		},
	})
	s.Require().NoError(merr.CheckRPCCall(inserted, err))
	deleted, err := c.MilvusClient.Delete(ctx, &milvuspb.DeleteRequest{
		CollectionName: name, Expr: integration.Int64Field + " in [0]",
	})
	s.Require().NoError(merr.CheckRPCCall(deleted, err))

	resp, err := c.MilvusClient.FlushAll(ctx, &milvuspb.FlushAllRequest{})
	s.Require().NoError(merr.CheckRPCCall(resp, err))
	s.Require().Len(resp.GetFlushAllMsgs(), 2)
	// Only one PChannel received DML. Leave both idle after FlushAll and
	// compare the durable catalog position to each channel's own full TSO.
	catalog := streamingcatalog.NewCataLog(etcdkv.NewEtcdKV(c.EtcdCli, path.Join(c.RootPath(), "meta")))
	for channel, msg := range resp.GetFlushAllMsgs() {
		receipt := message.MilvusMessageToImmutableMessage(msg).TimeTick()
		s.Require().NotZero(receipt)
		var published uint64
		s.Require().EventuallyWithT(func(t *assert.CollectT) {
			cp, err := catalog.GetConsumeCheckpoint(ctx, channel)
			if assert.NoError(t, err) {
				published = cp.GetTimeTick()
				assert.GreaterOrEqual(t, published, receipt, "PChannel %s", channel)
			}
		}, 30*time.Second, 100*time.Millisecond)
		s.T().Logf("%s receipt=%d published=%d", channel, receipt, published)
	}
}

func TestFlushAllCheckpoint(t *testing.T) {
	suite.Run(t, new(flushAllCheckpointSuite))
}
