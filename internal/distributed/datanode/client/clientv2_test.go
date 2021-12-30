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

package grpcdatanodeclient

import (
	"context"
	"testing"
	"time"

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/proto/internalpb"
	"github.com/milvus-io/milvus/internal/proto/milvuspb"
	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

func TestClientv2(t *testing.T) {
	dir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	embedEtcd, err := startEmbedEtcd(ctx, dir)
	cancel()
	assert.Nil(t, err)
	defer embedEtcd.Close()
	etcdAddr := embedEtcd.Clients[0].Addr().String()

	etcdCli, err := clientv3.NewFromURL(etcdAddr)
	assert.Nil(t, err)
	defer etcdCli.Close()

	s, nodeID, err := startMockDataNodeService(context.Background(), etcdCli)
	assert.Nil(t, err)
	defer s.GracefulStop()

	cli := NewClientV2(int64(nodeID), etcdAddr)
	assert.NoError(t, cli.Init())
	assert.NoError(t, cli.Start())
	defer func() {
		assert.NoError(t, cli.Stop())
	}()

	resp, err := cli.GetComponentStates(context.Background())
	assert.Nil(t, err)
	assert.EqualValues(t, &internalpb.ComponentStates{}, resp)

	resp2, err := cli.GetStatisticsChannel(context.Background())
	assert.Nil(t, err)
	assert.EqualValues(t, &milvuspb.StringResponse{}, resp2)

	resp3, err := cli.WatchDmChannels(context.Background(), &datapb.WatchDmChannelsRequest{})
	assert.Nil(t, err)
	assert.EqualValues(t, &commonpb.Status{}, resp3)

	resp4, err := cli.FlushSegments(context.Background(), &datapb.FlushSegmentsRequest{})
	assert.Nil(t, err)
	assert.EqualValues(t, &commonpb.Status{}, resp4)

	resp5, err := cli.GetMetrics(context.Background(), &milvuspb.GetMetricsRequest{})
	assert.Nil(t, err)
	assert.EqualValues(t, &milvuspb.GetMetricsResponse{}, resp5)

	resp6, err := cli.Compaction(context.Background(), &datapb.CompactionPlan{})
	assert.Nil(t, err)
	assert.EqualValues(t, &commonpb.Status{}, resp6)
}
