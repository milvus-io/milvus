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

package idempotency

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	walpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/failpoint"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
	fixture "github.com/milvus-io/milvus/tests/integration/failpoint"
)

type FailpointSuite struct {
	integration.MiniClusterSuite
	controlDir string
}

func (s *FailpointSuite) SetupSuite() {
	s.controlDir = s.T().TempDir()
	var points []string
	for _, point := range []string{walpoint.BeforeIdempotencyAppend, walpoint.AfterIdempotencyAppend, walpoint.AfterBackendAppend} {
		points = append(points, point+`=return("`+s.configPath(point)+`")`)
	}
	s.WithClusterOptions(cluster.WithProcessEnv(func(role string, _ int64) map[string]string {
		env := map[string]string{"GO_FAILPOINTS": "", "GO_FAILPOINTS_HTTP": ""}
		if role == "streamingnode" {
			env["GO_FAILPOINTS"] = strings.Join(points, ";")
		}
		return env
	}))
	// Explicitly exercise the proxy's multi-message transaction path as well
	// as single-message inserts, independently of the split-chunk defaults.
	s.WithMilvusConfig("proxy.splitChunk", "true")
	s.WithMilvusConfig("pulsar.maxMessageSize", "524288")
	s.WithMilvusConfig("pulsar.messageReserveSize", "65536")
	s.WithMilvusConfig("dataCoord.enableCompaction", "false")
	s.WithMilvusConfig("streaming.walBalancer.balancePolicy.minRebalanceIntervalThreshold", "1ms")
	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
}

func (s *FailpointSuite) configPath(point string) string {
	return filepath.Join(s.controlDir, filepath.Base(point)+".json")
}

// arm writes configuration atomically. It selects an exact key and optionally a
// shard, and keeps the one-shot claim outside the process that will be killed.
func (s *FailpointSuite) arm(point, key, vchannel, messageType, action string) string {
	// Keep release files alive until suite teardown has stopped every process.
	dir, err := os.MkdirTemp(s.controlDir, "hit-")
	s.Require().NoError(err)
	config := walpoint.Config{PID: s.Cluster.GetAllStreamingNodes()[0].PID(), Key: key, VChannel: vchannel, MessageType: messageType, Action: action, Directory: dir}
	data, err := json.Marshal(config)
	s.Require().NoError(err)
	path := s.configPath(point)
	s.Require().NoError(os.WriteFile(path+".tmp", data, 0o600))
	s.Require().NoError(os.Rename(path+".tmp", path))
	s.T().Cleanup(func() {
		_ = os.Remove(path)
		_ = os.WriteFile(filepath.Join(dir, "release"), nil, 0o600)
	})
	return dir
}

func (s *FailpointSuite) receipts(dir string, count int) []walpoint.Receipt {
	var receipts []walpoint.Receipt
	s.Require().Eventually(func() bool {
		files, err := filepath.Glob(filepath.Join(dir, "*.json"))
		if err != nil || len(files) != count {
			return false
		}
		receipts = nil
		for _, file := range files {
			data, err := os.ReadFile(file)
			if err != nil {
				return false
			}
			var receipt walpoint.Receipt
			if json.Unmarshal(data, &receipt) != nil {
				return false
			}
			receipts = append(receipts, receipt)
		}
		return true
	}, time.Minute, 20*time.Millisecond, "the selected write must actually reach the failpoint")
	for _, receipt := range receipts {
		s.Require().NotEmpty(receipt.Key)
		s.Require().NotEmpty(receipt.VChannel)
		s.Require().Positive(receipt.PID)
		s.T().Logf("failpoint hit: pid=%d key=%s channel=%s type=%s messageID=%s tick=%d rows=%d",
			receipt.PID, receipt.Key, receipt.VChannel, receipt.MessageType, receipt.MessageID, receipt.TimeTick, len(receipt.IDs))
	}
	return receipts
}

type writeCase struct {
	name        string
	rows, dim   int
	messageType string
}

var writeCases = []writeCase{
	{name: "single", rows: 32, dim: 8, messageType: message.MessageTypeInsert.String()},
	{name: "transaction", rows: 1024, dim: 256, messageType: message.MessageTypeCommitTxn.String()},
}

func (s *FailpointSuite) create(ctx context.Context, tc writeCase, shards int) (*milvuspb.InsertRequest, []string) {
	name := "idempotency_" + funcutil.GenRandomStr()
	schema := integration.ConstructSchema(name, tc.dim, true)
	schema.Fields = append(schema.Fields, &schemapb.FieldSchema{Name: "ordinal", DataType: schemapb.DataType_Int64})
	data, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{
		CollectionName: name, Schema: data, ShardsNum: int32(shards),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	return &milvuspb.InsertRequest{
		CollectionName: name, NumRows: uint32(tc.rows * shards),
		FieldsData: []*schemapb.FieldData{
			integration.NewInt64FieldDataWithStart("ordinal", tc.rows*shards, 0),
			integration.NewFloatVectorFieldData(integration.FloatVecField, tc.rows*shards, tc.dim),
		},
	}, desc.GetVirtualChannelNames()
}

func (s *FailpointSuite) insert(ctx context.Context, req *milvuspb.InsertRequest, key string) (*milvuspb.MutationResult, error) {
	ctx = metadata.AppendToOutgoingContext(ctx, util.HeaderIdempotencyKey, key)
	return s.Cluster.MilvusClient.Insert(ctx, proto.Clone(req).(*milvuspb.InsertRequest))
}

func (s *FailpointSuite) retry(ctx context.Context, req *milvuspb.InsertRequest, key string) []int64 {
	var ids []int64
	s.Require().Eventually(func() bool {
		attempt, cancel := context.WithTimeout(ctx, 15*time.Second)
		defer cancel()
		result, err := s.insert(attempt, req, key)
		if merr.CheckRPCCall(result, err) != nil {
			return false
		}
		ids = result.GetIDs().GetIntId().GetData()
		return len(ids) == int(req.GetNumRows())
	}, 2*time.Minute, 100*time.Millisecond)
	return ids
}

type writeResult struct {
	result *milvuspb.MutationResult
	err    error
}

func (s *FailpointSuite) startInsert(ctx context.Context, req *milvuspb.InsertRequest, key string) <-chan writeResult {
	result := make(chan writeResult, 1)
	go func() {
		response, err := s.insert(ctx, req, key)
		result <- writeResult{response, err}
	}()
	return result
}

func (s *FailpointSuite) assertPending(result <-chan writeResult) {
	select {
	case r := <-result:
		s.FailNow("write completed before releasing its barrier", "%v / %v", r.result, r.err)
	default:
	}
}

func (s *FailpointSuite) assertOriginalIDs(ids []int64, receipts []walpoint.Receipt) {
	seen := make(map[uint32]struct{})
	for _, receipt := range receipts {
		s.Require().NotEmpty(receipt.MessageID, "receipt must follow real persistence")
		s.Require().Positive(receipt.TimeTick)
		s.Require().NotEmpty(receipt.IDs)
		s.Require().Len(receipt.Offsets, len(receipt.IDs))
		for i, offset := range receipt.Offsets {
			s.Require().Less(int(offset), len(ids))
			_, duplicate := seen[offset]
			s.Require().False(duplicate, "each original row belongs to exactly one shard")
			seen[offset] = struct{}{}
			s.Require().Equal(receipt.IDs[i], ids[offset], "original autoID at row offset %d", offset)
		}
	}
}

func (s *FailpointSuite) verify(ctx context.Context, req *milvuspb.InsertRequest, ids []int64, dim int) {
	client := s.Cluster.MilvusClient
	name := req.GetCollectionName()
	flush, err := client.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{name}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	s.WaitForFlush(ctx, flush.GetFlushCollSegIDs()[name].GetData(), flush.GetCollFlushTs()[name], "", name)
	stats, err := client.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(stats, err))
	var rowCount string
	for _, stat := range stats.GetStats() {
		if stat.GetKey() == "row_count" {
			rowCount = stat.GetValue()
		}
	}
	s.Require().Equal(strconv.Itoa(int(req.GetNumRows())), rowCount, "physical rows must not be hidden by primary-key deduplication")
	status, err := client.CreateIndex(ctx, &milvuspb.CreateIndexRequest{
		CollectionName: name, FieldName: integration.FloatVecField,
		ExtraParams: integration.ConstructIndexParam(dim, "FLAT", "L2"),
	})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, integration.FloatVecField)
	status, err = client.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	query, err := client.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: name, Expr: "ordinal >= 0", OutputFields: []string{integration.Int64Field, "ordinal"},
		ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(query, err))
	var pks, ordinals []int64
	for _, field := range query.GetFieldsData() {
		switch field.GetFieldName() {
		case integration.Int64Field:
			pks = field.GetScalars().GetLongData().GetData()
		case "ordinal":
			ordinals = field.GetScalars().GetLongData().GetData()
		}
	}
	s.Require().Len(pks, len(ids))
	s.Require().Len(ordinals, len(ids))
	seen := make(map[int64]struct{})
	for i, ordinal := range ordinals {
		s.Require().GreaterOrEqual(ordinal, int64(0))
		s.Require().Less(ordinal, int64(len(ids)))
		s.Require().Equal(ids[ordinal], pks[i])
		seen[ordinal] = struct{}{}
	}
	s.Require().Len(seen, len(ids), "all original rows must survive exactly once")
	status, err = client.ReleaseCollection(ctx, &milvuspb.ReleaseCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
}

func (s *FailpointSuite) TestBackendAcknowledgementLost() {
	for _, tc := range writeCases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Minute)
			defer cancel()
			req, _ := s.create(ctx, tc, 1)
			key := funcutil.GenRandomStr()
			dir := s.arm(walpoint.AfterBackendAppend, key, "", tc.messageType, "error")
			observed := s.arm(walpoint.AfterIdempotencyAppend, key, "", tc.messageType, "observe")
			first, err := s.insert(ctx, req, key)
			s.Require().NoError(merr.CheckRPCCall(first, err))
			lost := s.receipts(dir, 1)[0]
			committed := s.receipts(observed, 1)
			s.Require().Equal(lost.TimeTick, committed[0].TimeTick, "backend retry must preserve the logical append")
			s.Require().NotEqual(lost.MessageID, committed[0].MessageID, "the backend must actually be retried")
			ids := s.retry(ctx, req, key)
			s.Require().Equal(first.GetIDs().GetIntId().GetData(), ids)
			s.assertOriginalIDs(ids, committed)
			// Also exercise WAL reopening and recovered idempotency state.
			s.restartStreamingNode()
			s.Require().Equal(ids, s.retry(ctx, req, key))
			s.verify(ctx, req, ids, tc.dim)
		})
	}
}

func (s *FailpointSuite) restartStreamingNode(pid ...int) {
	nodes := s.Cluster.GetAllStreamingNodes()
	s.Require().Len(nodes, 1)
	oldID := nodes[0].GetNodeID()
	if len(pid) > 0 {
		s.Require().Equal(pid[0], nodes[0].PID(), "kill the owner that published the receipt")
	}
	_ = nodes[0].Kill() // A SIGKILL exit is expected; Kill waits for process exit.
	s.Require().False(nodes[0].IsWorking())
	s.Require().Eventually(func() bool { return len(s.Cluster.GetAllStreamingNodes()) == 0 }, time.Second, time.Millisecond)
	node := s.Cluster.AddStreamingNode()
	s.Require().NotEqual(oldID, node.GetNodeID())
}

func (s *FailpointSuite) TestCrashBeforeWindowComplete() {
	for _, tc := range writeCases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Minute)
			defer cancel()
			req, _ := s.create(ctx, tc, 1)
			key := funcutil.GenRandomStr()
			dir := s.arm(walpoint.AfterIdempotencyAppend, key, "", tc.messageType, "pause")
			attempt, stop := context.WithCancel(ctx)
			defer stop()
			pending := s.startInsert(attempt, req, key)
			receipts := s.receipts(dir, 1)
			s.assertPending(pending)
			stop() // Do not allow the original RPC to retry after owner replacement.
			s.restartStreamingNode(receipts[0].PID)
			ids := s.retry(ctx, req, key)
			s.assertOriginalIDs(ids, receipts)
			s.Require().Len(receipts[0].IDs, int(req.NumRows))
			s.verify(ctx, req, ids, tc.dim)
		})
	}
}

func (s *FailpointSuite) TestPartialShardCommit() {
	for _, tc := range writeCases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Minute)
			defer cancel()
			req, channels := s.create(ctx, tc, 4)
			s.Require().Len(channels, 4)
			key := funcutil.GenRandomStr()
			blocked := s.arm(walpoint.BeforeIdempotencyAppend, key, channels[0], tc.messageType, "pause")
			written := s.arm(walpoint.AfterIdempotencyAppend, key, "", tc.messageType, "observe")
			attempt, stop := context.WithCancel(ctx)
			defer stop()
			pending := s.startInsert(attempt, req, key)
			before := s.receipts(blocked, 1)
			s.Require().Empty(before[0].MessageID)
			committed := s.receipts(written, 3)
			for _, receipt := range committed {
				s.Require().NotEqual(channels[0], receipt.VChannel)
				s.Require().Len(receipt.IDs, tc.rows)
			}
			s.assertPending(pending)
			stop()
			s.restartStreamingNode(before[0].PID)
			ids := s.retry(ctx, req, key)
			s.assertOriginalIDs(ids, committed)
			s.Require().Equal(ids, s.retry(ctx, req, key))
			s.verify(ctx, req, ids, tc.dim)
		})
	}
}

func (s *FailpointSuite) TestClientTimeoutAfterDurableAppend() {
	for _, tc := range writeCases {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(s.T().Context(), 5*time.Minute)
			defer cancel()
			req, _ := s.create(ctx, tc, 1)
			key := funcutil.GenRandomStr()
			dir := s.arm(walpoint.AfterIdempotencyAppend, key, "", tc.messageType, "pause")
			attempt, stop := context.WithTimeout(ctx, 15*time.Second)
			defer stop()
			pending := s.startInsert(attempt, req, key)
			receipts := s.receipts(dir, 1)
			s.assertPending(pending)
			select {
			case result := <-pending:
				s.Require().Error(merr.CheckRPCCall(result.result, result.err))
				s.Require().ErrorIs(attempt.Err(), context.DeadlineExceeded)
			case <-ctx.Done():
				s.FailNow("client deadline did not finish the request")
			}
			s.Require().NoError(os.WriteFile(filepath.Join(dir, "release"), nil, 0o600))
			ids := s.retry(ctx, req, key)
			s.assertOriginalIDs(ids, receipts)
			for range 3 {
				s.Require().Equal(ids, s.retry(ctx, req, key))
			}
			s.verify(ctx, req, ids, tc.dim)
		})
	}
}

func TestIdempotencyFailpoints(t *testing.T) {
	suite.Run(t, new(FailpointSuite))
}

func TestMain(m *testing.M) {
	os.Exit(fixture.Run(m))
}
