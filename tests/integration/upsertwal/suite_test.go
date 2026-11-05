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

package upsertwal

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/client/handler/producer"
	walpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/failpoint"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	upsertfixture "github.com/milvus-io/milvus/pkg/v3/streaming/util/message/testutils"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration"
	"github.com/milvus-io/milvus/tests/integration/cluster"
	"github.com/milvus-io/milvus/tests/integration/cluster/process"
	fixture "github.com/milvus-io/milvus/tests/integration/failpoint"
)

type upsertWALFixture struct {
	integration.MiniClusterSuite
	controlDir string
}

type (
	UpsertWALSuite            struct{ upsertWALFixture }
	UpsertWALCASDisabledSuite struct{ upsertWALFixture }
)

func TestUpsertWAL(t *testing.T) { suite.Run(t, new(UpsertWALSuite)) }

func TestUpsertWALCASDisabled(t *testing.T) { suite.Run(t, new(UpsertWALCASDisabledSuite)) }

func (s *UpsertWALCASDisabledSuite) SetupSuite() {
	s.WithMilvusConfig("streaming.partialUpdate.enableCAS", "false")
	s.upsertWALFixture.SetupSuite()
}

func TestMain(m *testing.M) { os.Exit(fixture.Run(m)) }

func (s *upsertWALFixture) SetupSuite() {
	s.controlDir = s.T().TempDir()
	s.WithClusterOptions(cluster.WithProcessEnv(func(role string, _ int64) map[string]string {
		if role != "streamingnode" {
			return nil
		}
		return map[string]string{"GO_FAILPOINTS": strings.Join([]string{
			walpoint.AfterBackendAppend + `=return("` + filepath.Join(s.controlDir, "backend.json") + `")`,
			walpoint.BeforeIdempotencyAppend + `=return("` + filepath.Join(s.controlDir, "commit.json") + `")`,
		}, ";")}
	}))
	s.WithMilvusConfig("streaming.wal.upsert.unifiedEnabled", "true")
	s.WithMilvusConfig("function.enableWriteBeforeMaterialization", "true")
	s.WithMilvusConfig("common.session.ttl", "10")
	s.WithMilvusConfig("metrics.port", "0")
	s.WithMilvusConfig("dataCoord.enableCompaction", "true")
	s.WithMilvusConfig("dataCoord.compaction.enableAutoCompaction", "false")
	s.WithMilvusConfig("pulsar.maxMessageSize", "524288")
	s.WithMilvusConfig("pulsar.messageReserveSize", "65536")
	s.WithMilvusConfig("streaming.walBalancer.balancePolicy.minRebalanceIntervalThreshold", "1ms")
	s.WithOptions(integration.WithDropAllCollectionsWhenTestTearDown())
	s.MiniClusterSuite.SetupSuite()
}

type collection struct {
	name, vchannel string
	id, partition  int64
	schema         *schemapb.CollectionSchema
}

func (s *upsertWALFixture) create(ctx context.Context) collection {
	return s.createWithPK(ctx, schemapb.DataType_Int64, false)
}

func (s *upsertWALFixture) createWithPK(ctx context.Context, pkType schemapb.DataType, autoID bool, configure ...func(*schemapb.CollectionSchema)) collection {
	name := "native_upsert_" + funcutil.GenRandomStr()
	schema := &schemapb.CollectionSchema{Name: name, Fields: []*schemapb.FieldSchema{
		{Name: "pk", DataType: pkType, IsPrimaryKey: true, AutoID: autoID},
		{Name: "marker", DataType: schemapb.DataType_Int64},
		{Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}},
	}}
	if pkType == schemapb.DataType_VarChar {
		schema.Fields[0].TypeParams = []*commonpb.KeyValuePair{{Key: "max_length", Value: "64"}}
	}
	for _, apply := range configure {
		apply(schema)
	}
	payload, err := proto.Marshal(schema)
	s.Require().NoError(err)
	status, err := s.Cluster.MilvusClient.CreateCollection(ctx, &milvuspb.CreateCollectionRequest{CollectionName: name, Schema: payload, ShardsNum: 1})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	desc, err := s.Cluster.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(desc, err))
	partitions, err := s.Cluster.MilvusClient.ShowPartitions(ctx, &milvuspb.ShowPartitionsRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(partitions, err))
	s.Require().Len(partitions.GetPartitionIDs(), 1)
	status, err = s.Cluster.MilvusClient.CreateIndex(ctx, &milvuspb.CreateIndexRequest{CollectionName: name, FieldName: "vector", ExtraParams: integration.ConstructIndexParam(2, "FLAT", "L2")})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForIndexBuilt(ctx, name, "vector")
	status, err = s.Cluster.MilvusClient.LoadCollection(ctx, &milvuspb.LoadCollectionRequest{CollectionName: name})
	s.Require().NoError(merr.CheckRPCCall(status, err))
	s.WaitForLoad(ctx, name)
	return collection{name: name, id: desc.GetCollectionID(), vchannel: desc.GetVirtualChannelNames()[0], partition: partitions.GetPartitionIDs()[0], schema: desc.GetSchema()}
}

var rowSequence atomic.Int64

func (c collection) upsert(pk, marker int64, key ...string) message.MutableMessage {
	fields := make([]*schemapb.FieldData, 0, 3)
	for _, field := range c.schema.GetFields() {
		data := &schemapb.FieldData{FieldId: field.GetFieldID(), FieldName: field.GetName(), Type: field.GetDataType()}
		switch field.GetName() {
		case "pk", "marker":
			value := pk
			if field.GetName() == "marker" {
				value = marker
			}
			data.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{value}}}}}
			if field.GetDataType() == schemapb.DataType_VarChar {
				data.Field = &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: []string{strconv.FormatInt(value, 10)}}}}}
			}
		case "vector":
			data.Field = &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}}}}}
		default:
			continue
		}
		fields = append(fields, data)
	}
	version := c.schema.GetVersion()
	rowID := (1 << 62) + rowSequence.Add(1)
	keys := &schemapb.IDs{IdField: &schemapb.IDs_IntId{IntId: &schemapb.LongArray{Data: []int64{pk}}}}
	for _, field := range c.schema.GetFields() {
		if !field.GetIsPrimaryKey() {
			continue
		}
		if field.GetAutoID() {
			rowID = pk
		}
		if field.GetDataType() == schemapb.DataType_VarChar {
			keys = &schemapb.IDs{IdField: &schemapb.IDs_StrId{StrId: &schemapb.StringArray{Data: []string{strconv.FormatInt(pk, 10)}}}}
		}
	}
	idempotencyKey := ""
	if len(key) > 0 {
		idempotencyKey = key[0]
	}
	var mapping *messagespb.UpsertRequestRowMapping
	if idempotencyKey != "" {
		mapping = &messagespb.UpsertRequestRowMapping{RowOffsets: []uint32{0}}
	}
	return message.NewUpsertMessageBuilderV2().WithVChannel(c.vchannel).WithIdempotencyKey(message.NewCollectionScopedIdempotencyKey(c.id, idempotencyKey)).
		WithHeader(&message.UpsertMessageHeader{
			CollectionId: c.id, SchemaVersion: &version, DeleteRows: 1,
			Partitions: []*message.PartitionSegmentAssignment{{PartitionId: c.partition, Rows: 1, BinarySize: 32}},
		}).
		WithBody(func() *message.UpsertMessageBody {
			body := upsertfixture.Body(&msgpb.InsertRequest{CollectionID: c.id, NumRows: 1, RowIDs: []int64{rowID}, FieldsData: fields}, &msgpb.DeleteRequest{PartitionID: -1, PrimaryKeys: keys})
			body.RequestRowMapping = mapping
			return body
		}()).MustBuildMutable()
}

func (s *upsertWALFixture) assertRows(ctx context.Context, c collection, pk, marker int64, expected int) {
	result, err := s.Cluster.MilvusClient.Query(ctx, &milvuspb.QueryRequest{
		CollectionName: c.name,
		Expr:           c.rowExpression(pk, marker), OutputFields: []string{"pk", "marker"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
	})
	s.Require().NoError(merr.CheckRPCCall(result, err))
	for _, field := range result.GetFieldsData() {
		s.Require().Equal(expected, len(field.GetScalars().GetLongData().GetData())+len(field.GetScalars().GetStringData().GetData()), "query must check the old and new row content")
	}
	s.Require().Len(result.GetFieldsData(), 2)
}

func (c collection) rowExpression(pk, marker int64) string {
	for _, field := range c.schema.GetFields() {
		if field.GetIsPrimaryKey() && field.GetDataType() == schemapb.DataType_VarChar {
			return fmt.Sprintf("pk == %q && marker == %d", strconv.FormatInt(pk, 10), marker)
		}
	}
	return fmt.Sprintf("pk == %d && marker == %d", pk, marker)
}

func (s *upsertWALFixture) flush(ctx context.Context, c collection) {
	flush, err := s.Cluster.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{c.name}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	s.WaitForFlush(ctx, flush.GetFlushCollSegIDs()[c.name].GetData(), flush.GetCollFlushTs()[c.name], "", c.name)
}

func (s *upsertWALFixture) assertPhysicalRows(ctx context.Context, c collection, rows int64) {
	s.flush(ctx, c)
	statistics, err := s.Cluster.MilvusClient.GetCollectionStatistics(ctx, &milvuspb.GetCollectionStatisticsRequest{CollectionName: c.name})
	s.Require().NoError(merr.CheckRPCCall(statistics, err))
	for _, item := range statistics.GetStats() {
		if item.GetKey() == "row_count" {
			s.Require().Equal(strconv.FormatInt(rows, 10), item.GetValue(), "automatic compaction is disabled; duplicate inserts must not hide behind PK reduction")
			return
		}
	}
	s.FailNow("collection statistics have no row_count")
}

type testProducer struct {
	producer.Producer
	closeOnce sync.Once
}

func (p *testProducer) Close() { p.closeOnce.Do(p.Producer.Close) }

// rawProducer uses the real Produce stream and the current assignment term.
// Tests recreate this producer after a StreamingNode restart.
func rawProducer(t *testing.T, ctx context.Context, c *cluster.MiniClusterV3, vchannel string) producer.Producer {
	t.Helper()
	pchannel := funcutil.ToPhysicalChannel(vchannel)
	var cli producer.Producer
	var conn *grpc.ClientConn
	require.Eventually(t, func() bool {
		var meta streamingpb.PChannelMeta
		response, err := c.EtcdCli.Get(ctx, path.Join(c.RootPath(), "meta", "streamingcoord-meta", "pchannel", pchannel))
		if err != nil || len(response.Kvs) != 1 || proto.Unmarshal(response.Kvs[0].Value, &meta) != nil ||
			meta.GetChannel().GetTerm() <= 0 || meta.GetNode().GetAddress() == "" {
			return false
		}
		probe, cancel := context.WithTimeout(ctx, time.Second)
		defer cancel()
		conn, err = process.DailGRPClient(probe, meta.GetNode().GetAddress(), c.RootPath(), meta.GetNode().GetServerId())
		if err != nil {
			return false
		}
		cli, err = producer.CreateProducer(ctx, &producer.ProducerOptions{Assignment: &types.PChannelInfoAssigned{
			Channel: types.NewPChannelInfoFromProto(meta.GetChannel()),
			Node:    types.NewStreamingNodeInfoFromProto(meta.GetNode()),
		}}, streamingpb.NewStreamingNodeHandlerServiceClient(conn))
		if err != nil {
			_ = conn.Close()
			return false
		}
		return true
	}, 30*time.Second, 100*time.Millisecond, "the PChannel must have a current owner")
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	wrapped := &testProducer{Producer: cli}
	t.Cleanup(wrapped.Close)
	return wrapped
}

func (s *upsertWALFixture) restartStreamingNode(collections ...collection) {
	nodes := s.Cluster.GetAllStreamingNodes()
	s.Require().Len(nodes, 1)
	_ = nodes[0].Kill()
	s.Require().False(nodes[0].IsWorking())
	s.Cluster.AddStreamingNode()
	for _, c := range collections {
		s.waitForQueryRecovery(s.T().Context(), c)
	}
}

func (s *upsertWALFixture) waitForQueryRecovery(ctx context.Context, c collection) {
	s.Require().Eventually(func() bool {
		attempt, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()
		response, err := s.Cluster.MilvusClient.Query(attempt, &milvuspb.QueryRequest{
			CollectionName: c.name, Expr: "marker >= 0", OutputFields: []string{"marker"},
			QueryParams: []*commonpb.KeyValuePair{{Key: "limit", Value: "1"}}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong,
		})
		return merr.CheckRPCCall(response, err) == nil
	}, 90*time.Second, 100*time.Millisecond, "query routing must recover after the old StreamingNode lease expires")
}
