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
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	walpoint "github.com/milvus-io/milvus/internal/streamingnode/server/wal/failpoint"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/tests/integration/cluster"
)

// A separate source cluster prevents topology changes from affecting other tests.
type UpsertWALCDCSuite struct{ upsertWALFixture }

func TestUpsertWALCDC(t *testing.T) { suite.Run(t, new(UpsertWALCDCSuite)) }

func startCDC(t *testing.T, ctx context.Context, workDir string, c *cluster.MiniClusterV3) *exec.Cmd {
	t.Helper()
	logDir := os.Getenv("MILVUS_UPSERT_WAL_LOG_DIR")
	if logDir == "" {
		logDir = t.TempDir()
	}
	// #nosec G703 -- The test runner selects this private artifact directory.
	require.NoError(t, os.MkdirAll(logDir, 0o700))
	logPath := filepath.Join(logDir, "cdc-"+c.RootPath()+".log")
	// #nosec G703 -- The runner selects the directory and the fixture creates the cluster name.
	output, err := os.Create(logPath)
	require.NoError(t, err)
	// #nosec G204 -- The test runner selects the trusted binary and cluster alias.
	cmd := exec.CommandContext(ctx, filepath.Join(workDir, "bin", "milvus"), "run", "cdc", "-alias", c.RootPath())
	cmd.Dir = workDir
	cmd.Env = append(os.Environ(),
		"ETCD_ROOTPATH="+c.RootPath(), "ETCD_ENDPOINTS="+strings.Join(c.EtcdCli.Endpoints(), ","),
		"MSGCHANNEL_CHANNAMEPREFIX_CLUSTER="+c.RootPath(), "MINIO_ROOTPATH="+c.RootPath(),
		"MQ_TYPE=pulsar", "METRICS_PORT=0", "LOG_LEVEL=info")
	cmd.Stdout, cmd.Stderr = output, output
	require.NoError(t, cmd.Start())
	t.Logf("CDC process pid=%d log=%s", cmd.Process.Pid, logPath)
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGCONT)
		_ = cmd.Process.Signal(syscall.SIGTERM)
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		_ = output.Close()
	})
	return cmd
}

func applyTopology(ctx context.Context, clients []milvuspb.MilvusServiceClient, config *commonpb.ReplicateConfiguration) error {
	completed := make(chan error, len(clients))
	for _, client := range clients {
		go func(client milvuspb.MilvusServiceClient) {
			response, err := client.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{ReplicateConfiguration: proto.Clone(config).(*commonpb.ReplicateConfiguration)})
			completed <- merr.CheckRPCCall(response, err)
		}(client)
	}
	var firstError error
	for range clients {
		if err := <-completed; err != nil && firstError == nil {
			firstError = err
		}
	}
	return firstError
}

func (s *UpsertWALCDCSuite) TestReplicationAndSwitchover() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	port := funcutil.GetAvailablePort()
	targetControl := filepath.Join(s.T().TempDir(), "backend.json")
	target := cluster.NewMiniClusterV3(ctx, cluster.WithWorkDir(s.WorkDir()), cluster.WithEtcdCli(s.Cluster.EtcdCli), cluster.WithProcessEnv(func(role string, _ int64) map[string]string {
		if role != "streamingnode" {
			return nil
		}
		return map[string]string{"GO_FAILPOINTS": walpoint.AfterBackendAppend + `=return("` + targetControl + `")`}
	}), cluster.WithExtraEnv(map[string]string{
		"PROXY_PORT": strconv.Itoa(port), "PROXY_INTERNALPORT": strconv.Itoa(funcutil.GetAvailablePort()),
		"ROOTCOORD_PORT":                    strconv.Itoa(funcutil.GetAvailablePort()),
		"STREAMING_WAL_ENABLEUPSERTMESSAGE": "true", "METRICS_PORT": "0",
		"COMMON_SESSION_TTL": "10",
		"STREAMING_WALBALANCER_BALANCEPOLICY_MINREBALANCEINTERVALTHRESHOLD": "1ms",
	}))
	defer target.Stop()
	conn, err := grpc.NewClient(fmt.Sprintf("127.0.0.1:%d", port), grpc.WithTransportCredentials(insecure.NewCredentials()))
	s.Require().NoError(err)
	defer conn.Close()
	// MiniClusterV3's public client uses 19530. The test owns the second endpoint.
	target.MilvusClient = milvuspb.NewMilvusServiceClient(conn)
	sourceCDC := startCDC(s.T(), ctx, s.WorkDir(), s.Cluster)
	startCDC(s.T(), ctx, s.WorkDir(), target)
	clusterConfig := func(c *cluster.MiniClusterV3, port int) *commonpb.MilvusCluster {
		return &commonpb.MilvusCluster{ClusterId: c.RootPath(), Pchannels: []string{c.RootPath() + "-rootcoord-dml_0", c.RootPath() + "-rootcoord-dml_1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: fmt.Sprintf("http://127.0.0.1:%d", port)}}
	}
	config := &commonpb.ReplicateConfiguration{
		Clusters:             []*commonpb.MilvusCluster{clusterConfig(s.Cluster, 19530), clusterConfig(target, port)},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{{SourceClusterId: s.Cluster.RootPath(), TargetClusterId: target.RootPath()}},
	}
	clients := []milvuspb.MilvusServiceClient{s.Cluster.MilvusClient, target.MilvusClient}
	s.Require().NoError(applyTopology(ctx, clients, config))
	c := s.create(ctx)
	var targetDescription *milvuspb.DescribeCollectionResponse
	s.Require().Eventually(func() bool {
		response, err := target.MilvusClient.DescribeCollection(ctx, &milvuspb.DescribeCollectionRequest{CollectionName: c.name})
		if merr.CheckRPCCall(response, err) != nil {
			return false
		}
		targetDescription = response
		return response.GetCollectionID() == c.id
	}, time.Minute, 100*time.Millisecond)
	s.Require().Eventually(func() bool {
		response, err := target.MilvusClient.GetLoadingProgress(ctx, &milvuspb.GetLoadingProgressRequest{CollectionName: c.name})
		return merr.CheckRPCCall(response, err) == nil && response.GetProgress() == 100
	}, time.Minute, 100*time.Millisecond, "the primary Load configuration must reach the secondary through WAL")
	producer := rawProducer(s.T(), ctx, s.Cluster, c.vchannel)
	initial := message.MustAsMutableUpsertMessageV2(c.upsert(11, 1))
	insert := message.NewInsertMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.InsertMessageHeaderV2{CollectionId: c.id, Partitions: initial.Header().Partitions, SchemaVersion: initial.Header().SchemaVersion}).
		WithBody(&message.InsertMessageBody{Rows: initial.MustBody().GetRows()}).MustBuildMutable()
	_, err = producer.Append(ctx, insert)
	s.Require().NoError(err)
	s.replicatedRows(ctx, target.MilvusClient, c, 0, 1)
	_, err = producer.Append(ctx, c.upsert(11, 2))
	s.Require().NoError(err)
	s.assertRows(ctx, c, 11, 1, 0)
	s.assertRows(ctx, c, 11, 2, 1)
	s.replicatedRows(ctx, target.MilvusClient, c, 1, 2)
	targetNodes := target.GetAllStreamingNodes()
	s.Require().Len(targetNodes, 1)
	prefixDir := s.T().TempDir()
	prefixConfig, err := json.Marshal(walpoint.Config{PID: targetNodes[0].PID(), MessageType: message.MessageTypeUpsert.String(), Action: "pause", Directory: prefixDir})
	s.Require().NoError(err)
	s.Require().NoError(os.WriteFile(targetControl, prefixConfig, 0o600))
	begin, err := producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	s.Require().NotNil(begin.TxnCtx)
	_, err = producer.Append(ctx, c.upsert(11, 3).WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	var receipt walpoint.Receipt
	s.Require().Eventually(func() bool {
		files, err := filepath.Glob(filepath.Join(prefixDir, "*.json"))
		if err != nil || len(files) != 1 {
			return false
		}
		data, err := os.ReadFile(files[0])
		return err == nil && json.Unmarshal(data, &receipt) == nil && receipt.MessageID != "" && receipt.TimeTick > 0
	}, time.Minute, 10*time.Millisecond, "the secondary must persist the body before the crash, without receiving Commit")
	s.Require().Equal(targetNodes[0].PID(), receipt.PID)
	_ = targetNodes[0].Kill()
	s.Require().NoError(os.Remove(targetControl))
	target.AddStreamingNode()
	s.replicatedRows(ctx, target.MilvusClient, c, 2, 3)
	// Stop the sender after the next receiving body becomes durable. Local Flush
	// must abort that prefix, and replay must rebuild it from Begin after resume.
	targetNodes = target.GetAllStreamingNodes()
	s.Require().Len(targetNodes, 1)
	flushPrefixDir := s.T().TempDir()
	prefixConfig, err = json.Marshal(walpoint.Config{PID: targetNodes[0].PID(), MessageType: message.MessageTypeUpsert.String(), Action: "pause", Directory: flushPrefixDir})
	s.Require().NoError(err)
	s.Require().NoError(os.WriteFile(targetControl, prefixConfig, 0o600))
	begin, err = producer.Append(ctx, message.NewBeginTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.BeginTxnMessageHeader{KeepaliveMilliseconds: 600000}).WithBody(&message.BeginTxnMessageBody{}).MustBuildMutable())
	s.Require().NoError(err)
	s.Require().NotNil(begin.TxnCtx)
	_, err = producer.Append(ctx, c.upsert(11, 4).WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	_, err = producer.Append(ctx, message.NewCommitTxnMessageBuilderV2().WithVChannel(c.vchannel).
		WithHeader(&message.CommitTxnMessageHeader{BodyCnt: 1}).WithBody(&message.CommitTxnMessageBody{}).MustBuildMutable().WithTxnContext(*begin.TxnCtx))
	s.Require().NoError(err)
	s.Require().Eventually(func() bool {
		files, err := filepath.Glob(filepath.Join(flushPrefixDir, "*.json"))
		if err != nil || len(files) != 1 {
			return false
		}
		data, err := os.ReadFile(files[0])
		return err == nil && json.Unmarshal(data, &receipt) == nil && receipt.MessageID != ""
	}, time.Minute, 10*time.Millisecond)
	s.Require().NoError(sourceCDC.Process.Signal(syscall.SIGSTOP))
	defer func() { _ = sourceCDC.Process.Signal(syscall.SIGCONT) }()
	s.Require().NoError(os.WriteFile(filepath.Join(flushPrefixDir, "release"), nil, 0o600))
	flush, err := target.MilvusClient.Flush(ctx, &milvuspb.FlushRequest{CollectionNames: []string{c.name}})
	s.Require().NoError(merr.CheckRPCCall(flush, err))
	s.Require().NoError(os.Remove(targetControl))
	s.Require().NoError(sourceCDC.Process.Signal(syscall.SIGCONT))
	s.replicatedRows(ctx, target.MilvusClient, c, 3, 4)
	producer.Close()
	reverse := proto.Clone(config).(*commonpb.ReplicateConfiguration)
	reverse.CrossClusterTopology[0].SourceClusterId = target.RootPath()
	reverse.CrossClusterTopology[0].TargetClusterId = s.Cluster.RootPath()
	s.Require().NoError(applyTopology(ctx, clients, reverse))
	defer func() {
		restore, stop := context.WithTimeout(context.Background(), time.Minute)
		defer stop()
		s.NoError(applyTopology(restore, clients, config))
	}()
	newPrimary := c
	newPrimary.vchannel, newPrimary.schema = targetDescription.GetVirtualChannelNames()[0], targetDescription.GetSchema()
	newProducer := rawProducer(s.T(), ctx, target, newPrimary.vchannel)
	_, err = newProducer.Append(ctx, newPrimary.upsert(11, 5))
	s.Require().NoError(err)
	s.replicatedRows(ctx, target.MilvusClient, c, 4, 5)
	s.replicatedRows(ctx, s.Cluster.MilvusClient, c, 4, 5)
}

func (s *UpsertWALCDCSuite) replicatedRows(ctx context.Context, client milvuspb.MilvusServiceClient, c collection, oldMarker, newMarker int64) {
	s.Require().Eventually(func() bool {
		for _, marker := range []int64{oldMarker, newMarker} {
			response, err := client.Query(ctx, &milvuspb.QueryRequest{CollectionName: c.name, Expr: fmt.Sprintf("pk == 11 && marker == %d", marker), OutputFields: []string{"pk", "marker"}, ConsistencyLevel: commonpb.ConsistencyLevel_Strong})
			if merr.CheckRPCCall(response, err) != nil || len(response.GetFieldsData()) != 2 {
				return false
			}
			expected := 0
			if marker == newMarker {
				expected = 1
			}
			for _, field := range response.GetFieldsData() {
				if len(field.GetScalars().GetLongData().GetData()) != expected {
					return false
				}
			}
		}
		return true
	}, time.Minute, 100*time.Millisecond, "CDC must remove the old row and preserve exactly one new row")
}
