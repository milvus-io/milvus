package datacoord

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"slices"
	"strings"
	"time"

	"github.com/tidwall/gjson"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/datacoord/ddl"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/replicateutil"
)

type splitRetirement struct {
	Cluster      string `json:"cluster"`
	Topology     string `json:"topology"`
	CollectionID int64  `json:"collection_id"`
	TaskID       int64  `json:"task_id"`
	Source       string `json:"source"`
	Retired      bool   `json:"retired"`
}

// Identify members, directions and physical channel slots. Connection settings
// are local to each site. The primary holds its shared cluster resource key
// across this live check and the subsequent append, excluding topology changes.
func splitTopologyID(cfg *commonpb.ReplicateConfiguration) (string, error) {
	version := proto.Clone(cfg).(*commonpb.ReplicateConfiguration)
	for _, member := range version.Clusters {
		member.ConnectionParam = nil
	}
	slices.SortFunc(version.Clusters, func(a, b *commonpb.MilvusCluster) int { return strings.Compare(a.GetClusterId(), b.GetClusterId()) })
	slices.SortFunc(version.CrossClusterTopology, func(a, b *commonpb.CrossClusterTopology) int {
		return strings.Compare(a.GetTargetClusterId(), b.GetTargetClusterId())
	})
	data, err := proto.MarshalOptions{Deterministic: true}.Marshal(version)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

func (s *Server) splitRetirementProgress(ctx context.Context, _ *milvuspb.GetMetricsRequest, query gjson.Result) (string, error) {
	cfg, err := ddl.GetReplicationTopology(ctx)
	if err != nil {
		return "", err
	}
	if cfg == nil || s.splitStore == nil {
		return "", merr.WrapErrServiceNotReadyMsg("split replication metadata is unavailable")
	}
	version, err := splitTopologyID(cfg)
	if err != nil {
		return "", err
	}
	task := s.splitStore.Get(query.Get("task_id").Int())
	result := splitRetirement{
		Cluster: Params.CommonCfg.ClusterPrefix.GetValue(), Topology: version,
		CollectionID: task.GetCollectionId(), TaskID: task.GetTaskId(), Source: task.GetSourceVchannel(),
		Retired: task.GetState() == datapb.SplitShardTaskState_SplitShardTaskRetired && split.HasSourceFenceProof(task),
	}
	payload, err := json.Marshal(result)
	return string(payload), err
}

func (s *Server) checkSplitParentRetired(ctx context.Context, parent *datapb.SplitShardTask) error {
	if !split.HasSourceFenceProof(parent) {
		return merr.WrapErrServiceNotReadyMsg("source fence for parent split %d remains unconfirmed", parent.GetTaskId())
	}
	cfg, err := ddl.GetReplicationTopology(ctx)
	if err != nil || len(cfg.GetClusters()) <= 1 {
		return err
	}
	return checkSplitPeers(ctx, cfg, Params.CommonCfg.ClusterPrefix.GetValue(), parent)
}

func checkSplitPeers(ctx context.Context, cfg *commonpb.ReplicateConfiguration, local string, parent *datapb.SplitShardTask) error {
	h, err := replicateutil.NewConfigHelper(local, cfg)
	if err != nil {
		return err
	}
	version, err := splitTopologyID(cfg)
	if err != nil {
		return err
	}
	sourceChannel, collection, index, err := funcutil.ParseVChannel(parent.GetSourceVchannel())
	if err != nil {
		return err
	}
	checkCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	group, peerCtx := errgroup.WithContext(checkCtx)
	for _, member := range cfg.GetClusters() {
		if member.GetClusterId() == local {
			continue
		}
		group.Go(func() error {
			channel, err := h.GetCurrentCluster().GetTargetChannel(sourceChannel, member.GetClusterId())
			if err != nil {
				return err
			}
			want := splitRetirement{
				Cluster: member.GetClusterId(), Topology: version, CollectionID: parent.GetCollectionId(),
				TaskID: parent.GetTaskId(), Source: funcutil.GetVirtualChannel(channel, collection, index), Retired: true,
			}
			return verifyPeerRetirement(peerCtx, member, want)
		})
	}
	return group.Wait()
}

func verifyPeerRetirement(ctx context.Context, peer *commonpb.MilvusCluster, want splitRetirement) error {
	client, err := cluster.NewPeerClient(ctx, peer)
	if err != nil {
		return err
	}
	defer client.Close(ctx)
	req, err := metricsinfo.ConstructGetMetricsRequest(map[string]interface{}{metricsinfo.MetricTypeKey: metricsinfo.SplitRetirementKey, "task_id": want.TaskID})
	if err != nil {
		return err
	}
	resp, err := client.GetService().GetMetrics(ctx, req)
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return err
	}
	return want.check(resp.GetResponse())
}

func (want splitRetirement) check(payload string) error {
	var got splitRetirement
	if err := json.Unmarshal([]byte(payload), &got); err != nil {
		return err
	}
	if got != want {
		return merr.WrapErrServiceNotReadyMsg("cluster %s has not confirmed retirement of split %d source %s in the current topology", want.Cluster, want.TaskID, want.Source)
	}
	return nil
}
