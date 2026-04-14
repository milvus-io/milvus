package replication

import (
	"context"
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/suite"
	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/tests/integration"
)

type DropReplicateConfigSuite struct {
	integration.MiniClusterSuite
}

func TestDropReplicateConfig(t *testing.T) {
	suite.Run(t, new(DropReplicateConfigSuite))
}

func (s *DropReplicateConfigSuite) getPChannelNames() []string {
	rootCoordDml := paramtable.Get().CommonCfg.RootCoordDml.GetValue()
	dmlChannelNum := paramtable.Get().RootCoordCfg.DmlChannelNum.GetAsInt()
	pchannels := make([]string, dmlChannelNum)
	for i := 0; i < dmlChannelNum; i++ {
		pchannels[i] = fmt.Sprintf("%s_%d", rootCoordDml, i)
	}
	return pchannels
}

// TestDropAfterSetup sets a single-cluster config, then drops it, then sets a new one.
func (s *DropReplicateConfigSuite) TestDropAfterSetup() {
	ctx := context.Background()
	clusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
	pchannels := s.getPChannelNames()

	// Step 1: Set up single-cluster config
	config := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId:       clusterID,
				Pchannels:       pchannels,
				ConnectionParam: &commonpb.ConnectionParam{Uri: "http://localhost:19530", Token: "test"},
			},
		},
	}
	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: config,
	})
	s.NoError(err)
	s.NoError(merr.Error(resp))

	// Step 2: Drop with empty config
	resp, err = s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{},
	})
	s.NoError(err)
	s.NoError(merr.Error(resp))

	// Verify replicate config is nil via API
	getCfgResp, err := s.Cluster.MilvusClient.GetReplicateConfiguration(ctx, &milvuspb.GetReplicateConfigurationRequest{})
	s.NoError(err)
	s.NoError(merr.Error(getCfgResp.GetStatus()))
	s.Nil(getCfgResp.GetConfiguration(), "replicate config should be nil after drop")

	// Verify replicate config key is removed from etcd
	rootPath := s.Cluster.RootPath()
	configKey := path.Join(rootPath, streamingcoord.ReplicateConfigurationKey)
	getResp, err := s.Cluster.EtcdCli.Get(ctx, configKey)
	s.NoError(err)
	s.Equal(int64(0), getResp.Count, "replicate config key should be removed after drop")

	// Verify all replicate pchannel meta keys are removed from etcd
	pchannelPrefix := path.Join(rootPath, streamingcoord.ReplicatePChannelMetaPrefix)
	listResp, err := s.Cluster.EtcdCli.Get(ctx, pchannelPrefix, clientv3.WithPrefix(), clientv3.WithCountOnly())
	s.NoError(err)
	s.Equal(int64(0), listResp.Count, "replicate pchannel meta keys should be removed after drop")

	// Step 3: Drop again (idempotent)
	resp, err = s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{},
	})
	s.NoError(err)
	s.NoError(merr.Error(resp))

	// Step 4: Set up new config after drop (proves no stale state)
	newConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{
				ClusterId:       clusterID,
				Pchannels:       pchannels,
				ConnectionParam: &commonpb.ConnectionParam{Uri: "http://localhost:19530", Token: "test"},
			},
		},
	}
	resp, err = s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: newConfig,
	})
	s.NoError(err)
	s.NoError(merr.Error(resp))
}

// TestDropWithoutConfig drops when there's no config - should be idempotent.
func (s *DropReplicateConfigSuite) TestDropWithoutConfig() {
	ctx := context.Background()

	resp, err := s.Cluster.MilvusClient.UpdateReplicateConfiguration(ctx, &milvuspb.UpdateReplicateConfigurationRequest{
		ReplicateConfiguration: &commonpb.ReplicateConfiguration{},
	})
	s.NoError(err)
	s.NoError(merr.Error(resp))
}
