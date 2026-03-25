package milvusclient

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMilvusClusterBuilder(t *testing.T) {
	t.Run("default_values", func(t *testing.T) {
		cluster := NewMilvusClusterBuilder("test-cluster").Build()
		assert.Equal(t, "test-cluster", cluster.ClusterId)
		assert.Equal(t, "localhost:19530", cluster.ConnectionParam.Uri)
		assert.Equal(t, "", cluster.ConnectionParam.Token)
		assert.Empty(t, cluster.Pchannels)
	})

	t.Run("with_all_options", func(t *testing.T) {
		cluster := NewMilvusClusterBuilder("my-cluster").
			WithURI("remote:19531").
			WithToken("my-token").
			WithPchannels("ch1", "ch2").
			Build()

		assert.Equal(t, "my-cluster", cluster.ClusterId)
		assert.Equal(t, "remote:19531", cluster.ConnectionParam.Uri)
		assert.Equal(t, "my-token", cluster.ConnectionParam.Token)
		assert.Equal(t, []string{"ch1", "ch2"}, cluster.Pchannels)
	})

	t.Run("chained_pchannels", func(t *testing.T) {
		cluster := NewMilvusClusterBuilder("c1").
			WithPchannels("ch1").
			WithPchannels("ch2", "ch3").
			Build()

		assert.Equal(t, []string{"ch1", "ch2", "ch3"}, cluster.Pchannels)
	})
}

func TestReplicateConfigurationBuilder(t *testing.T) {
	t.Run("empty_config", func(t *testing.T) {
		req := NewReplicateConfigurationBuilder().Build()
		assert.NotNil(t, req.ReplicateConfiguration)
		assert.Empty(t, req.ReplicateConfiguration.Clusters)
		assert.Empty(t, req.ReplicateConfiguration.CrossClusterTopology)
		assert.False(t, req.ForcePromote)
	})

	t.Run("with_clusters_and_topology", func(t *testing.T) {
		source := NewMilvusClusterBuilder("source").
			WithPchannels("s-ch1").
			WithURI("source:19530").
			WithToken("s-token").
			Build()
		target := NewMilvusClusterBuilder("target").
			WithPchannels("t-ch1").
			WithURI("target:19530").
			WithToken("t-token").
			Build()

		req := NewReplicateConfigurationBuilder().
			WithCluster(source).
			WithCluster(target).
			WithTopology("source", "target").
			Build()

		assert.Len(t, req.ReplicateConfiguration.Clusters, 2)
		assert.Equal(t, "source", req.ReplicateConfiguration.Clusters[0].ClusterId)
		assert.Equal(t, "target", req.ReplicateConfiguration.Clusters[1].ClusterId)
		assert.Len(t, req.ReplicateConfiguration.CrossClusterTopology, 1)
		assert.Equal(t, "source", req.ReplicateConfiguration.CrossClusterTopology[0].SourceClusterId)
		assert.Equal(t, "target", req.ReplicateConfiguration.CrossClusterTopology[0].TargetClusterId)
		assert.False(t, req.ForcePromote)
	})

	t.Run("with_force_promote", func(t *testing.T) {
		cluster := NewMilvusClusterBuilder("standalone").
			WithPchannels("ch1").
			Build()

		req := NewReplicateConfigurationBuilder().
			WithCluster(cluster).
			WithForcePromote().
			Build()

		assert.True(t, req.ForcePromote)
		assert.Len(t, req.ReplicateConfiguration.Clusters, 1)
		assert.Equal(t, "standalone", req.ReplicateConfiguration.Clusters[0].ClusterId)
		assert.Empty(t, req.ReplicateConfiguration.CrossClusterTopology)
	})

	t.Run("without_force_promote", func(t *testing.T) {
		req := NewReplicateConfigurationBuilder().Build()
		assert.False(t, req.ForcePromote)
	})

	t.Run("multiple_topologies", func(t *testing.T) {
		req := NewReplicateConfigurationBuilder().
			WithTopology("a", "b").
			WithTopology("a", "c").
			Build()

		assert.Len(t, req.ReplicateConfiguration.CrossClusterTopology, 2)
		assert.Equal(t, "b", req.ReplicateConfiguration.CrossClusterTopology[0].TargetClusterId)
		assert.Equal(t, "c", req.ReplicateConfiguration.CrossClusterTopology[1].TargetClusterId)
	})
}
