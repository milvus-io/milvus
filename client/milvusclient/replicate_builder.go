package milvusclient

import "github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

// MilvusClusterBuilder defines the interface for building Milvus cluster configuration
type MilvusClusterBuilder interface {
	// WithPchannels adds physical channels
	WithPchannels(pchannels ...string) MilvusClusterBuilder

	// WithToken sets the authentication token
	WithToken(token string) MilvusClusterBuilder

	// WithURI sets the connection URI
	WithURI(uri string) MilvusClusterBuilder

	// Build constructs and returns the MilvusCluster object
	Build() *commonpb.MilvusCluster
}

// ReplicateConfigurationBuilder defines the interface for building replicate configuration
type ReplicateConfigurationBuilder interface {
	// WithCluster adds a cluster configuration, you can use MilvusClusterBuilder to create a cluster
	WithCluster(cluster *commonpb.MilvusCluster) ReplicateConfigurationBuilder

	// WithTopology adds a cross-cluster topology configuration
	WithTopology(sourceClusterID, targetClusterID string) ReplicateConfigurationBuilder

	// Build constructs and returns the configuration object
	Build() *commonpb.ReplicateConfiguration
}

// NewMilvusClusterBuilder creates a new Milvus cluster builder
func NewMilvusClusterBuilder(clusterID string) MilvusClusterBuilder {
	return newMilvusClusterBuilder(clusterID)
}

// NewReplicateConfigurationBuilder creates a new replicate configuration builder
func NewReplicateConfigurationBuilder() ReplicateConfigurationBuilder {
	return newReplicateConfigurationBuilder()
}

type milvusClusterBuilder struct {
	clusterID string
	uri       string
	token     string
	pchannels []string
}

func newMilvusClusterBuilder(clusterID string) MilvusClusterBuilder {
	return &milvusClusterBuilder{
		clusterID: clusterID,
		uri:       "localhost:19530", // default URI
		token:     "",                // default empty token
		pchannels: []string{},
	}
}

func (b *milvusClusterBuilder) WithPchannels(pchannels ...string) MilvusClusterBuilder {
	b.pchannels = append(b.pchannels, pchannels...)
	return b
}

func (b *milvusClusterBuilder) WithToken(token string) MilvusClusterBuilder {
	b.token = token
	return b
}

func (b *milvusClusterBuilder) WithURI(uri string) MilvusClusterBuilder {
	b.uri = uri
	return b
}

func (b *milvusClusterBuilder) Build() *commonpb.MilvusCluster {
	return &commonpb.MilvusCluster{
		ClusterId: b.clusterID,
		ConnectionParam: &commonpb.ConnectionParam{
			Uri:   b.uri,
			Token: b.token,
		},
		Pchannels: b.pchannels,
	}
}

type replicateConfigurationBuilder struct {
	config *commonpb.ReplicateConfiguration
}

func newReplicateConfigurationBuilder() ReplicateConfigurationBuilder {
	return &replicateConfigurationBuilder{
		config: &commonpb.ReplicateConfiguration{
			Clusters:             []*commonpb.MilvusCluster{},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{},
		},
	}
}

func (b *replicateConfigurationBuilder) WithCluster(cluster *commonpb.MilvusCluster) ReplicateConfigurationBuilder {
	b.config.Clusters = append(b.config.Clusters, cluster)
	return b
}

func (b *replicateConfigurationBuilder) WithTopology(sourceClusterID, targetClusterID string) ReplicateConfigurationBuilder {
	topology := &commonpb.CrossClusterTopology{
		SourceClusterId: sourceClusterID,
		TargetClusterId: targetClusterID,
	}
	b.config.CrossClusterTopology = append(b.config.CrossClusterTopology, topology)
	return b
}

func (b *replicateConfigurationBuilder) Build() *commonpb.ReplicateConfiguration {
	return b.config
}
