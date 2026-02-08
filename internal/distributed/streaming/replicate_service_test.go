package streaming

import (
	"context"
	"strings"
	"testing"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/distributed/streaming/internal/producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/mock_client"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/handler/mock_producer"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/client/mock_handler"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestReplicateService(t *testing.T) {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()

	h := mock_handler.NewMockHandlerClient(t)
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
		msg := message.MustAsMutableCreateCollectionMessageV1(mm)
		assert.True(t, strings.HasPrefix(msg.VChannel(), "by-dev"))
		for _, vchannel := range msg.BroadcastHeader().VChannels {
			assert.True(t, strings.HasPrefix(vchannel, "by-dev"))
		}
		b := msg.MustBody()
		for _, vchannel := range b.VirtualChannelNames {
			assert.True(t, strings.HasPrefix(vchannel, "by-dev"))
		}
		for _, pchannel := range b.PhysicalChannelNames {
			assert.True(t, strings.HasPrefix(pchannel, "by-dev"))
		}
		return &types.AppendResult{
			MessageID: walimplstest.NewTestMessageID(1),
			TimeTick:  1,
		}, nil
	}).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()

	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
		"by-dev",
		&commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		},
	), nil)
	rs := &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}
	replicateMsgs := createReplicateCreateCollectionMessages()

	for _, msg := range replicateMsgs {
		_, err := rs.Append(context.Background(), msg)
		assert.NoError(t, err)
	}
}

func TestReplicateService_GetReplicateConfiguration(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		expectedConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{
					ClusterId: "primary",
					ConnectionParam: &commonpb.ConnectionParam{
						Uri:   "http://primary:19530",
						Token: "secret-token",
					},
					Pchannels: []string{"channel1"},
				},
				{
					ClusterId: "secondary",
					ConnectionParam: &commonpb.ConnectionParam{
						Uri:   "http://secondary:19530",
						Token: "another-secret",
					},
					Pchannels: []string{"channel1"},
				},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "secondary"},
			},
		}

		as.EXPECT().GetReplicateConfiguration(mock.Anything, mock.Anything).Return(
			replicateutil.MustNewConfigHelper("secondary", expectedConfig), nil,
		)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "secondary",
				streamingCoordClient: c,
			},
		}

		config, err := rs.GetReplicateConfiguration(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, config)

		// Tokens should be sanitized for all clusters
		assert.Empty(t, config.Clusters[0].ConnectionParam.Token)
		assert.Empty(t, config.Clusters[1].ConnectionParam.Token)
		// URIs should be preserved
		assert.Equal(t, "http://primary:19530", config.Clusters[0].ConnectionParam.Uri)
		assert.Equal(t, "http://secondary:19530", config.Clusters[1].ConnectionParam.Uri)
	})

	t.Run("lifetime_closed", func(t *testing.T) {
		lifetime := typeutil.NewLifetime()
		lifetime.SetState(typeutil.LifetimeStateStopped)
		lifetime.Wait()

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime: lifetime,
			},
		}

		config, err := rs.GetReplicateConfiguration(context.Background())
		assert.Error(t, err)
		assert.Nil(t, config)
		assert.ErrorIs(t, err, ErrWALAccesserClosed)
	})

	t.Run("assignment_error", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		as.EXPECT().GetReplicateConfiguration(mock.Anything, mock.Anything).Return(
			nil, errors.New("assignment service unavailable"),
		)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "secondary",
				streamingCoordClient: c,
			},
		}

		config, err := rs.GetReplicateConfiguration(context.Background())
		assert.Error(t, err)
		assert.Nil(t, config)
		assert.Contains(t, err.Error(), "assignment service unavailable")
	})

	t.Run("standalone_single_cluster", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		standaloneConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{
					ClusterId: "standalone",
					ConnectionParam: &commonpb.ConnectionParam{
						Uri:   "http://standalone:19530",
						Token: "standalone-secret",
					},
					Pchannels: []string{"ch1"},
				},
			},
		}

		as.EXPECT().GetReplicateConfiguration(mock.Anything, mock.Anything).Return(
			replicateutil.MustNewConfigHelper("standalone", standaloneConfig), nil,
		)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "standalone",
				streamingCoordClient: c,
			},
		}

		config, err := rs.GetReplicateConfiguration(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Len(t, config.Clusters, 1)
		assert.Equal(t, "standalone", config.Clusters[0].ClusterId)
		assert.Empty(t, config.Clusters[0].ConnectionParam.Token)
		assert.Equal(t, "http://standalone:19530", config.Clusters[0].ConnectionParam.Uri)
		assert.Empty(t, config.CrossClusterTopology)
	})
}

func createReplicateCreateCollectionMessages() []message.ReplicateMutableMessage {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}
	schemaBytes, _ := proto.Marshal(schema)
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{2},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionID:   1,
			CollectionName: "collection",
			PartitionName:  "partition",
			PhysicalChannelNames: []string{
				"primary-rootcoord-dml_0",
				"primary-rootcoord-dml_1",
			},
			VirtualChannelNames: []string{
				"primary-rootcoord-dml_0_1v0",
				"primary-rootcoord-dml_1_1v1",
			},
			Schema: schemaBytes,
		}).
		WithBroadcast([]string{"primary-rootcoord-dml_0_1v0", "primary-rootcoord-dml_1_1v1"}).
		MustBuildBroadcast()
	msgs := msg.WithBroadcastID(100).SplitIntoMutableMessage()
	replicateMsgs := make([]message.ReplicateMutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
			pulsar.NewMessageID(1, 2, 3, 4),
		))
		replicateMsgs = append(replicateMsgs, message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto()))
	}
	return replicateMsgs
}

func newReplicateService(t *testing.T, c *mock_client.MockClient, h *mock_handler.MockHandlerClient) *replicateService {
	p := mock_producer.NewMockProducer(t)
	p.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: walimplstest.NewTestMessageID(1),
		TimeTick:  1,
	}, nil).Maybe()
	p.EXPECT().IsAvailable().Return(true).Maybe()
	p.EXPECT().Available().Return(make(chan struct{})).Maybe()
	h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()
	return &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}
}

func TestReplicateServiceUpdateConfiguration(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().UpdateReplicateConfiguration(mock.Anything, mock.Anything).Return(nil)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		req := &milvuspb.UpdateReplicateConfigurationRequest{}
		err := rs.UpdateReplicateConfiguration(context.Background(), req)
		assert.NoError(t, err)
	})

	t.Run("closed_lifetime", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		h := mock_handler.NewMockHandlerClient(t)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}
		rs.lifetime.SetState(typeutil.LifetimeStateStopped)
		rs.lifetime.Wait()

		req := &milvuspb.UpdateReplicateConfigurationRequest{}
		err := rs.UpdateReplicateConfiguration(context.Background(), req)
		assert.Error(t, err)
	})
}

func TestReplicateServiceGetCheckpoint(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		h := mock_handler.NewMockHandlerClient(t)

		expectedCheckpoint := &wal.ReplicateCheckpoint{
			ClusterID: "primary",
			PChannel:  "primary-rootcoord-dml_0",
			TimeTick:  100,
		}
		h.EXPECT().GetReplicateCheckpoint(mock.Anything, "test-channel").Return(expectedCheckpoint, nil)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		checkpoint, err := rs.GetReplicateCheckpoint(context.Background(), "test-channel")
		assert.NoError(t, err)
		assert.Equal(t, expectedCheckpoint, checkpoint)
	})

	t.Run("error", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		h := mock_handler.NewMockHandlerClient(t)

		h.EXPECT().GetReplicateCheckpoint(mock.Anything, "bad-channel").Return(nil, errors.New("not found"))

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		_, err := rs.GetReplicateCheckpoint(context.Background(), "bad-channel")
		assert.Error(t, err)
	})

	t.Run("closed_lifetime", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		h := mock_handler.NewMockHandlerClient(t)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}
		rs.lifetime.SetState(typeutil.LifetimeStateStopped)
		rs.lifetime.Wait()

		_, err := rs.GetReplicateCheckpoint(context.Background(), "test-channel")
		assert.Error(t, err)
	})
}

func TestReplicateServiceAppendClosed(t *testing.T) {
	c := mock_client.NewMockClient(t)
	h := mock_handler.NewMockHandlerClient(t)

	rs := &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}
	rs.lifetime.SetState(typeutil.LifetimeStateStopped)
	rs.lifetime.Wait()

	replicateMsgs := createReplicateCreateCollectionMessages()
	_, err := rs.Append(context.Background(), replicateMsgs[0])
	assert.Error(t, err)
}

func TestReplicateServiceAlterReplicateConfigMessage(t *testing.T) {
	replicateConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}

	t.Run("alter_config_current_cluster_in_new_config", func(t *testing.T) {
		// New config includes the current cluster "by-dev" => overwriteAlterReplicateConfigMessage returns nil immediately
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)

		rs := newReplicateService(t, c, h)

		// Build an AlterReplicateConfig message where the new config includes by-dev
		newConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "by-dev"},
			},
		}
		replicateMsgs := createReplicateAlterConfigMessages(newConfig)
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})

	t.Run("alter_config_current_cluster_removed", func(t *testing.T) {
		// New config does NOT include "by-dev" => overwriteAlterReplicateConfigMessage overwrites header
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)

		rs := newReplicateService(t, c, h)

		// Build an AlterReplicateConfig message where the new config does NOT include by-dev
		newConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1"}},
			},
		}
		replicateMsgs := createReplicateAlterConfigMessages(newConfig)
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})

	t.Run("primary_cluster_rejects_replicate", func(t *testing.T) {
		// If the current cluster is primary, it should reject replicate messages
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		// Config where by-dev is the primary
		primaryConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0"}},
			},
		}
		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", primaryConfig,
		), nil)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		replicateMsgs := createReplicateCreateCollectionMessages()
		_, err := rs.Append(context.Background(), replicateMsgs[0])
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "primary cluster cannot receive replicate message")
	})

	t.Run("unmapped_vchannel_error", func(t *testing.T) {
		// Message has a vchannel whose pchannel is NOT in the source cluster's mapping
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		// Create a message with an unmapped vchannel
		msg := message.NewCreateDatabaseMessageBuilderV2().
			WithHeader(&message.CreateDatabaseMessageHeader{}).
			WithBody(&message.CreateDatabaseMessageBody{}).
			WithVChannel("primary-unknown-dml_0_1v0").
			MustBuildMutable()
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
			pulsar.NewMessageID(1, 2, 3, 4),
		))
		replicateMsg := message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto())
		_, err := rs.Append(context.Background(), replicateMsg)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to get target channel")
	})

	t.Run("alter_config_invalid_topology_error", func(t *testing.T) {
		// AlterReplicateConfig message with invalid topology triggers non-ErrCurrentClusterNotFound error
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)

		rs := newReplicateService(t, c, h)

		// Build an AlterReplicateConfig message with invalid topology (references nonexistent cluster)
		invalidConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "by-dev", TargetClusterId: "nonexistent"},
			},
		}
		replicateMsgs := createReplicateAlterConfigMessages(invalidConfig)
		_, err := rs.Append(context.Background(), replicateMsgs[0])
		assert.Error(t, err)
	})

	t.Run("source_cluster_not_found", func(t *testing.T) {
		// If the source cluster is not in the config
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		// Config without the "primary" cluster (source)
		cfg := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "other-primary", Pchannels: []string{"other-rootcoord-dml_0", "other-rootcoord-dml_1"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "other-primary", TargetClusterId: "by-dev"},
			},
		}
		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", cfg,
		), nil)

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		// The replicate message has source cluster "primary" which is not in config
		replicateMsgs := createReplicateCreateCollectionMessages()
		_, err := rs.Append(context.Background(), replicateMsgs[0])
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "source cluster primary not found")
	})
}

func TestReplicateServiceControlChannel(t *testing.T) {
	replicateConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}

	t.Run("control_channel_valid", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)
		as.EXPECT().GetLatestAssignments(mock.Anything).Return(&types.VersionedStreamingNodeAssignments{
			CChannel: &streamingpb.CChannelAssignment{
				Meta: &streamingpb.CChannelMeta{
					Pchannel: "by-dev-rootcoord-dml_0",
				},
			},
		}, nil)

		rs := newReplicateService(t, c, h)

		// Create a non-broadcast message on the control channel
		// primary-rootcoord-dml_0_vcchan maps to by-dev-rootcoord-dml_0_vcchan
		replicateMsgs := createReplicateControlChannelMessages()
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})

	t.Run("control_channel_invalid_pchannel", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)
		// Return a different pchannel than expected
		as.EXPECT().GetLatestAssignments(mock.Anything).Return(&types.VersionedStreamingNodeAssignments{
			CChannel: &streamingpb.CChannelAssignment{
				Meta: &streamingpb.CChannelMeta{
					Pchannel: "by-dev-rootcoord-dml_99",
				},
			},
		}, nil)

		rs := newReplicateService(t, c, h)

		replicateMsgs := createReplicateControlChannelMessages()
		_, err := rs.Append(context.Background(), replicateMsgs[0])
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid control channel")
	})

	t.Run("control_channel_get_assignments_error", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
		h := mock_handler.NewMockHandlerClient(t)

		as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(replicateutil.MustNewConfigHelper(
			"by-dev", replicateConfig,
		), nil)
		as.EXPECT().GetLatestAssignments(mock.Anything).Return(nil, errors.New("assignments unavailable"))

		rs := newReplicateService(t, c, h)

		replicateMsgs := createReplicateControlChannelMessages()
		_, err := rs.Append(context.Background(), replicateMsgs[0])
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "assignments unavailable")
	})
}

func TestReplicateServiceGetConfigError(t *testing.T) {
	c := mock_client.NewMockClient(t)
	as := mock_client.NewMockAssignmentService(t)
	c.EXPECT().Assignment().Return(as).Maybe()
	h := mock_handler.NewMockHandlerClient(t)

	as.EXPECT().GetReplicateConfiguration(mock.Anything).Return(nil, errors.New("config unavailable"))

	rs := &replicateService{
		walAccesserImpl: &walAccesserImpl{
			lifetime:             typeutil.NewLifetime(),
			clusterID:            "by-dev",
			streamingCoordClient: c,
			handlerClient:        h,
			producers:            make(map[string]*producer.ResumableProducer),
		},
	}

	replicateMsgs := createReplicateCreateCollectionMessages()
	_, err := rs.Append(context.Background(), replicateMsgs[0])
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "config unavailable")
}

func createReplicateAlterConfigMessages(newConfig *commonpb.ReplicateConfiguration) []message.ReplicateMutableMessage {
	alterMsg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: newConfig,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast([]string{"primary-rootcoord-dml_0_1v0", "primary-rootcoord-dml_1_1v1"}).
		MustBuildBroadcast()
	msgs := alterMsg.WithBroadcastID(200).SplitIntoMutableMessage()
	replicateMsgs := make([]message.ReplicateMutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
			pulsar.NewMessageID(1, 2, 3, 4),
		))
		replicateMsgs = append(replicateMsgs, message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto()))
	}
	return replicateMsgs
}

func createReplicateControlChannelMessages() []message.ReplicateMutableMessage {
	// Create a non-broadcast message on the control channel (vchannel ends with "_vcchan")
	msg := message.NewCreateDatabaseMessageBuilderV2().
		WithHeader(&message.CreateDatabaseMessageHeader{}).
		WithBody(&message.CreateDatabaseMessageBody{}).
		WithVChannel("primary-rootcoord-dml_0_vcchan").
		MustBuildMutable()
	immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
		pulsar.NewMessageID(1, 2, 3, 4),
	))
	replicateMsg := message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto())
	return []message.ReplicateMutableMessage{replicateMsg}
}
