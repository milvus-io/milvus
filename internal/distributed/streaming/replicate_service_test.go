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
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
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

func TestReplicateService_SkipMessageTypes(t *testing.T) {
	newReplicateService := func(t *testing.T, skipTypes map[string]struct{}) *replicateService {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()
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
		), nil).Maybe()

		h := mock_handler.NewMockHandlerClient(t)
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
			skipMessageTypes: skipTypes,
		}
	}

	skipSet := buildSkipMessageTypes([]string{"AlterResourceGroup", "DropResourceGroup"})

	t.Run("skip_AlterResourceGroup", func(t *testing.T) {
		rs := newReplicateService(t, skipSet)
		broadcastMsg := message.NewAlterResourceGroupMessageBuilderV2().
			WithHeader(&message.AlterResourceGroupMessageHeader{}).
			WithBody(&message.AlterResourceGroupMessageBody{}).
			WithBroadcast([]string{"primary-rootcoord-dml_0_cchannel"}).
			MustBuildBroadcast()
		msgs := broadcastMsgToReplicateMsgs(broadcastMsg)
		for _, msg := range msgs {
			_, err := rs.Append(context.Background(), msg)
			assert.Error(t, err)
			se := status.AsStreamingError(err)
			assert.NotNil(t, se)
			assert.True(t, se.IsIgnoredOperation())
		}
	})

	t.Run("skip_DropResourceGroup", func(t *testing.T) {
		rs := newReplicateService(t, skipSet)
		broadcastMsg := message.NewDropResourceGroupMessageBuilderV2().
			WithHeader(&message.DropResourceGroupMessageHeader{ResourceGroupName: "test_rg"}).
			WithBody(&message.DropResourceGroupMessageBody{}).
			WithBroadcast([]string{"primary-rootcoord-dml_0_cchannel"}).
			MustBuildBroadcast()
		msgs := broadcastMsgToReplicateMsgs(broadcastMsg)
		for _, msg := range msgs {
			_, err := rs.Append(context.Background(), msg)
			assert.Error(t, err)
			se := status.AsStreamingError(err)
			assert.NotNil(t, se)
			assert.True(t, se.IsIgnoredOperation())
		}
	})

	t.Run("non_skipped_message_passthrough", func(t *testing.T) {
		rs := newReplicateService(t, skipSet)
		replicateMsgs := createReplicateCreateCollectionMessages()
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})

	t.Run("empty_skip_list", func(t *testing.T) {
		rs := newReplicateService(t, buildSkipMessageTypes(nil))
		replicateMsgs := createReplicateCreateCollectionMessages()
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})
}

func broadcastMsgToReplicateMsgs(broadcastMsg message.BroadcastMutableMessage) []message.ReplicateMutableMessage {
	msgs := broadcastMsg.WithBroadcastID(200).SplitIntoMutableMessage()
	replicateMsgs := make([]message.ReplicateMutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(pulsar2.NewPulsarID(
			pulsar.NewMessageID(1, 2, 3, 4),
		))
		replicateMsgs = append(replicateMsgs, message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto()))
	}
	return replicateMsgs
}

func TestReplicateService_AlterConfigPChannelIncreasing(t *testing.T) {
	// New config adds a 3rd channel (dml_2)
	newConfig := &commonpb.ReplicateConfiguration{
		Clusters: []*commonpb.MilvusCluster{
			{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1", "primary-rootcoord-dml_2"}},
			{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0", "by-dev-rootcoord-dml_1", "by-dev-rootcoord-dml_2"}},
		},
		CrossClusterTopology: []*commonpb.CrossClusterTopology{
			{SourceClusterId: "primary", TargetClusterId: "by-dev"},
		},
	}

	t.Run("with_flag_maps_all_channels", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		h := mock_handler.NewMockHandlerClient(t)
		p := mock_producer.NewMockProducer(t)
		p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
			msg := message.MustAsMutableAlterReplicateConfigMessageV2(mm)
			// With IsPchannelIncreasing flag, all 3 channels (including new one)
			// should be mapped using the new config from the message header.
			bh := msg.BroadcastHeader()
			assert.NotNil(t, bh)
			assert.Len(t, bh.VChannels, 3, "all channels including new one should be mapped")
			for _, vchannel := range bh.VChannels {
				assert.True(t, strings.HasPrefix(vchannel, "by-dev"), "vchannel should be mapped to secondary cluster")
			}
			return &types.AppendResult{
				MessageID: walimplstest.NewTestMessageID(1),
				TimeTick:  1,
			}, nil
		}).Maybe()
		p.EXPECT().IsAvailable().Return(true).Maybe()
		p.EXPECT().Available().Return(make(chan struct{})).Maybe()
		h.EXPECT().CreateProducer(mock.Anything, mock.Anything).Return(p, nil).Maybe()

		// Current (old) config has 2 channels
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
		as.EXPECT().GetLatestAssignments(mock.Anything).Return(nil, errors.New("not needed")).Maybe()

		rs := &replicateService{
			walAccesserImpl: &walAccesserImpl{
				lifetime:             typeutil.NewLifetime(),
				clusterID:            "by-dev",
				streamingCoordClient: c,
				handlerClient:        h,
				producers:            make(map[string]*producer.ResumableProducer),
			},
		}

		// Build AlterReplicateConfig broadcast with 3 channels and IsPchannelIncreasing flag
		replicateMsgs := createReplicateAlterConfigMessages(newConfig,
			[]string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1", "primary-rootcoord-dml_2"},
			true)

		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})

	t.Run("without_flag_fails_for_unknown_channel", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		h := mock_handler.NewMockHandlerClient(t)

		// Current (old) config has 2 channels
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

		// Build AlterReplicateConfig broadcast with 3 channels but NO IsPchannelIncreasing flag
		replicateMsgs := createReplicateAlterConfigMessages(newConfig,
			[]string{"primary-rootcoord-dml_0", "primary-rootcoord-dml_1", "primary-rootcoord-dml_2"},
			false)

		// Should fail because the old config doesn't know about primary-rootcoord-dml_2
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to get target channel")
		}
	})

	t.Run("with_flag_invalid_config_in_header", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		h := mock_handler.NewMockHandlerClient(t)

		// Current config has 2 channels
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

		// Build with IsPchannelIncreasing flag but an invalid config (no topology, multiple primary => error)
		invalidConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0"}},
			},
			// Missing CrossClusterTopology => both clusters are "primary" => primaryCount != 1
		}
		replicateMsgs := createReplicateAlterConfigMessages(invalidConfig,
			[]string{"primary-rootcoord-dml_0"},
			true)

		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "failed to parse new replicate config")
		}
	})

	t.Run("with_flag_source_cluster_missing_in_new_config", func(t *testing.T) {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		h := mock_handler.NewMockHandlerClient(t)

		// Current config has 2 channels
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

		// Build with IsPchannelIncreasing flag but the new config uses "other-cluster" instead of "primary"
		// The replicate header has ClusterID="primary", but the new config doesn't contain "primary"
		missingSourceConfig := &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "other-cluster", Pchannels: []string{"other-rootcoord-dml_0"}},
				{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "other-cluster", TargetClusterId: "by-dev"},
			},
		}
		replicateMsgs := createReplicateAlterConfigMessages(missingSourceConfig,
			[]string{"primary-rootcoord-dml_0"},
			true)

		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "source cluster primary not found in new replicate configuration")
		}
	})
}

func createReplicateAlterConfigMessages(newConfig *commonpb.ReplicateConfiguration, broadcastChannels []string, isPchannelIncreasing bool) []message.ReplicateMutableMessage {
	alterMsg := message.NewAlterReplicateConfigMessageBuilderV2().
		WithHeader(&message.AlterReplicateConfigMessageHeader{
			ReplicateConfiguration: newConfig,
			IsPchannelIncreasing:   isPchannelIncreasing,
		}).
		WithBody(&message.AlterReplicateConfigMessageBody{}).
		WithBroadcast(broadcastChannels).
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

func TestReplicateService_AlterLoadConfigUseLocalReplicaConfig(t *testing.T) {
	newReplicateServiceForAlterLoadConfig := func(t *testing.T, appendAssert func(t *testing.T, mm message.MutableMessage)) *replicateService {
		c := mock_client.NewMockClient(t)
		as := mock_client.NewMockAssignmentService(t)
		c.EXPECT().Assignment().Return(as).Maybe()

		h := mock_handler.NewMockHandlerClient(t)
		p := mock_producer.NewMockProducer(t)
		p.EXPECT().Append(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, mm message.MutableMessage) (*types.AppendResult, error) {
			appendAssert(t, mm)
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
					{ClusterId: "primary", Pchannels: []string{"primary-rootcoord-dml_0"}},
					{ClusterId: "by-dev", Pchannels: []string{"by-dev-rootcoord-dml_0"}},
				},
				CrossClusterTopology: []*commonpb.CrossClusterTopology{
					{SourceClusterId: "primary", TargetClusterId: "by-dev"},
				},
			},
		), nil)

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

	t.Run("config_enabled", func(t *testing.T) {
		old := paramtable.Get().StreamingCfg.ReplicationUseLocalReplicaConfig.SwapTempValue("true")
		defer paramtable.Get().StreamingCfg.ReplicationUseLocalReplicaConfig.SwapTempValue(old)

		rs := newReplicateServiceForAlterLoadConfig(t, func(t *testing.T, mm message.MutableMessage) {
			alterLoadConfigMsg := message.MustAsMutableAlterLoadConfigMessageV2(mm)
			assert.True(t, alterLoadConfigMsg.Header().GetUseLocalReplicaConfig(),
				"replicated AlterLoadConfig should have UseLocalReplicaConfig=true when config is enabled")
			assert.True(t, strings.HasPrefix(mm.VChannel(), "by-dev"),
				"vchannel should be remapped to secondary cluster prefix")
		})

		replicateMsgs := createReplicateAlterLoadConfigMessages()
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})

	t.Run("config_disabled", func(t *testing.T) {
		old := paramtable.Get().StreamingCfg.ReplicationUseLocalReplicaConfig.SwapTempValue("false")
		defer paramtable.Get().StreamingCfg.ReplicationUseLocalReplicaConfig.SwapTempValue(old)

		rs := newReplicateServiceForAlterLoadConfig(t, func(t *testing.T, mm message.MutableMessage) {
			alterLoadConfigMsg := message.MustAsMutableAlterLoadConfigMessageV2(mm)
			assert.False(t, alterLoadConfigMsg.Header().GetUseLocalReplicaConfig(),
				"replicated AlterLoadConfig should have UseLocalReplicaConfig=false when config is disabled")
		})

		replicateMsgs := createReplicateAlterLoadConfigMessages()
		for _, msg := range replicateMsgs {
			_, err := rs.Append(context.Background(), msg)
			assert.NoError(t, err)
		}
	})
}

func TestBuildSkipMessageTypes(t *testing.T) {
	t.Run("normal", func(t *testing.T) {
		m := buildSkipMessageTypes([]string{"AlterResourceGroup", "DropResourceGroup"})
		assert.Len(t, m, 2)
		_, ok := m["AlterResourceGroup"]
		assert.True(t, ok)
		_, ok = m["DropResourceGroup"]
		assert.True(t, ok)
	})

	t.Run("empty_strings_filtered", func(t *testing.T) {
		m := buildSkipMessageTypes([]string{"", "AlterResourceGroup", ""})
		assert.Len(t, m, 1)
		_, ok := m["AlterResourceGroup"]
		assert.True(t, ok)
	})

	t.Run("nil_input", func(t *testing.T) {
		m := buildSkipMessageTypes(nil)
		assert.Empty(t, m)
	})
}

func createReplicateAlterLoadConfigMessages() []message.ReplicateMutableMessage {
	msg := message.NewAlterLoadConfigMessageBuilderV2().
		WithHeader(&message.AlterLoadConfigMessageHeader{
			CollectionId: 1,
			PartitionIds: []int64{100},
			Replicas: []*messagespb.LoadReplicaConfig{
				{ReplicaId: 1, ResourceGroupName: "rg1"},
				{ReplicaId: 2, ResourceGroupName: "rg2"},
			},
		}).
		WithBody(&message.AlterLoadConfigMessageBody{}).
		WithBroadcast([]string{"primary-rootcoord-dml_0_1v0"}).
		MustBuildBroadcast()

	msgs := msg.WithBroadcastID(200).SplitIntoMutableMessage()
	replicateMsgs := make([]message.ReplicateMutableMessage, 0, len(msgs))
	for _, msg := range msgs {
		immutableMsg := msg.WithLastConfirmedUseMessageID().WithTimeTick(1).IntoImmutableMessage(
			pulsar2.NewPulsarID(pulsar.NewMessageID(1, 2, 3, 4)),
		)
		replicateMsgs = append(replicateMsgs, message.MustNewReplicateMessage("primary", immutableMsg.IntoImmutableMessageProto()))
	}
	return replicateMsgs
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
		replicateMsgs := createSimpleReplicateAlterConfigMessages(newConfig)

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
		replicateMsgs := createSimpleReplicateAlterConfigMessages(newConfig)

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
		replicateMsgs := createSimpleReplicateAlterConfigMessages(invalidConfig)

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

func createSimpleReplicateAlterConfigMessages(newConfig *commonpb.ReplicateConfiguration) []message.ReplicateMutableMessage {
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
