package assignment

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/util/streamingutil/service/mock_lazygrpc"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/replicateutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestAssignmentService(t *testing.T) {
	paramtable.Init()

	s := mock_lazygrpc.NewMockService[streamingpb.StreamingCoordAssignmentServiceClient](t)
	c := mock_streamingpb.NewMockStreamingCoordAssignmentServiceClient(t)
	s.EXPECT().GetService(mock.Anything).Return(c, nil)
	cc := mock_streamingpb.NewMockStreamingCoordAssignmentService_AssignmentDiscoverClient(t)
	c.EXPECT().AssignmentDiscover(mock.Anything).Return(cc, nil)
	c.EXPECT().UpdateWALBalancePolicy(mock.Anything, mock.Anything).Return(&streamingpb.UpdateWALBalancePolicyResponse{}, nil)
	k := 0
	closeCh := make(chan struct{})
	cc.EXPECT().Send(mock.Anything).Return(nil)
	cc.EXPECT().CloseSend().Return(nil)
	cc.EXPECT().Recv().RunAndReturn(func() (*streamingpb.AssignmentDiscoverResponse, error) {
		resps := []*streamingpb.AssignmentDiscoverResponse{
			{
				Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
					FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{
						Version: &streamingpb.VersionPair{Global: 1, Local: 2},
						Assignments: []*streamingpb.StreamingNodeAssignment{
							{
								Node:     &streamingpb.StreamingNodeInfo{ServerId: 1},
								Channels: []*streamingpb.PChannelInfo{{Name: "c1", Term: 1}, {Name: "c2", Term: 2}},
							},
						},
						VersionByRevision: &streamingpb.VersionPair{Global: 1, Local: 2},
					},
				},
			},
			{
				Response: &streamingpb.AssignmentDiscoverResponse_FullAssignment{
					FullAssignment: &streamingpb.FullStreamingNodeAssignmentWithVersion{
						Version: &streamingpb.VersionPair{Global: 2, Local: 3},
						Assignments: []*streamingpb.StreamingNodeAssignment{
							{
								Node:     &streamingpb.StreamingNodeInfo{ServerId: 1},
								Channels: []*streamingpb.PChannelInfo{{Name: "c1", Term: 1}, {Name: "c2", Term: 2}},
							},
							{
								Node:     &streamingpb.StreamingNodeInfo{ServerId: 2},
								Channels: []*streamingpb.PChannelInfo{{Name: "c3", Term: 1}, {Name: "c4", Term: 2}},
							},
						},
						VersionByRevision: &streamingpb.VersionPair{Global: 2, Local: 3},
					},
				},
			},
			nil,
		}
		errs := []error{
			nil,
			nil,
			io.ErrUnexpectedEOF,
		}
		if k > len(resps) {
			return nil, io.EOF
		} else if k == len(resps) {
			<-closeCh
			k++
			return &streamingpb.AssignmentDiscoverResponse{
				Response: &streamingpb.AssignmentDiscoverResponse_Close{},
			}, nil
		}
		time.Sleep(25 * time.Millisecond)
		k++
		return resps[k-1], errs[k-1]
	})

	assignmentService := NewAssignmentService(s)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	var finalAssignments *types.VersionedStreamingNodeAssignments
	err := assignmentService.AssignmentDiscover(ctx, func(vsna *types.VersionedStreamingNodeAssignments) error {
		finalAssignments = vsna
		return nil
	})
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.True(t, finalAssignments.Version.EQ(typeutil.VersionInt64Pair{Global: 2, Local: 3}))

	assign, err := assignmentService.GetLatestAssignments(ctx)
	assert.NoError(t, err)
	assert.True(t, assign.Version.EQ(typeutil.VersionInt64Pair{Global: 2, Local: 3}))

	resp, err := assignmentService.UpdateWALBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{})
	assert.NoError(t, err)
	assert.NotNil(t, resp)

	assignmentService.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, errors.New("test"))

	// Repeated report error at the same term should be ignored.
	assignmentService.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, errors.New("test"))
	assignmentService.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, errors.New("test"))

	// test close
	go close(closeCh)
	time.Sleep(10 * time.Millisecond)
	assignmentService.Close()

	// running assignment service should be closed too.
	err = assignmentService.AssignmentDiscover(ctx, func(vsna *types.VersionedStreamingNodeAssignments) error {
		return nil
	})
	se := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)

	err = assignmentService.ReportAssignmentError(ctx, types.PChannelInfo{Name: "c1", Term: 1}, errors.New("test"))
	se = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)

	assignmentService.GetLatestAssignments(ctx)
	se = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)

	assignmentService.UpdateWALBalancePolicy(ctx, &streamingpb.UpdateWALBalancePolicyRequest{})
	se = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)

	// GetReplicateConfiguration should also return shutdown error
	_, err = assignmentService.GetReplicateConfiguration(ctx)
	se = status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN, se.Code)
}

func TestWatcher_GetLatestReplicateConfiguration(t *testing.T) {
	t.Run("returns_config_when_available", func(t *testing.T) {
		w := newWatcher()

		expectedConfig := replicateutil.MustNewConfigHelper("primary", &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"ch1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530"}},
				{ClusterId: "secondary", Pchannels: []string{"ch2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://secondary:19530"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "secondary"},
			},
		})

		// Update watcher with valid version and config
		w.Update(types.VersionedStreamingNodeAssignments{
			Version:               typeutil.VersionInt64Pair{Global: 1, Local: 1},
			Assignments:           map[int64]types.StreamingNodeAssignment{},
			ReplicateConfigHelper: expectedConfig,
		})

		config, err := w.GetLatestReplicateConfiguration(context.Background())
		assert.NoError(t, err)
		assert.NotNil(t, config)
		assert.Equal(t, expectedConfig, config)
	})

	t.Run("blocks_until_config_available", func(t *testing.T) {
		w := newWatcher()

		resultCh := make(chan *replicateutil.ConfigHelper, 1)
		errCh := make(chan error, 1)

		go func() {
			config, err := w.GetLatestReplicateConfiguration(context.Background())
			resultCh <- config
			errCh <- err
		}()

		// Verify it's blocking (no result yet)
		select {
		case <-resultCh:
			t.Fatal("GetLatestReplicateConfiguration should be blocking")
		case <-time.After(50 * time.Millisecond):
		}

		expectedConfig := replicateutil.MustNewConfigHelper("primary", &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"ch1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530"}},
			},
		})

		// Now provide the config
		w.Update(types.VersionedStreamingNodeAssignments{
			Version:               typeutil.VersionInt64Pair{Global: 1, Local: 1},
			Assignments:           map[int64]types.StreamingNodeAssignment{},
			ReplicateConfigHelper: expectedConfig,
		})

		select {
		case config := <-resultCh:
			assert.NotNil(t, config)
			assert.Equal(t, expectedConfig, config)
		case <-time.After(time.Second):
			t.Fatal("GetLatestReplicateConfiguration should have returned")
		}
		assert.NoError(t, <-errCh)
	})

	t.Run("blocks_when_version_set_but_config_nil", func(t *testing.T) {
		w := newWatcher()

		// Update with valid version but nil config
		w.Update(types.VersionedStreamingNodeAssignments{
			Version:               typeutil.VersionInt64Pair{Global: 1, Local: 1},
			Assignments:           map[int64]types.StreamingNodeAssignment{},
			ReplicateConfigHelper: nil,
		})

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		_, err := w.GetLatestReplicateConfiguration(ctx)
		assert.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("returns_error_on_context_cancel", func(t *testing.T) {
		w := newWatcher()

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // cancel immediately

		_, err := w.GetLatestReplicateConfiguration(ctx)
		assert.ErrorIs(t, err, context.Canceled)
	})

	t.Run("returns_latest_config_after_multiple_updates", func(t *testing.T) {
		w := newWatcher()

		config1 := replicateutil.MustNewConfigHelper("primary", &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"ch1"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530"}},
			},
		})
		config2 := replicateutil.MustNewConfigHelper("primary", &commonpb.ReplicateConfiguration{
			Clusters: []*commonpb.MilvusCluster{
				{ClusterId: "primary", Pchannels: []string{"ch1", "ch2"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://primary:19530"}},
				{ClusterId: "secondary", Pchannels: []string{"ch3", "ch4"}, ConnectionParam: &commonpb.ConnectionParam{Uri: "http://secondary:19530"}},
			},
			CrossClusterTopology: []*commonpb.CrossClusterTopology{
				{SourceClusterId: "primary", TargetClusterId: "secondary"},
			},
		})

		w.Update(types.VersionedStreamingNodeAssignments{
			Version:               typeutil.VersionInt64Pair{Global: 1, Local: 1},
			Assignments:           map[int64]types.StreamingNodeAssignment{},
			ReplicateConfigHelper: config1,
		})
		w.Update(types.VersionedStreamingNodeAssignments{
			Version:               typeutil.VersionInt64Pair{Global: 2, Local: 2},
			Assignments:           map[int64]types.StreamingNodeAssignment{},
			ReplicateConfigHelper: config2,
		})

		config, err := w.GetLatestReplicateConfiguration(context.Background())
		assert.NoError(t, err)
		assert.Equal(t, config2, config)
	})
}
