package querycoordv2

import (
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/querycoordv2/job"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewAutoLoadConfigBroadcastAndManualLoad(t *testing.T) {
	s := &Server{idAllocator: func() (int64, error) { return 1000, nil }}
	coll := &metacache.CollectionInfo{ID: 100, Schema: &schemapb.CollectionSchema{}}
	expected := qviewsExpectedLoadConfig{AutoLoad: true, PartitionIDs: []int64{10}, ReplicaNumber: map[string]int{"rg1": 1}}
	msg, err := s.generateAlterLoadConfigMessageForLoadCollection(t.Context(), coll, nil, expected)
	require.NoError(t, err)
	header := message.MustAsBroadcastAlterLoadConfigMessageV2(msg).Header()
	// The persisted broadcast must retain the source across replay, not a context value.
	wire, err := proto.Marshal(header)
	require.NoError(t, err)
	replayed := &messagespb.AlterLoadConfigMessageHeader{}
	require.NoError(t, proto.Unmarshal(wire, replayed))
	current := loadmgr.FromAlterLoadConfigMessage(replayed)
	require.True(t, current.AutoLoad)
	require.True(t, loadConfigIntoAlterLoadConfigHeader(current).GetAutoLoad())
	msg, err = s.generateAlterLoadConfigMessageForLoadCollection(t.Context(), coll, current, expected)
	require.NoError(t, err)
	require.Nil(t, msg, "an identical automatic load remains a no-op")

	// Replica/partition operations through the admission adapter retain the source.
	adapted := qviewsCurrentLoadConfig(current)
	require.True(t, adapted.AutoLoad)
	require.True(t, adapted.IntoLoadConfigMessageHeader().GetAutoLoad())
	msg, err = job.GenerateAlterLoadConfigMessage(t.Context(), &job.AlterLoadConfigRequest{
		CollectionInfo: coll, Current: adapted,
		Expected: job.ExpectedLoadConfig{
			ExpectedAutoLoad: adapted.AutoLoad, ExpectedPartitionIDs: []int64{10, 20},
			ExpectedReplicaNumber: map[string]int{"rg1": 1},
		},
	})
	require.NoError(t, err)
	require.True(t, message.MustAsBroadcastAlterLoadConfigMessageV2(msg).Header().GetAutoLoad())

	// Explicit manual Load must publish even if only the source changes.
	expected.AutoLoad = false
	msg, err = s.generateAlterLoadConfigMessageForLoadCollection(t.Context(), coll, current, expected)
	require.NoError(t, err)
	require.NotNil(t, msg)
	require.False(t, message.MustAsBroadcastAlterLoadConfigMessageV2(msg).Header().GetAutoLoad())
	require.True(t, current.AutoLoad, "published config remains immutable")
}

func TestLoadCollectionSharedAdmissionAndRefresh(t *testing.T) {
	s := &Server{}
	s.UpdateStateCode(commonpb.StateCode_Abnormal)
	status, err := s.LoadCollection(t.Context(), &querypb.LoadCollectionRequest{CollectionID: 100})
	require.NoError(t, err)
	require.Error(t, merr.Error(status), "the shared helper must retain health admission")

	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failure"}[fail], func(t *testing.T) {
			s.UpdateStateCode(commonpb.StateCode_Healthy)
			var refreshErr error
			if fail {
				refreshErr = merr.ErrServiceUnavailable
			}
			patch := mockey.Mock((*Server).refreshCollection).Return(refreshErr).Build()
			defer patch.UnPatch()
			// Refresh does not create a new load intent or need a LoadConfigStore.
			status, err := s.LoadCollection(t.Context(), &querypb.LoadCollectionRequest{CollectionID: 100, Refresh: true})
			require.NoError(t, err)
			require.ErrorIs(t, merr.Error(status), refreshErr)
			require.EqualValues(t, 1, patch.Times())
		})
	}
}
