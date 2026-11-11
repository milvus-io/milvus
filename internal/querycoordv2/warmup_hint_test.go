package querycoordv2

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func enableWarmupHintGate(t *testing.T, enabled string) {
	paramtable.Init()
	key := paramtable.Get().QueryCoordCfg.EnableLoadCollectionWarmupHint.Key
	require.NoError(t, paramtable.Get().Save(key, enabled))
	t.Cleanup(func() { _ = paramtable.Get().Reset(key) })
}

func TestPrewarmCollectionAdmission(t *testing.T) {
	enableWarmupHintGate(t, "true")
	s, _, _ := newReadinessTestServer(t)
	r := s.qviewsRuntime
	t.Cleanup(r.stopWarmups)
	s.meta = newViewMeta(&metacache.CollectionInfo{
		ID: 100, Schema: &schemapb.CollectionSchema{}, VChannelNames: []string{"by-dev-rootcoord-dml_100v0"},
	})
	s.idAllocator = func() (int64, error) { t.Fatal("prewarm must not allocate an identity"); return 0, nil }
	cfg := r.loadConfigStore.GetConfig(100)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	status, err := s.PrewarmCollection(ctx, &querypb.PrewarmCollectionRequest{CollectionID: 100})
	require.NoError(t, merr.CheckRPCCall(status, err), "admission does not wait for Up")
	require.Same(t, cfg, r.loadConfigStore.GetConfig(100), "prewarm cannot restate load intent")
	cancel()

	r.loadConfigStore.StopPlanning(100)
	status, err = s.PrewarmCollection(t.Context(), &querypb.PrewarmCollectionRequest{CollectionID: 100})
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrCollectionNotLoaded)
	require.Nil(t, r.loadConfigStore.GetConfig(100), "prewarm cannot auto-load a released collection")
}

func TestPrewarmCollectionRejectsUnsupportedRequests(t *testing.T) {
	enableWarmupHintGate(t, "false")
	s := &Server{}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	req := &querypb.PrewarmCollectionRequest{CollectionID: 100}
	status, err := s.PrewarmCollection(t.Context(), req)
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrServiceUnimplemented)
	require.NoError(t, paramtable.Get().Save(paramtable.Get().QueryCoordCfg.EnableLoadCollectionWarmupHint.Key, "true"))
	s.meta = newViewMeta(&metacache.CollectionInfo{
		ID: 100, Schema: &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{Name: "f", ExternalField: "col"}}},
	})
	status, err = s.PrewarmCollection(t.Context(), req)
	require.ErrorIs(t, merr.CheckRPCCall(status, err), merr.ErrParameterInvalid)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	status, err = s.PrewarmCollection(ctx, req)
	require.NoError(t, err)
	require.Equal(t, merr.CanceledCode, status.GetCode())
}
