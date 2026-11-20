package coordinator

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/datacoord"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestGetDataViewRetentionForwarding(t *testing.T) {
	dc := &datacoord.Server{}
	req := &datapb.GetDataViewRetentionRequest{Pchannel: "p"}
	expected := &datapb.GetDataViewRetentionResponse{Status: merr.Success(), CollectionIds: []int64{1}, VchannelIndices: []uint32{0}, TransformStartAfterTimeticks: []uint64{30}}
	patch := mockey.Mock((*datacoord.Server).GetDataViewRetention).To(func(s *datacoord.Server, _ context.Context, actual *datapb.GetDataViewRetentionRequest) (*datapb.GetDataViewRetentionResponse, error) {
		require.Same(t, dc, s)
		require.Same(t, req, actual)
		return expected, nil
	}).Build()
	defer patch.UnPatch()
	resp, err := (&mixCoordImpl{datacoordServer: dc}).GetDataViewRetention(t.Context(), req)
	require.NoError(t, err)
	require.Same(t, expected, resp)
}
