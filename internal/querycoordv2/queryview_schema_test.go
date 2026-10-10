package querycoordv2

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/meta"
	"github.com/milvus-io/milvus/internal/views/coord/loadmgr"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSchemaHandoffRequiresPublishedMetadata(t *testing.T) {
	for _, tc := range []struct {
		name      string
		published int32
		loaded    bool
		waitErr   error
		wantErr   error
		waits     int
	}{
		{name: "metadata not published", published: 1, loaded: true, wantErr: merr.ErrServiceNotReady},
		{name: "released", published: 2},
		{name: "handoff", published: 2, loaded: true, waits: 1},
		{name: "release during handoff", published: 2, loaded: true, waitErr: merr.WrapErrCollectionNotLoaded(1), waits: 1},
		{name: "handoff timeout", published: 2, loaded: true, waitErr: context.DeadlineExceeded, wantErr: context.DeadlineExceeded, waits: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stage, waits := 0, 0
			patches := []*mockey.Mocker{
				mockey.Mock((*meta.CoordinatorBroker).DescribeCollection).To(func(_ *meta.CoordinatorBroker, _ context.Context, id int64) (*milvuspb.DescribeCollectionResponse, error) {
					require.Equal(t, int64(1), id)
					stage = 1
					return &milvuspb.DescribeCollectionResponse{Schema: &schemapb.CollectionSchema{Version: tc.published}, VirtualChannelNames: []string{"p_1v0", "p_1v1"}}, nil
				}).Build(),
				mockey.Mock((*loadmgr.CollectionLoadManager).UpdateSchema).To(func(_ *loadmgr.CollectionLoadManager, _ context.Context, id int64, version int32) (bool, error) {
					require.Equal(t, 1, stage)
					require.Equal(t, int32(2), version)
					stage = 2
					return tc.loaded, nil
				}).Build(),
				mockey.Mock((*Server).waitCollectionReady).To(func(_ *Server, _ context.Context, req *querypb.EnsureCollectionReadyRequest) error {
					require.Equal(t, 2, stage)
					require.Equal(t, []string{"p_1v0", "p_1v1"}, req.ExpectedVchannels)
					waits++
					return tc.waitErr
				}).Build(),
			}
			defer func() {
				for _, patch := range patches {
					patch.UnPatch()
				}
			}()
			s := &Server{broker: &meta.CoordinatorBroker{}, qviewsRuntime: &qviewsRuntime{loadManager: &loadmgr.CollectionLoadManager{}}}
			version := int32(2)
			err := s.updateQueryViewSchema(t.Context(), &querypb.UpdateLoadConfigRequest{CollectionIDs: []int64{1}, SchemaVersion: &version})
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tc.waits, waits)
		})
	}
}
