package rootcoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSchemaProtocolAdmission(t *testing.T) {
	for _, role := range []string{typeutil.ProxyRole, typeutil.StreamingNodeRole, typeutil.QueryNodeRole} {
		for _, protocol := range []string{"", "unsupported", common.QueryViewSchemaProtocol} {
			t.Run(role+"/"+protocol, func(t *testing.T) {
				mockey.PatchConvey("reject before broadcasting an unsupported schema", t, func() {
					core := &Core{session: &sessionutil.Session{}}
					mockey.Mock((*sessionutil.Session).GetSessions).To(func(_ *sessionutil.Session, _ context.Context, requested string) (map[string]*sessionutil.Session, int64, error) {
						value := common.QueryViewSchemaProtocol
						if requested == role {
							value = protocol
						}
						return map[string]*sessionutil.Session{"node": {SessionRaw: sessionutil.SessionRaw{ServerID: 42, QueryViewSchemaProtocol: value}}}, 1, nil
					}).Build()
					broadcasts := 0
					mockey.Mock((*mock_broadcaster.MockBroadcastAPI).Broadcast).To(func(_ *mock_broadcaster.MockBroadcastAPI, _ context.Context, _ message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
						broadcasts++
						return &types.BroadcastAppendResult{}, nil
					}).Build()
					msg := message.NewAlterCollectionMessageBuilderV2().
						WithHeader(&messagespb.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionSchema}}}).
						WithBody(&messagespb.AlterCollectionMessageBody{}).WithBroadcast([]string{"p0_1v0"}).MustBuildBroadcast()
					err := core.broadcastCollectionAlter(t.Context(), &mock_broadcaster.MockBroadcastAPI{}, msg)
					if protocol == common.QueryViewSchemaProtocol {
						require.NoError(t, err)
						require.Equal(t, 1, broadcasts)
					} else {
						require.ErrorIs(t, err, merr.ErrServiceUnavailable)
						require.Contains(t, err.Error(), role)
						require.Zero(t, broadcasts)
					}
				})
			})
		}
	}
}

func TestSchemaProtocolAdmissionDiscoveryErrorAndMetadataOnly(t *testing.T) {
	mockey.PatchConvey("discovery failure blocks schema but not metadata", t, func() {
		core := &Core{session: &sessionutil.Session{}}
		mockey.Mock((*sessionutil.Session).GetSessions).Return(nil, int64(0), merr.ErrServiceUnavailable).Build()
		broadcasts := 0
		mockey.Mock((*mock_broadcaster.MockBroadcastAPI).Broadcast).To(func(_ *mock_broadcaster.MockBroadcastAPI, _ context.Context, _ message.BroadcastMutableMessage) (*types.BroadcastAppendResult, error) {
			broadcasts++
			return &types.BroadcastAppendResult{}, nil
		}).Build()
		for _, mask := range []string{message.FieldMaskCollectionSchema, message.FieldMaskCollectionProperties} {
			msg := message.NewAlterCollectionMessageBuilderV2().WithHeader(&messagespb.AlterCollectionMessageHeader{UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{mask}}}).WithBody(&messagespb.AlterCollectionMessageBody{}).WithBroadcast([]string{"p0_1v0"}).MustBuildBroadcast()
			err := core.broadcastCollectionAlter(t.Context(), &mock_broadcaster.MockBroadcastAPI{}, msg)
			if mask == message.FieldMaskCollectionSchema {
				require.ErrorIs(t, err, merr.ErrServiceUnavailable)
			} else {
				require.NoError(t, err)
			}
		}
		require.Equal(t, 1, broadcasts)
	})
}
