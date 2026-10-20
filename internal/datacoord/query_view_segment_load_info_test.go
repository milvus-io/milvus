package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewSegmentLoadInfoRejectsUnknownReplayCursor(t *testing.T) {
	m := &meta{indexMeta: &indexMeta{}}
	patchIndexes := mockey.Mock((*indexMeta).getQueryViewIndexSnapshot).Return([]*model.Index(nil), map[int64]map[UniqueID]*model.SegmentIndex(nil)).Build()
	defer patchIndexes.UnPatch()
	patchSegment := mockey.Mock((*meta).GetSegment).Return(&SegmentInfo{SegmentInfo: &datapb.SegmentInfo{ID: 10, CollectionID: 1}}).Build()
	defer patchSegment.UnPatch()
	s := &Server{meta: m}
	s.UpdateStateCode(commonpb.StateCode_Healthy)
	infos, indexes, err := s.GetQueryViewSegmentLoadInfos(context.Background(), 1, []int64{10})
	require.ErrorIs(t, err, merr.ErrServiceNotReady)
	require.Nil(t, infos)
	require.Nil(t, indexes)
}
