package datacoord

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/segcore/loadresource"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryViewSegmentLoadInfoPreservesV3ResourceStatistics(t *testing.T) {
	segment := &datapb.SegmentInfo{
		ID: 10, CollectionID: 1, PartitionID: 2, NumOfRows: 100,
		StorageVersion: storage.StorageV3, ManifestPath: "segment/_manifest/1.manifest",
		Stats: &datapb.Statistics{
			InsertBinlogSize: 1600, DeltaBinlogSize: 64,
			LoadResource: &datapb.LoadResourceStatistics{ColumnGroups: []*datapb.ColumnGroupStatistics{
				{GroupId: 100, FieldIds: []int64{100, 101}, MemorySize: 1600},
			}},
		},
	}
	info := (&Server{}).packQueryViewSegmentLoadInfo(segment, nil, nil)
	require.True(t, proto.Equal(segment.GetStats(), info.GetStats()))
	require.Empty(t, info.GetBinlogPaths())
	require.Empty(t, info.GetDeltalogs())
	binlogs, deltalogs := loadresource.ResolveSegmentEstimateLogs(&schemapb.CollectionSchema{}, info)
	require.Equal(t, []*datapb.FieldBinlog{{
		FieldID: 100, ChildFields: []int64{100, 101},
		Binlogs: []*datapb.Binlog{{EntriesNum: 100, MemorySize: 1600}},
	}}, binlogs)
	require.Equal(t, []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{MemorySize: 64}}}}, deltalogs)
}

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
