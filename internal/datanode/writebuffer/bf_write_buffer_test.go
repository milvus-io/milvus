package writebuffer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	milvus_storage "github.com/milvus-io/milvus-storage/go/storage"
	"github.com/milvus-io/milvus-storage/go/storage/options"
	"github.com/milvus-io/milvus-storage/go/storage/schema"
	"github.com/milvus-io/milvus/internal/datanode/broker"
	"github.com/milvus-io/milvus/internal/datanode/metacache"
	"github.com/milvus-io/milvus/internal/datanode/syncmgr"
	"github.com/milvus-io/milvus/internal/proto/datapb"
	"github.com/milvus-io/milvus/internal/querycoordv2/params"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/tsoutil"
)

type BFWriteBufferSuite struct {
	suite.Suite
	collID         int64
	channelName    string
	collSchema     *schemapb.CollectionSchema
	syncMgr        *syncmgr.MockSyncManager
	metacache      *metacache.MockMetaCache
	broker         *broker.MockBroker
	storageV2Cache *metacache.StorageV2Cache
}

func (s *BFWriteBufferSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	s.collID = 100
	s.collSchema = &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64,
			},
			{
				FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64,
			},
			{
				FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
			},
			{
				FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: "128"},
				},
			},
		},
	}
}

func (s *BFWriteBufferSuite) composeInsertMsg(segmentID int64, rowCount int, dim int) ([]int64, *msgstream.InsertMsg) {
	tss := lo.RepeatBy(rowCount, func(idx int) int64 { return int64(tsoutil.ComposeTSByTime(time.Now(), int64(idx))) })
	vectors := lo.RepeatBy(rowCount, func(_ int) []float32 {
		return lo.RepeatBy(dim, func(_ int) float32 { return rand.Float32() })
	})
	flatten := lo.Flatten(vectors)
	return tss, &msgstream.InsertMsg{
		InsertRequest: msgpb.InsertRequest{
			SegmentID:  segmentID,
			Version:    msgpb.InsertDataVersion_ColumnBased,
			RowIDs:     tss,
			Timestamps: lo.Map(tss, func(id int64, _ int) uint64 { return uint64(id) }),
			FieldsData: []*schemapb.FieldData{
				{
					FieldId: common.RowIDField, FieldName: common.RowIDFieldName, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: tss,
								},
							},
						},
					},
				},
				{
					FieldId: common.TimeStampField, FieldName: common.TimeStampFieldName, Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: tss,
								},
							},
						},
					},
				},
				{
					FieldId: common.StartOfUserFieldID, FieldName: "pk", Type: schemapb.DataType_Int64,
					Field: &schemapb.FieldData_Scalars{
						Scalars: &schemapb.ScalarField{
							Data: &schemapb.ScalarField_LongData{
								LongData: &schemapb.LongArray{
									Data: tss,
								},
							},
						},
					},
				},
				{
					FieldId: common.StartOfUserFieldID + 1, FieldName: "vector", Type: schemapb.DataType_FloatVector,
					Field: &schemapb.FieldData_Vectors{
						Vectors: &schemapb.VectorField{
							Dim: int64(dim),
							Data: &schemapb.VectorField_FloatVector{
								FloatVector: &schemapb.FloatArray{
									Data: flatten,
								},
							},
						},
					},
				},
			},
		},
	}
}

func (s *BFWriteBufferSuite) composeDeleteMsg(pks []storage.PrimaryKey) *msgstream.DeleteMsg {
	delMsg := &msgstream.DeleteMsg{
		DeleteRequest: msgpb.DeleteRequest{
			PrimaryKeys: storage.ParsePrimaryKeys2IDs(pks),
			Timestamps:  lo.RepeatBy(len(pks), func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)) }),
		},
	}
	return delMsg
}

func (s *BFWriteBufferSuite) SetupTest() {
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.metacache.EXPECT().Schema().Return(s.collSchema).Maybe()
	s.metacache.EXPECT().Collection().Return(s.collID).Maybe()
	s.broker = broker.NewMockBroker(s.T())
	var err error
	s.storageV2Cache, err = metacache.NewStorageV2Cache(s.collSchema)
	s.Require().NoError(err)
}

func (s *BFWriteBufferSuite) TestBufferData() {
	wb, err := NewBFWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, &writeBufferOption{})
	s.NoError(err)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1000}, metacache.NewBloomFilterSet())
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().GetSegmentByID(int64(1000)).Return(nil, false)
	s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything).Return()
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
	s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything, mock.Anything).Return([]int64{})

	pks, msg := s.composeInsertMsg(1000, 10, 128)
	delMsg := s.composeDeleteMsg(lo.Map(pks, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) }))

	err = wb.BufferData([]*msgstream.InsertMsg{msg}, []*msgstream.DeleteMsg{delMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
	s.NoError(err)
}

func (s *BFWriteBufferSuite) TestAutoSync() {
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlushInsertBufferSize.Key, "1")

	s.Run("normal_auto_sync", func() {
		wb, err := NewBFWriteBuffer(s.channelName, s.metacache, nil, s.syncMgr, &writeBufferOption{
			syncPolicies: []SyncPolicy{
				GetFullBufferPolicy(),
				GetSyncStaleBufferPolicy(paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)),
				GetFlushingSegmentsPolicy(s.metacache),
			},
		})
		s.NoError(err)

		seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1000}, metacache.NewBloomFilterSet())
		s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
		s.metacache.EXPECT().GetSegmentByID(int64(1000)).Return(nil, false)
		s.metacache.EXPECT().GetSegmentByID(int64(1002)).Return(seg, true)
		s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return([]int64{1002})
		s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything, mock.Anything).Return([]int64{})
		s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything).Return()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything).Return(nil)

		pks, msg := s.composeInsertMsg(1000, 10, 128)
		delMsg := s.composeDeleteMsg(lo.Map(pks, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) }))

		err = wb.BufferData([]*msgstream.InsertMsg{msg}, []*msgstream.DeleteMsg{delMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.NoError(err)
	})
}

func (s *BFWriteBufferSuite) TestBufferDataWithStorageV2() {
	params.Params.CommonCfg.EnableStorageV2.SwapTempValue("true")
	params.Params.CommonCfg.StorageScheme.SwapTempValue("file")
	tmpDir := s.T().TempDir()
	arrowSchema, err := metacache.ConvertToArrowSchema(s.collSchema.Fields)
	s.Require().NoError(err)
	space, err := milvus_storage.Open(fmt.Sprintf("file:///%s", tmpDir), options.NewSpaceOptionBuilder().
		SetSchema(schema.NewSchema(arrowSchema, &schema.SchemaOptions{
			PrimaryColumn: "pk", VectorColumn: "vector", VersionColumn: common.TimeStampFieldName,
		})).Build())
	s.Require().NoError(err)
	s.storageV2Cache.SetSpace(1000, space)
	wb, err := NewBFWriteBuffer(s.channelName, s.metacache, s.storageV2Cache, s.syncMgr, &writeBufferOption{})
	s.NoError(err)

	seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1000}, metacache.NewBloomFilterSet())
	s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg})
	s.metacache.EXPECT().GetSegmentByID(int64(1000)).Return(nil, false)
	s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything, mock.Anything).Return([]int64{})
	s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything).Return()
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

	pks, msg := s.composeInsertMsg(1000, 10, 128)
	delMsg := s.composeDeleteMsg(lo.Map(pks, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) }))

	err = wb.BufferData([]*msgstream.InsertMsg{msg}, []*msgstream.DeleteMsg{delMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
	s.NoError(err)
}

func (s *BFWriteBufferSuite) TestAutoSyncWithStorageV2() {
	params.Params.CommonCfg.EnableStorageV2.SwapTempValue("true")
	paramtable.Get().Save(paramtable.Get().DataNodeCfg.FlushInsertBufferSize.Key, "1")
	tmpDir := s.T().TempDir()
	arrowSchema, err := metacache.ConvertToArrowSchema(s.collSchema.Fields)
	s.Require().NoError(err)

	space, err := milvus_storage.Open(fmt.Sprintf("file:///%s", tmpDir), options.NewSpaceOptionBuilder().
		SetSchema(schema.NewSchema(arrowSchema, &schema.SchemaOptions{
			PrimaryColumn: "pk", VectorColumn: "vector", VersionColumn: common.TimeStampFieldName,
		})).Build())
	s.Require().NoError(err)
	s.storageV2Cache.SetSpace(1002, space)

	s.Run("normal_auto_sync", func() {
		wb, err := NewBFWriteBuffer(s.channelName, s.metacache, s.storageV2Cache, s.syncMgr, &writeBufferOption{
			syncPolicies: []SyncPolicy{
				GetFullBufferPolicy(),
				GetSyncStaleBufferPolicy(paramtable.Get().DataNodeCfg.SyncPeriod.GetAsDuration(time.Second)),
				GetFlushingSegmentsPolicy(s.metacache),
			},
		})
		s.NoError(err)

		seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1000}, metacache.NewBloomFilterSet())
		segCompacted := metacache.NewSegmentInfo(&datapb.SegmentInfo{ID: 1000}, metacache.NewBloomFilterSet())
		metacache.CompactTo(2001)(segCompacted)

		s.metacache.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg, segCompacted})
		s.metacache.EXPECT().GetSegmentByID(int64(1000)).Return(nil, false)
		s.metacache.EXPECT().GetSegmentByID(int64(1002)).Return(seg, true)
		s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything).Return([]int64{1002})
		s.metacache.EXPECT().GetSegmentIDsBy(mock.Anything, mock.Anything).Return([]int64{1003}) // mocked compacted
		s.metacache.EXPECT().RemoveSegments(mock.Anything).Return([]int64{1003})
		s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything).Return()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything, mock.Anything).Return()
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.Anything).Return(nil)

		pks, msg := s.composeInsertMsg(1000, 10, 128)
		delMsg := s.composeDeleteMsg(lo.Map(pks, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) }))

		err = wb.BufferData([]*msgstream.InsertMsg{msg}, []*msgstream.DeleteMsg{delMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200})
		s.NoError(err)
	})
}

func TestBFWriteBuffer(t *testing.T) {
	suite.Run(t, new(BFWriteBufferSuite))
}
