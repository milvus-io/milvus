package writebuffer

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/samber/lo"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/testutils"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type L0WriteBufferSuite struct {
	testutils.PromMetricsSuite
	channelName string
	collID      int64
	collSchema  *schemapb.CollectionSchema
	pkSchema    *schemapb.FieldSchema
	syncMgr     *syncmgr.MockSyncManager
	metacache   *metacache.MockMetaCache
	allocator   *allocator.MockGIDAllocator
}

func (s *L0WriteBufferSuite) SetupSuite() {
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

	for _, field := range s.collSchema.Fields {
		if field.GetIsPrimaryKey() {
			s.pkSchema = field
			break
		}
	}
	s.channelName = "by-dev-rootcoord-dml_0v0"
}

func (s *L0WriteBufferSuite) composeInsertMsg(segmentID int64, rowCount int, dim int, pkType schemapb.DataType) ([]int64, *msgstream.InsertMsg) {
	tss := lo.RepeatBy(rowCount, func(idx int) int64 { return int64(tsoutil.ComposeTSByTimeWithLogical(time.Now(), int64(idx))) })
	vectors := lo.RepeatBy(rowCount, func(_ int) []float32 {
		return lo.RepeatBy(dim, func(_ int) float32 { return rand.Float32() })
	})
	flatten := lo.Flatten(vectors)
	var pkField *schemapb.FieldData
	switch pkType {
	case schemapb.DataType_Int64:
		pkField = &schemapb.FieldData{
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
		}
	case schemapb.DataType_VarChar:
		pkField = &schemapb.FieldData{
			FieldId: common.StartOfUserFieldID, FieldName: "pk", Type: schemapb.DataType_VarChar,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: lo.Map(tss, func(v int64, _ int) string { return fmt.Sprintf("%v", v) }),
						},
					},
				},
			},
		}
	}
	return tss, &msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
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
				pkField,
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

func (s *L0WriteBufferSuite) textSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_text_collection",
		Fields: append(append([]*schemapb.FieldSchema{}, s.collSchema.Fields...), &schemapb.FieldSchema{
			FieldID:  common.StartOfUserFieldID + 2,
			Name:     "text",
			DataType: schemapb.DataType_Text,
		}),
	}
}

func (s *L0WriteBufferSuite) composeTextInsertMsg(segmentID int64, rowCount int) ([]int64, *msgstream.InsertMsg) {
	pks, msg := s.composeInsertMsg(segmentID, rowCount, 128, schemapb.DataType_Int64)
	msg.FieldsData = append(msg.FieldsData, &schemapb.FieldData{
		FieldId:   common.StartOfUserFieldID + 2,
		FieldName: "text",
		Type:      schemapb.DataType_Text,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: lo.RepeatBy(rowCount, func(idx int) string {
							return fmt.Sprintf("text-%d", idx)
						}),
					},
				},
			},
		},
	})
	return pks, msg
}

func (s *L0WriteBufferSuite) composeDeleteMsg(pks []storage.PrimaryKey) *msgstream.DeleteMsg {
	delMsg := &msgstream.DeleteMsg{
		DeleteRequest: &msgpb.DeleteRequest{
			PrimaryKeys: storage.ParsePrimaryKeys2IDs(pks),
			Timestamps:  lo.RepeatBy(len(pks), func(idx int) uint64 { return tsoutil.ComposeTSByTimeWithLogical(time.Now(), int64(idx)+1) }),
		},
	}
	return delMsg
}

func (s *L0WriteBufferSuite) SetupTest() {
	s.syncMgr = syncmgr.NewMockSyncManager(s.T())
	s.metacache = metacache.NewMockMetaCache(s.T())
	s.metacache.EXPECT().GetSchema(mock.Anything).Return(s.collSchema).Maybe()
	s.metacache.EXPECT().Collection().Return(s.collID).Maybe()
	s.allocator = allocator.NewMockGIDAllocator()
	s.allocator.AllocOneF = func() (int64, error) { return int64(tsoutil.ComposeTSByTime(time.Now())), nil }
}

func (s *L0WriteBufferSuite) newTextMetaCache(schema *schemapb.CollectionSchema) *metacache.MockMetaCache {
	mc := metacache.NewMockMetaCache(s.T())
	mc.EXPECT().GetSchema(mock.Anything).Return(schema).Maybe()
	mc.EXPECT().Collection().Return(s.collID).Maybe()
	return mc
}

func (s *L0WriteBufferSuite) newTextRealMetaCache(schema *schemapb.CollectionSchema) metacache.MetaCache {
	return metacache.NewMetaCache(&datapb.ChannelWatchInfo{
		Schema: schema,
		Vchan: &datapb.VchannelInfo{
			CollectionID: s.collID,
			ChannelName:  s.channelName,
		},
	}, func(*datapb.SegmentInfo) pkoracle.PkStat {
		return pkoracle.NewBloomFilterSet()
	}, metacache.NewBM25StatsFactory)
}

func (s *L0WriteBufferSuite) TestBufferData() {
	s.Run("normal_run", func() {

		wb, err := NewL0WriteBuffer(s.channelName, s.metacache, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
		})
		s.NoError(err)

		pks, msg := s.composeInsertMsg(1000, 10, 128, schemapb.DataType_Int64)
		delMsg := s.composeDeleteMsg(lo.Map(pks, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) }))

		s.metacache.EXPECT().GetSegmentByID(int64(1000)).Return(nil, false).Once()
		s.metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
		s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

		metrics.DataNodeFlowGraphBufferDataSize.Reset()
		insertData, err := PrepareInsert(s.collSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, []*msgstream.DeleteMsg{delMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		value, err := metrics.DataNodeFlowGraphBufferDataSize.GetMetricWithLabelValues(paramtable.GetStringNodeID(), fmt.Sprint(s.metacache.Collection()))
		s.NoError(err)
		s.MetricsEqual(value, 5616)

		delMsg = s.composeDeleteMsg(lo.Map(pks, func(id int64, _ int) storage.PrimaryKey { return storage.NewInt64PrimaryKey(id) }))
		err = wb.BufferData([]*InsertData{}, []*msgstream.DeleteMsg{delMsg}, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		s.MetricsEqual(value, 5856)
	})
}

func TestL0WriteBuffer(t *testing.T) {
	suite.Run(t, new(L0WriteBufferSuite))
}
