package writebuffer

import (
	"context"
	"fmt"
	"math/rand"
	"sync/atomic"
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
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
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

type fakeGrowingFlushSource struct {
	currentOffset int64
	flushFunc     func(context.Context, int64, int64, *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error)
	releaseFunc   func()
}

func (s fakeGrowingFlushSource) CurrentOffset() int64 {
	if s.currentOffset > 0 {
		return s.currentOffset
	}
	return 10
}

func (s fakeGrowingFlushSource) FlushGrowingData(ctx context.Context, startOffset, endOffset int64, config *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
	if s.flushFunc != nil {
		return s.flushFunc(ctx, startOffset, endOffset, config)
	}
	return &syncmgr.GrowingFlushResult{ManifestPath: "manifest", NumRows: 10}, nil
}

func (s fakeGrowingFlushSource) Release() {
	if s.releaseFunc != nil {
		s.releaseFunc()
	}
}

type fakeGrowingSourceProvider struct {
	source syncmgr.GrowingFlushSource
	state  syncmgr.GrowingSourceState
}

func (p fakeGrowingSourceProvider) GetGrowingFlushSource(int64, int64, *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
	return p.source, p.state
}

type fakeGrowingSourceReleaseHandoffProvider struct {
	fakeGrowingSourceProvider
	fenceTs  uint64
	segments []syncmgr.GrowingSourceReleaseHandoffSegment
}

func (p *fakeGrowingSourceReleaseHandoffProvider) PrepareGrowingSourceReleaseHandoff(ctx context.Context, fenceTs uint64, segments []syncmgr.GrowingSourceReleaseHandoffSegment) error {
	p.fenceTs = fenceTs
	p.segments = append([]syncmgr.GrowingSourceReleaseHandoffSegment(nil), segments...)
	return nil
}

func (p *fakeGrowingSourceReleaseHandoffProvider) IsReleasePrepared(segmentID int64, checkpointTs uint64) bool {
	if p.fenceTs != 0 && checkpointTs > p.fenceTs {
		return false
	}
	for _, segment := range p.segments {
		if segment.SegmentID == segmentID && segment.TargetOffset > 0 {
			return true
		}
	}
	return false
}

func (p *fakeGrowingSourceReleaseHandoffProvider) IsReleaseAllowed(segmentID int64, checkpointTs uint64) bool {
	if p.fenceTs != 0 && checkpointTs > p.fenceTs {
		return false
	}
	for _, segment := range p.segments {
		if segment.SegmentID == segmentID {
			return true
		}
	}
	return false
}

func (p *fakeGrowingSourceReleaseHandoffProvider) ClearReleasePrepared(segmentID int64) {
}

func (p *fakeGrowingSourceReleaseHandoffProvider) MarkReleaseDetached(segmentID int64) {
}

func (p *fakeGrowingSourceReleaseHandoffProvider) ReleasePreparedSegments() []int64 {
	return nil
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
	tss := lo.RepeatBy(rowCount, func(idx int) int64 { return int64(tsoutil.ComposeTSByTime(time.Now(), int64(idx))) })
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
			Timestamps:  lo.RepeatBy(len(pks), func(idx int) uint64 { return tsoutil.ComposeTSByTime(time.Now(), int64(idx)+1) }),
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
	s.allocator.AllocOneF = func() (int64, error) { return int64(tsoutil.ComposeTSByTime(time.Now(), 0)), nil }
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
		paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "false")
		defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)

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

func (s *L0WriteBufferSuite) TestBufferDataGrowingSourceMode() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)

	s.Run("no_usable_source_buffers_payload", func() {
		textSchema := s.textSchema()
		metacache := s.newTextMetaCache(textSchema)
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1000, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		// GetSegmentByID is invoked three times per BufferData on the first
		// insert: (1) decideGrowingFlushSource reads the sticky flushSourceMode,
		// (2) growingSourceBaseOffset reads the row counter, (3) CreateNewGrowingSegment
		// checks for prior existence.
		metacache.EXPECT().GetSegmentByID(int64(1000)).Return(nil, false).Times(3)
		metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
		metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		segBuf := l0wb.buffers[int64(1000)]
		s.NotNil(segBuf)
		s.False(segBuf.insertBuffer.IsEmpty())
		s.EqualValues(100, wb.GetCheckpoint().GetTimestamp())
	})

	s.Run("usable_source_records_progress_and_pins_checkpoint", func() {
		textSchema := s.textSchema()
		metacache := s.newTextMetaCache(textSchema)
		var resolvedSegmentID int64
		var resolvedTargetOffset int64
		resolveCalls := 0
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				resolveCalls++
				resolvedSegmentID = segmentID
				resolvedTargetOffset = targetOffset
				if resolveCalls == 1 {
					return fakeGrowingFlushSource{}, syncmgr.GrowingSourceUsable
				}
				return nil, syncmgr.GrowingSourceUnavailable
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1001, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		// 3 first-insert path calls plus 1 triggerSync policy check for the
		// recorded growing source progress.
		metacache.EXPECT().GetSegmentByID(int64(1001)).Return(nil, false).Times(4)
		metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
		metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.Nil(l0wb.buffers[int64(1001)])
		s.Contains(l0wb.growingSourceProgress, int64(1001))
		s.EqualValues(1001, resolvedSegmentID)
		s.EqualValues(10, resolvedTargetOffset)
		s.EqualValues(100, wb.GetCheckpoint().GetTimestamp())
	})

	s.Run("writebuffer_source_is_sticky_after_payload_yielded", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		sourceState := syncmgr.GrowingSourceUnavailable
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, sourceState
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1101, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.NotNil(l0wb.buffers[int64(1101)])
		s.NotContains(l0wb.growingSourceProgress, int64(1101))

		_, err = l0wb.getSyncTask(context.Background(), 1101)
		s.NoError(err)
		s.Nil(l0wb.buffers[int64(1101)])

		sourceState = syncmgr.GrowingSourceUsable
		_, msg = s.composeTextInsertMsg(1101, 5)
		insertData, err = PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 300}, &msgpb.MsgPosition{Timestamp: 400}, 100)
		s.NoError(err)

		s.NotNil(l0wb.buffers[int64(1101)])
		s.NotContains(l0wb.growingSourceProgress, int64(1101))
	})

	s.Run("close_stops_growing_source_retry_timer", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		sourceState := syncmgr.GrowingSourcePending
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, sourceState
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1102, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.False(l0wb.growingSourceRetryScheduled)
		err = wb.SealSegments(context.Background(), []int64{1102})
		s.NoError(err)
		l0wb.EvictBuffer()
		s.True(l0wb.growingSourceRetryScheduled)
		wb.Close(context.Background(), false)
		sourceState = syncmgr.GrowingSourceUsable
		time.Sleep(50 * time.Millisecond)
	})

	s.Run("drop_close_skips_unavailable_growing_source", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			metaWriter:                 metaWriter,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourcePending
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1103, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		metaWriter.EXPECT().DropChannel(mock.Anything, s.channelName).Return(nil).Once()
		s.NotPanics(func() {
			wb.Close(context.Background(), true)
		})

		l0wb := wb.(*l0WriteBuffer)
		s.NotContains(l0wb.growingSourceProgress, int64(1103))
		// flushSourceMode lives on metacache.SegmentInfo and is reclaimed
		// when the segment is removed from metacache by the drop path; no
		// dedicated map to assert against on the writeBuffer side anymore.
	})

	s.Run("pending_source_records_progress_instead_of_falling_back_to_writebuffer", func() {
		textSchema := s.textSchema()
		metacache := s.newTextMetaCache(textSchema)
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourcePending
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1002, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		// 3 first-insert path calls plus 1 triggerSync policy check.
		metacache.EXPECT().GetSegmentByID(int64(1002)).Return(nil, false).Times(4)
		metacache.EXPECT().AddSegment(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return()
		metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return()

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.Nil(l0wb.buffers[int64(1002)])
		s.Contains(l0wb.growingSourceProgress, int64(1002))
		s.EqualValues(100, wb.GetCheckpoint().GetTimestamp())
	})

	s.Run("pending_source_syncs_after_it_becomes_usable_and_acks_progress", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		sourceState := syncmgr.GrowingSourcePending
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, sourceState
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1003, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.Nil(l0wb.buffers[int64(1003)])
		s.Contains(l0wb.growingSourceProgress, int64(1003))
		s.EqualValues(100, wb.GetCheckpoint().GetTimestamp())

		sourceState = syncmgr.GrowingSourceUsable
		taskCh := make(chan *syncmgr.GrowingSourceSyncTask, 1)
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				taskCh <- textTask
				return conc.Go(func() (struct{}, error) {
					for _, callback := range callbacks {
						if err := callback(nil); err != nil {
							return struct{}{}, err
						}
					}
					return struct{}{}, nil
				}), nil
			}).Once()

		futures := l0wb.syncSegments(context.Background(), []int64{1003})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		textTask := <-taskCh
		s.EqualValues(1003, textTask.SegmentID())
		s.EqualValues(10, textTask.TargetOffset())
		s.EqualValues(10, textTask.BatchRows())
		s.EqualValues(100, textTask.StartPosition().GetTimestamp())
		s.EqualValues(200, textTask.Checkpoint().GetTimestamp())
		s.Contains(l0wb.growingSourceProgress, int64(1003))
		s.Empty(l0wb.growingSourceProgress[int64(1003)].batches)
		s.EqualValues(200, wb.GetCheckpoint().GetTimestamp())
	})

	s.Run("source_becomes_unavailable_when_building_task_retries_without_fatal", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		resolveCalls := 0
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				resolveCalls++
				if resolveCalls <= 3 {
					return fakeGrowingFlushSource{}, syncmgr.GrowingSourceUsable
				}
				return nil, syncmgr.GrowingSourceUnavailable
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1009, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.Contains(l0wb.growingSourceProgress, int64(1009))
		s.EqualValues(100, wb.GetCheckpoint().GetTimestamp())
		segment, ok := metacache.GetSegmentByID(1009)
		s.True(ok)
		s.Equal(commonpb.SegmentState_Growing, segment.State())
	})

	s.Run("pending_source_syncs_after_source_becomes_usable", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		sourceState := atomic.Int32{}
		sourceState.Store(int32(syncmgr.GrowingSourcePending))
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourceState(sourceState.Load())
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1007, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		taskCh := make(chan *syncmgr.GrowingSourceSyncTask, 1)
		doneCh := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				taskCh <- textTask
				return conc.Go(func() (struct{}, error) {
					defer close(doneCh)
					for _, callback := range callbacks {
						if err := callback(nil); err != nil {
							return struct{}{}, err
						}
					}
					return struct{}{}, nil
				}), nil
			}).Once()

		sourceState.Store(int32(syncmgr.GrowingSourceUsable))
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1007})
		s.Require().Len(futures, 1)
		textTask := <-taskCh
		s.EqualValues(1007, textTask.SegmentID())
		s.NoError(conc.AwaitAll(futures...))
		<-doneCh
		l0wb.mut.RLock()
		defer l0wb.mut.RUnlock()
		s.Contains(l0wb.growingSourceProgress, int64(1007))
		s.Empty(l0wb.growingSourceProgress[int64(1007)].batches)
	})

	s.Run("source_task_run_flushes_source_updates_meta_and_acks_progress", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
			metaWriter:  metaWriter,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1006, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask")).
			Run(func(_ context.Context, task *syncmgr.GrowingSourceSyncTask) {
				s.Equal("manifest", task.ManifestPath())
				s.EqualValues(10, task.BatchRows())
			}).Return(nil).Once()
		done := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					defer close(done)
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					return struct{}{}, err
				}), nil
			}).Once()

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1006})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		<-done

		s.Contains(l0wb.growingSourceProgress, int64(1006))
		s.Empty(l0wb.growingSourceProgress[int64(1006)].batches)
		segment, ok := metacache.GetSegmentByID(1006)
		s.True(ok)
		s.EqualValues(10, segment.FlushedRows())
		s.Equal("manifest", segment.ManifestPath())
	})

	s.Run("drop_close_reuses_synced_manifest_for_zero_row_growing_source_task", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		flushCalls := 0
		source := fakeGrowingFlushSource{
			flushFunc: func(_ context.Context, startOffset, endOffset int64, _ *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
				flushCalls++
				s.EqualValues(0, startOffset)
				s.EqualValues(10, endOffset)
				return &syncmgr.GrowingFlushResult{ManifestPath: "manifest-0-10", NumRows: 10}, nil
			},
		}
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
			metaWriter:  metaWriter,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return source, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		updateCalls := 0
		metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask")).
			Run(func(_ context.Context, task *syncmgr.GrowingSourceSyncTask) {
				updateCalls++
				if updateCalls == 1 {
					s.False(task.IsDrop())
					s.EqualValues(10, task.BatchRows())
				} else {
					s.True(task.IsDrop())
					s.EqualValues(0, task.BatchRows())
				}
				s.Equal("manifest-0-10", task.ManifestPath())
			}).Return(nil).Twice()
		metaWriter.EXPECT().DropChannel(mock.Anything, s.channelName).Return(nil).Once()

		done := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					if !textTask.IsDrop() {
						close(done)
					}
					return struct{}{}, err
				}), nil
			}).Twice()

		_, msg := s.composeTextInsertMsg(1013, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1013})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		<-done

		s.NotPanics(func() {
			wb.Close(context.Background(), true)
		})
		s.Equal(1, flushCalls)
	})

	s.Run("source_task_error_retries_without_default_error_handler_panic", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		flushCalls := 0
		source := fakeGrowingFlushSource{
			flushFunc: func(context.Context, int64, int64, *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
				flushCalls++
				if flushCalls == 1 {
					return nil, fmt.Errorf("mock growing source flush error")
				}
				return &syncmgr.GrowingFlushResult{ManifestPath: "manifest-retry", NumRows: 10}, nil
			},
		}
		errorHandlerCalled := false
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			errorHandler: func(error) {
				errorHandlerCalled = true
				panic("growing source task should not call writebuffer error handler directly")
			},
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return source, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		done := make(chan *syncmgr.GrowingSourceSyncTask, 2)
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					defer func() {
						done <- textTask
					}()
					err := textTask.Run(ctx)
					textTask.HandleError(err)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					return struct{}{}, nil
				}), nil
			}).Twice()

		_, msg := s.composeTextInsertMsg(1010, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1010})
		s.Require().Len(futures, 1)
		s.ErrorContains(conc.AwaitAll(futures...), "mock growing source flush error")
		firstTask := <-done

		s.False(errorHandlerCalled)
		progress, ok := l0wb.growingSourceProgress[int64(1010)]
		s.True(ok)
		s.EqualValues(1, progress.failureCount)
		s.Contains(progress.lastFailure, "mock growing source flush error")
		s.EqualValues(10, firstTask.BatchRows())
		segment, ok := metacache.GetSegmentByID(1010)
		s.True(ok)
		s.EqualValues(0, segment.FlushedRows())
		s.EqualValues(0, segment.SyncingRows())
		s.EqualValues(10, segment.BufferRows())

		futures = l0wb.syncSegments(context.Background(), []int64{1010})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		secondTask := <-done
		s.EqualValues(10, secondTask.BatchRows())
		segment, ok = metacache.GetSegmentByID(1010)
		s.True(ok)
		s.EqualValues(10, segment.FlushedRows())
		s.EqualValues(0, segment.SyncingRows())
		s.Equal("manifest-retry", segment.ManifestPath())
	})

	s.Run("source_task_row_count_mismatch_retries_without_meta_update", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		source := fakeGrowingFlushSource{
			flushFunc: func(context.Context, int64, int64, *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
				return &syncmgr.GrowingFlushResult{ManifestPath: "manifest-mismatch", NumRows: 9}, nil
			},
		}
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		errorHandlerCalled := false
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			metaWriter:                 metaWriter,
			growingSourceRetryInterval: time.Hour,
			errorHandler: func(error) {
				errorHandlerCalled = true
				panic("growing source row count mismatch should not call writebuffer error handler directly")
			},
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return source, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		done := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					defer close(done)
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					return struct{}{}, nil
				}), nil
			}).Once()

		_, msg := s.composeTextInsertMsg(1011, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1011})
		s.Require().Len(futures, 1)
		s.ErrorContains(conc.AwaitAll(futures...), "row count mismatch")
		<-done

		s.False(errorHandlerCalled)
		progress, ok := l0wb.growingSourceProgress[int64(1011)]
		s.True(ok)
		s.EqualValues(1, progress.failureCount)
		s.Contains(progress.lastFailure, "row count mismatch")
		segment, ok := metacache.GetSegmentByID(1011)
		s.True(ok)
		s.EqualValues(0, segment.FlushedRows())
		s.NotEqual("manifest-mismatch", segment.ManifestPath())
	})

	s.Run("source_task_uses_absolute_target_offset_after_partial_flush", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		var ranges [][2]int64
		source := fakeGrowingFlushSource{
			currentOffset: 15,
			flushFunc: func(_ context.Context, startOffset, endOffset int64, _ *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
				ranges = append(ranges, [2]int64{startOffset, endOffset})
				return &syncmgr.GrowingFlushResult{ManifestPath: fmt.Sprintf("manifest-%d", endOffset), NumRows: endOffset - startOffset}, nil
			},
		}
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return source, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		done := make(chan struct{}, 2)
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					done <- struct{}{}
					return struct{}{}, err
				}), nil
			}).Twice()

		_, msg := s.composeTextInsertMsg(1008, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1008})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		<-done

		segment, ok := metacache.GetSegmentByID(1008)
		s.True(ok)
		s.EqualValues(10, segment.FlushedRows())

		_, msg = s.composeTextInsertMsg(1008, 5)
		insertData, err = PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 300}, &msgpb.MsgPosition{Timestamp: 400}, 100)
		s.NoError(err)
		futures = l0wb.syncSegments(context.Background(), []int64{1008})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		<-done

		segment, ok = metacache.GetSegmentByID(1008)
		s.True(ok)
		s.EqualValues(15, segment.FlushedRows())
		s.Equal([][2]int64{{0, 10}, {10, 15}}, ranges)
	})

	s.Run("inflight_source_sync_gets_finalized_when_segment_is_flushed", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		source := fakeGrowingFlushSource{
			currentOffset: 10,
			flushFunc: func(_ context.Context, startOffset, endOffset int64, _ *syncmgr.GrowingFlushConfig) (*syncmgr.GrowingFlushResult, error) {
				return &syncmgr.GrowingFlushResult{
					ManifestPath: fmt.Sprintf("manifest-%d-%d", startOffset, endOffset),
					NumRows:      endOffset - startOffset,
				}, nil
			},
		}
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
			metaWriter:  metaWriter,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return source, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask")).
			Run(func(_ context.Context, task *syncmgr.GrowingSourceSyncTask) {
				if task.IsFlush() {
					s.EqualValues(0, task.BatchRows())
					s.Equal("manifest-0-10", task.ManifestPath())
					return
				}
				s.EqualValues(10, task.BatchRows())
				s.Equal("manifest-0-10", task.ManifestPath())
			}).Return(nil).Twice()

		firstMayRun := make(chan struct{})
		secondTaskCh := make(chan *syncmgr.GrowingSourceSyncTask, 1)
		callCount := 0
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				callCount++
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				if callCount == 1 {
					return conc.Go(func() (struct{}, error) {
						<-firstMayRun
						err := textTask.Run(ctx)
						for _, callback := range callbacks {
							if cbErr := callback(err); cbErr != nil {
								return struct{}{}, cbErr
							}
						}
						return struct{}{}, err
					}), nil
				}
				secondTaskCh <- textTask
				return conc.Go(func() (struct{}, error) {
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					return struct{}{}, err
				}), nil
			}).Twice()

		_, msg := s.composeTextInsertMsg(1012, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1012})
		s.Require().Len(futures, 1)
		err = wb.SealSegments(context.Background(), []int64{1012})
		s.NoError(err)
		l0wb.EvictBuffer()

		close(firstMayRun)
		s.NoError(conc.AwaitAll(futures...))
		secondTask := <-secondTaskCh
		s.True(secondTask.IsFlush())
		s.EqualValues(1012, secondTask.SegmentID())
		s.EqualValues(10, secondTask.TargetOffset())
		s.EqualValues(0, secondTask.BatchRows())

		s.Eventually(func() bool {
			_, ok := metacache.GetSegmentByID(1012)
			return !ok
		}, time.Second, 10*time.Millisecond)
		s.NotContains(l0wb.growingSourceProgress, int64(1012))
	})

	s.Run("sealed_pending_source_waits_without_fatal_and_retries_when_usable", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		sourceState := syncmgr.GrowingSourcePending
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, sourceState
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1005, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		err = wb.SealSegments(context.Background(), []int64{1005})
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		l0wb.EvictBuffer()
		segment, ok := metacache.GetSegmentByID(1005)
		s.True(ok)
		s.Equal(commonpb.SegmentState_Sealed, segment.State())
		s.Contains(l0wb.growingSourceProgress, int64(1005))

		sourceState = syncmgr.GrowingSourceUsable
		taskCh := make(chan *syncmgr.GrowingSourceSyncTask, 2)
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				taskCh <- textTask
				return conc.Go(func() (struct{}, error) {
					for _, callback := range callbacks {
						if err := callback(nil); err != nil {
							return struct{}{}, err
						}
					}
					return struct{}{}, nil
				}), nil
			}).Once()

		l0wb.retryGrowingSourceProgress()
		textTask := <-taskCh
		s.EqualValues(1005, textTask.SegmentID())
		s.True(textTask.IsFlush())
		s.EqualValues(10, textTask.BatchRows())

		s.Eventually(func() bool {
			_, ok := metacache.GetSegmentByID(1005)
			return !ok
		}, time.Second, 10*time.Millisecond)
		s.NotContains(l0wb.growingSourceProgress, int64(1005))
	})

	s.Run("dropped_text_progress_builds_drop_source_task", func() {
		textSchema := s.textSchema()
		metacache := s.newTextRealMetaCache(textSchema)
		resolveCalls := 0
		sourceState := syncmgr.GrowingSourcePending
		wb, err := NewL0WriteBuffer(s.channelName, metacache, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				resolveCalls++
				if resolveCalls == 1 {
					return fakeGrowingFlushSource{}, syncmgr.GrowingSourceUsable
				}
				return fakeGrowingFlushSource{}, sourceState
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1004, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)

		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		l0wb := wb.(*l0WriteBuffer)
		s.Contains(l0wb.growingSourceProgress, int64(1004))
		wb.DropPartitions([]int64{0})

		sourceState = syncmgr.GrowingSourceUsable
		taskCh := make(chan *syncmgr.GrowingSourceSyncTask, 1)
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				taskCh <- textTask
				return conc.Go(func() (struct{}, error) {
					for _, callback := range callbacks {
						if err := callback(nil); err != nil {
							return struct{}{}, err
						}
					}
					return struct{}{}, nil
				}), nil
			}).Once()

		futures := l0wb.syncSegments(context.Background(), []int64{1004})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))

		textTask := <-taskCh
		s.True(textTask.IsDrop())
		s.EqualValues(1004, textTask.SegmentID())
	})
}

func (s *L0WriteBufferSuite) TestGetGrowingFlushProgress() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)

	s.Run("synced_growing_source_segment_is_still_handoff_candidate_until_final_flush", func() {
		handoffProvider := &fakeGrowingSourceReleaseHandoffProvider{}
		registration := syncmgr.DefaultGrowingSourceRegistry().Register(s.channelName, handoffProvider)
		defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

		textSchema := s.textSchema()
		mc := s.newTextRealMetaCache(textSchema)
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		wb, err := NewL0WriteBuffer(s.channelName, mc, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
			metaWriter:  metaWriter,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask")).Return(nil).Once()
		done := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					defer close(done)
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					return struct{}{}, err
				}), nil
			}).Once()

		_, msg := s.composeTextInsertMsg(1200, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1200})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		<-done

		s.Contains(l0wb.growingSourceProgress, int64(1200))
		s.Empty(l0wb.growingSourceProgress[int64(1200)].batches)

		progress, err := wb.GetGrowingFlushProgress(context.Background(), []int64{1200}, 200)
		s.NoError(err)
		s.Len(progress, 1)
		s.EqualValues(1200, progress[0].SegmentID)
		s.True(progress[0].NeedReleaseHandoff)
		s.EqualValues(10, progress[0].TargetOffset)
		s.Equal(metacache.FlushSourceGrowing, progress[0].SourceMode)
		s.Equal([]syncmgr.GrowingSourceReleaseHandoffSegment{{SegmentID: 1200, TargetOffset: 10}}, handoffProvider.segments)
	})

	s.Run("dropped_growing_source_segment_is_still_handoff_candidate_until_progress_done", func() {
		handoffProvider := &fakeGrowingSourceReleaseHandoffProvider{}
		registration := syncmgr.DefaultGrowingSourceRegistry().Register(s.channelName, handoffProvider)
		defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

		textSchema := s.textSchema()
		mc := s.newTextRealMetaCache(textSchema)
		metaWriter := syncmgr.NewMockMetaWriter(s.T())
		wb, err := NewL0WriteBuffer(s.channelName, mc, s.syncMgr, &writeBufferOption{
			idAllocator: s.allocator,
			metaWriter:  metaWriter,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourceUsable
			},
		})
		s.NoError(err)

		metaWriter.EXPECT().UpdateGrowingSourceSync(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask")).Return(nil).Once()
		done := make(chan struct{})
		s.syncMgr.EXPECT().SyncData(mock.Anything, mock.AnythingOfType("*syncmgr.GrowingSourceSyncTask"), mock.Anything).
			RunAndReturn(func(ctx context.Context, task syncmgr.Task, callbacks ...func(error) error) (*conc.Future[struct{}], error) {
				textTask := task.(*syncmgr.GrowingSourceSyncTask)
				textTask.WithChunkManager(storage.NewLocalChunkManager(objectstorage.RootPath(s.T().TempDir())))
				return conc.Go(func() (struct{}, error) {
					defer close(done)
					err := textTask.Run(ctx)
					for _, callback := range callbacks {
						if cbErr := callback(err); cbErr != nil {
							return struct{}{}, cbErr
						}
					}
					return struct{}{}, err
				}), nil
			}).Once()

		_, msg := s.composeTextInsertMsg(1203, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)
		l0wb := wb.(*l0WriteBuffer)
		futures := l0wb.syncSegments(context.Background(), []int64{1203})
		s.Require().Len(futures, 1)
		s.NoError(conc.AwaitAll(futures...))
		<-done

		s.Contains(l0wb.growingSourceProgress, int64(1203))
		s.Empty(l0wb.growingSourceProgress[int64(1203)].batches)

		wb.DropPartitions([]int64{0})

		progress, err := wb.GetGrowingFlushProgress(context.Background(), []int64{1203}, 200)
		s.NoError(err)
		s.Len(progress, 1)
		s.EqualValues(1203, progress[0].SegmentID)
		s.True(progress[0].NeedReleaseHandoff)
		s.EqualValues(10, progress[0].TargetOffset)
		s.Equal(metacache.FlushSourceGrowing, progress[0].SourceMode)
		s.Equal([]syncmgr.GrowingSourceReleaseHandoffSegment{{SegmentID: 1203, TargetOffset: 10}}, handoffProvider.segments)
	})

	s.Run("waits_until_fence_processed_and_returns_growing_progress", func() {
		handoffProvider := &fakeGrowingSourceReleaseHandoffProvider{}
		registration := syncmgr.DefaultGrowingSourceRegistry().Register(s.channelName, handoffProvider)
		defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

		textSchema := s.textSchema()
		mc := s.newTextRealMetaCache(textSchema)
		wb, err := NewL0WriteBuffer(s.channelName, mc, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return fakeGrowingFlushSource{}, syncmgr.GrowingSourcePending
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1201, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		resultCh := make(chan []GrowingFlushSegmentProgress, 1)
		errCh := make(chan error, 1)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		go func() {
			progress, err := wb.GetGrowingFlushProgress(ctx, []int64{1201}, 300)
			if err != nil {
				errCh <- err
				return
			}
			resultCh <- progress
		}()

		select {
		case <-resultCh:
			s.Fail("progress query returned before fence was processed")
		case err := <-errCh:
			s.NoError(err)
		case <-time.After(30 * time.Millisecond):
		}

		wb.SetFlushTimestamp(300)

		select {
		case progress := <-resultCh:
			s.Len(progress, 1)
			s.EqualValues(1201, progress[0].SegmentID)
			s.True(progress[0].NeedReleaseHandoff)
			s.EqualValues(10, progress[0].TargetOffset)
			s.Equal(metacache.FlushSourceGrowing, progress[0].SourceMode)
			s.EqualValues(300, handoffProvider.fenceTs)
			s.Equal([]syncmgr.GrowingSourceReleaseHandoffSegment{{SegmentID: 1201, TargetOffset: 10}}, handoffProvider.segments)
		case err := <-errCh:
			s.NoError(err)
		case <-ctx.Done():
			s.Fail("progress query did not return after fence was processed", ctx.Err())
		}

		progress, err := wb.GetGrowingFlushProgress(context.Background(), nil, 300)
		s.NoError(err)
		s.Len(progress, 1)
		s.EqualValues(1201, progress[0].SegmentID)
		s.True(progress[0].NeedReleaseHandoff)
	})

	s.Run("writebuffer_backed_segment_has_no_growing_progress", func() {
		handoffProvider := &fakeGrowingSourceReleaseHandoffProvider{}
		registration := syncmgr.DefaultGrowingSourceRegistry().Register(s.channelName, handoffProvider)
		defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

		textSchema := s.textSchema()
		mc := s.newTextRealMetaCache(textSchema)
		wb, err := NewL0WriteBuffer(s.channelName, mc, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
			growingSourceResolver: func(segmentID int64, targetOffset int64, _ *msgpb.MsgPosition) (syncmgr.GrowingFlushSource, syncmgr.GrowingSourceState) {
				return nil, syncmgr.GrowingSourceUnavailable
			},
		})
		s.NoError(err)

		_, msg := s.composeTextInsertMsg(1202, 10)
		insertData, err := PrepareInsert(textSchema, s.pkSchema, []*msgstream.InsertMsg{msg})
		s.NoError(err)
		err = wb.BufferData(insertData, nil, &msgpb.MsgPosition{Timestamp: 100}, &msgpb.MsgPosition{Timestamp: 200}, 100)
		s.NoError(err)

		progress, err := wb.GetGrowingFlushProgress(context.Background(), []int64{1202}, 200)
		s.NoError(err)
		s.Len(progress, 1)
		s.EqualValues(1202, progress[0].SegmentID)
		s.False(progress[0].NeedReleaseHandoff)
		s.Zero(progress[0].TargetOffset)
		s.Equal(metacache.FlushSourceWriteBuffer, progress[0].SourceMode)
		s.Zero(handoffProvider.fenceTs)
		s.Empty(handoffProvider.segments)
	})

	s.Run("unknown_segment_has_no_release_handoff", func() {
		handoffProvider := &fakeGrowingSourceReleaseHandoffProvider{}
		registration := syncmgr.DefaultGrowingSourceRegistry().Register(s.channelName, handoffProvider)
		defer syncmgr.DefaultGrowingSourceRegistry().Unregister(registration)

		textSchema := s.textSchema()
		mc := s.newTextRealMetaCache(textSchema)
		wb, err := NewL0WriteBuffer(s.channelName, mc, s.syncMgr, &writeBufferOption{
			idAllocator:                s.allocator,
			growingSourceRetryInterval: time.Hour,
		})
		s.NoError(err)
		wb.SetFlushTimestamp(200)

		progress, err := wb.GetGrowingFlushProgress(context.Background(), []int64{1204}, 200)
		s.NoError(err)
		s.Len(progress, 1)
		s.EqualValues(1204, progress[0].SegmentID)
		s.False(progress[0].NeedReleaseHandoff)
		s.Zero(progress[0].TargetOffset)
		s.Equal(metacache.FlushSourceUnknown, progress[0].SourceMode)
		s.Zero(handoffProvider.fenceTs)
		s.Empty(handoffProvider.segments)
	})
}

func (s *L0WriteBufferSuite) TestCheckReleaseManualFlushNeed() {
	paramtable.Get().Save(paramtable.Get().CommonCfg.UseLoonFFI.Key, "true")
	paramtable.Get().Save(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key, "true")
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.UseLoonFFI.Key)
	defer paramtable.Get().Reset(paramtable.Get().CommonCfg.EnableGrowingSourceFlush.Key)

	textSchema := s.textSchema()
	mc := s.newTextRealMetaCache(textSchema)
	wb, err := NewL0WriteBuffer(s.channelName, mc, s.syncMgr, &writeBufferOption{
		idAllocator:                s.allocator,
		growingSourceRetryInterval: time.Hour,
	})
	s.NoError(err)

	checker := wb.(interface {
		CheckReleaseManualFlushNeed(segmentIDs []int64) bool
	})
	s.False(checker.CheckReleaseManualFlushNeed(nil))
	s.True(checker.CheckReleaseManualFlushNeed([]int64{1300}))

	wb.CreateNewGrowingSegment(0, 1301, &msgpb.MsgPosition{Timestamp: 100}, 100)
	mc.UpdateSegments(metacache.SetFlushSourceMode(metacache.FlushSourceWriteBuffer), metacache.WithSegmentIDs(1301))
	s.False(checker.CheckReleaseManualFlushNeed([]int64{1301}))

	wb.CreateNewGrowingSegment(0, 1302, &msgpb.MsgPosition{Timestamp: 100}, 100)
	mc.UpdateSegments(metacache.SetFlushSourceMode(metacache.FlushSourceGrowing), metacache.WithSegmentIDs(1302))
	s.True(checker.CheckReleaseManualFlushNeed([]int64{1302}))

	mc.UpdateSegments(metacache.UpdateState(commonpb.SegmentState_Flushed), metacache.WithSegmentIDs(1302))
	s.False(checker.CheckReleaseManualFlushNeed([]int64{1302}))
	s.True(checker.CheckReleaseManualFlushNeed([]int64{1301, 1300}))

	wb.CreateNewGrowingSegment(0, 1303, &msgpb.MsgPosition{Timestamp: 100}, 100)
	mc.UpdateSegments(metacache.SetFlushSourceMode(metacache.FlushSourceGrowing), metacache.WithSegmentIDs(1303))
	s.True(checker.CheckReleaseManualFlushNeed([]int64{1300, 1303}))
}

func (s *L0WriteBufferSuite) TestGrowingSourceRegistryResolveMultipleProviders() {
	registry := syncmgr.NewGrowingSourceRegistry()
	channel := "by-dev-rootcoord-dml_1v0"

	pendingReg := registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 8},
		state:  syncmgr.GrowingSourcePending,
	})
	pendingReg2 := registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 9},
		state:  syncmgr.GrowingSourcePending,
	})
	registry.Register(channel, fakeGrowingSourceProvider{state: syncmgr.GrowingSourceUnavailable})

	resolved, state := registry.Resolve(channel, 1000, 10, &msgpb.MsgPosition{Timestamp: 200})
	s.Equal(syncmgr.GrowingSourcePending, state)
	s.Nil(resolved)

	usableReg := registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 10},
		state:  syncmgr.GrowingSourceUsable,
	})
	usableReg2 := registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 12},
		state:  syncmgr.GrowingSourceUsable,
	})
	resolved, state = registry.Resolve(channel, 1000, 10, &msgpb.MsgPosition{Timestamp: 200})
	s.Equal(syncmgr.GrowingSourceUsable, state)
	s.NotNil(resolved)
	s.EqualValues(10, resolved.CurrentOffset())

	registry.Unregister(pendingReg)
	registry.Unregister(pendingReg2)
	resolved, state = registry.Resolve(channel, 1000, 10, &msgpb.MsgPosition{Timestamp: 200})
	s.Equal(syncmgr.GrowingSourceUsable, state)
	s.NotNil(resolved)
	s.EqualValues(10, resolved.CurrentOffset())

	registry.Unregister(usableReg2)
	resolved, state = registry.Resolve(channel, 1000, 10, &msgpb.MsgPosition{Timestamp: 200})
	s.Equal(syncmgr.GrowingSourceUsable, state)
	s.NotNil(resolved)
	s.EqualValues(10, resolved.CurrentOffset())

	registry.Unregister(usableReg)
	resolved, state = registry.Resolve(channel, 1000, 10, &msgpb.MsgPosition{Timestamp: 200})
	s.Equal(syncmgr.GrowingSourceUnavailable, state)
	s.Nil(resolved)
}

func (s *L0WriteBufferSuite) TestGrowingSourceRegistryResolveReleasesUnselectedSources() {
	registry := syncmgr.NewGrowingSourceRegistry()
	channel := "by-dev-rootcoord-dml_1v0"
	released := 0

	registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 10, releaseFunc: func() { released++ }},
		state:  syncmgr.GrowingSourceUsable,
	})
	registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 12, releaseFunc: func() { released++ }},
		state:  syncmgr.GrowingSourceUsable,
	})
	registry.Register(channel, fakeGrowingSourceProvider{
		source: fakeGrowingFlushSource{currentOffset: 11, releaseFunc: func() { released++ }},
		state:  syncmgr.GrowingSourcePending,
	})

	resolved, state := registry.Resolve(channel, 1000, 10, &msgpb.MsgPosition{Timestamp: 200})
	s.Equal(syncmgr.GrowingSourceUsable, state)
	s.NotNil(resolved)
	s.EqualValues(10, resolved.CurrentOffset())
	s.Equal(0, released)
}

func TestL0WriteBuffer(t *testing.T) {
	suite.Run(t, new(L0WriteBufferSuite))
}
