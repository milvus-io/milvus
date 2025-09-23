package compactor

import (
	"context"
	"fmt"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/compaction"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache"
	"github.com/milvus-io/milvus/internal/flushcommon/metacache/pkoracle"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/mocks/flushcommon/mock_util"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/objectstorage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type NamespaceCompactorTestSuite struct {
	suite.Suite
	binlogIO       *mock_util.MockBinlogIO
	schema         *schemapb.CollectionSchema
	sortedSegments []*datapb.CompactionSegmentBinlogs
}

func (s *NamespaceCompactorTestSuite) SetupSuite() {
	paramtable.Get().Init(paramtable.NewBaseTable())
	paramtable.Get().Save("common.storageType", "local")
	paramtable.Get().Save("common.storage.enableV2", "true")
	initcore.InitStorageV2FileSystem(paramtable.Get())

	s.binlogIO = mock_util.NewMockBinlogIO(s.T())
	s.binlogIO.EXPECT().Upload(mock.Anything, mock.Anything).Return(nil)
	s.schema = &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     "row_id",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     "timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      100,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "namespace",
				DataType: schemapb.DataType_Int64,
			},
		},
	}
	s.setupSortedSegments()
}

func (s *NamespaceCompactorTestSuite) setupSortedSegments() {
	num := 3
	rows := 10000
	collectionID := int64(100)
	partitionID := int64(100)
	alloc := allocator.NewLocalAllocator(0, math.MaxInt64)

	for i := 0; i < num; i++ {
		data, err := storage.NewInsertData(s.schema)
		s.Require().NoError(err)
		for j := 0; j < rows; j++ {
			v := map[int64]interface{}{
				common.RowIDField:     int64(j),
				common.TimeStampField: int64(tsoutil.ComposeTSByTime(getMilvusBirthday(), 0)),
				100:                   int64(j),
				101:                   int64(j),
			}
			data.Append(v)
		}
		pack := new(syncmgr.SyncPack).WithCollectionID(collectionID).WithPartitionID(partitionID).WithSegmentID(int64(i)).WithChannelName(fmt.Sprintf("by-dev-rootcoord-dml_0_%dv0", collectionID)).WithInsertData([]*storage.InsertData{data})
		rootPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
		cm := storage.NewLocalChunkManager(objectstorage.RootPath(rootPath))
		bfs := pkoracle.NewBloomFilterSet()
		seg := metacache.NewSegmentInfo(&datapb.SegmentInfo{}, bfs, nil)
		metacache.UpdateNumOfRows(int64(rows))(seg)
		mc := metacache.NewMockMetaCache(s.T())
		mc.EXPECT().Collection().Return(collectionID).Maybe()
		mc.EXPECT().GetSchema(mock.Anything).Return(s.schema).Maybe()
		mc.EXPECT().GetSegmentByID(int64(i)).Return(seg, true).Maybe()
		mc.EXPECT().GetSegmentsBy(mock.Anything, mock.Anything).Return([]*metacache.SegmentInfo{seg}).Maybe()
		mc.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Run(func(action metacache.SegmentAction, filters ...metacache.SegmentFilter) {
			action(seg)
		}).Return().Maybe()
		fields := typeutil.GetAllFieldSchemas(s.schema)
		columnGroups := storagecommon.SplitColumns(fields, map[int64]storagecommon.ColumnStats{}, storagecommon.NewSelectedDataTypePolicy(), storagecommon.NewRemanentShortPolicy(-1))
		bw := syncmgr.NewBulkPackWriterV2(mc, s.schema, cm, alloc, packed.DefaultWriteBufferSize, 0, &indexpb.StorageConfig{
			StorageType: "local",
			RootPath:    rootPath,
		}, columnGroups)
		inserts, _, _, _, _, err := bw.Write(context.Background(), pack)
		s.Require().NoError(err)
		s.sortedSegments = append(s.sortedSegments, &datapb.CompactionSegmentBinlogs{
			SegmentID:      int64(i),
			FieldBinlogs:   storage.SortFieldBinlogs(inserts),
			Deltalogs:      []*datapb.FieldBinlog{},
			StorageVersion: storage.StorageV2,
			IsSorted:       true,
		})
	}
}

func (s *NamespaceCompactorTestSuite) TestCompactSorted() {
	plan := &datapb.CompactionPlan{
		SegmentBinlogs: s.sortedSegments,
		Schema:         s.schema,
		PreAllocatedSegmentIDs: &datapb.IDRange{
			Begin: 0,
			End:   math.MaxInt64,
		},
		PreAllocatedLogIDs: &datapb.IDRange{
			Begin: 0,
			End:   math.MaxInt64,
		},
		MaxSize: 1024 * 1024 * 1024,
	}
	params := compaction.GenParams()
	sortedByFieldIDs := []int64{101, 100}

	c := NewNamespaceCompactor(context.Background(), plan, s.binlogIO, params, sortedByFieldIDs)
	result, err := c.Compact()

	s.Require().NoError(err)
	s.Require().Equal(datapb.CompactionTaskState_completed, result.State)
	for _, segment := range result.GetSegments() {
		s.assertSorted(segment, sortedByFieldIDs)
	}
}

func (s *NamespaceCompactorTestSuite) assertSorted(segment *datapb.CompactionSegment, sortedByFieldIDs []int64) {
	reader, err := storage.NewBinlogRecordReader(context.Background(), segment.GetInsertLogs(), s.schema, storage.WithVersion(segment.GetStorageVersion()), storage.WithStorageConfig(&indexpb.StorageConfig{
		StorageType: "local",
		RootPath:    paramtable.Get().LocalStorageCfg.Path.GetValue(),
	}))
	s.Require().NoError(err)
	records := make([]storage.Record, 0)
	for {
		record, err := reader.Next()
		if err != nil {
			break
		}
		record.Retain()
		records = append(records, record)
	}

	cmps := make([]func(ri, i, rj, j int) int, 0)
	for _, fieldID := range sortedByFieldIDs {
		field := typeutil.GetField(s.schema, fieldID)
		switch field.DataType {
		case schemapb.DataType_Int64:
			cmps = append(cmps, func(ri, i, rj, j int) int {
				vi := records[ri].Column(fieldID).(*array.Int64).Value(i)
				vj := records[rj].Column(fieldID).(*array.Int64).Value(j)
				if vi < vj {
					return -1
				}
				if vi > vj {
					return 1
				}
				return 0
			})
		case schemapb.DataType_String:
			cmps = append(cmps, func(ri, i, rj, j int) int {
				vi := records[ri].Column(fieldID).(*array.String).Value(i)
				vj := records[rj].Column(fieldID).(*array.String).Value(j)
				if vi < vj {
					return -1
				}
				if vi > vj {
					return 1
				}
				return 0
			})
		default:
			panic("unsupported data type")
		}
	}

	prevri := -1
	previ := -1

	for ri := 0; ri < len(records); ri++ {
		for i := 0; i < records[ri].Len(); i++ {
			if prevri == -1 {
				prevri = ri
				previ = i
				continue
			}
			for _, cmp := range cmps {
				c := cmp(prevri, previ, ri, i)
				s.Require().True(c <= 0, "not sorted")
				if c < 0 {
					break
				}
			}
			prevri = ri
			previ = i
		}
	}
}

func TestNamespaceCompactorTestSuite(t *testing.T) {
	suite.Run(t, new(NamespaceCompactorTestSuite))
}
