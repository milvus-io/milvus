package datacoord

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/util/lock"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	collID         = UniqueID(100)
	partID         = UniqueID(200)
	indexID        = UniqueID(300)
	fieldID        = UniqueID(400)
	indexName      = "_default_idx"
	segID          = UniqueID(500)
	buildID        = UniqueID(600)
	nodeID         = UniqueID(700)
	partitionKeyID = UniqueID(800)
	statsTaskID    = UniqueID(900)
)

func createIndexMeta(catalog metastore.DataCoordCatalog) *indexMeta {
	indexBuildInfo := newSegmentIndexBuildInfo()
	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 2,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 2,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 3,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 4,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 4,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 5,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 6,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 6,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 7,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 7,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "error",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 8,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 8,
		NodeID:              nodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 9,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 9,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})

	indexBuildInfo.Add(&model.SegmentIndex{
		SegmentID:           segID + 10,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 10,
		NodeID:              nodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes := typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]]()
	segIdx0 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx0.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1025,
		IndexID:             indexID,
		BuildID:             buildID,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      0,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx1 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx1.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 1,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 1,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx2 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx2.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 2,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 2,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           true,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx3 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx3.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 3,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 3,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx4 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx4.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 4,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 4,
		NodeID:              nodeID,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx5 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx5.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 5,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 5,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx6 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx6.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 6,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 6,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Finished,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx7 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx7.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 7,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 7,
		NodeID:              0,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_Failed,
		FailReason:          "error",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx8 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx8.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 8,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             1026,
		IndexID:             indexID,
		BuildID:             buildID + 8,
		NodeID:              nodeID + 1,
		IndexVersion:        1,
		IndexState:          commonpb.IndexState_InProgress,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx9 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx9.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 9,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 9,
		NodeID:              0,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIdx10 := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	segIdx10.Insert(indexID, &model.SegmentIndex{
		SegmentID:           segID + 10,
		CollectionID:        collID,
		PartitionID:         partID,
		NumRows:             500,
		IndexID:             indexID,
		BuildID:             buildID + 10,
		NodeID:              nodeID,
		IndexVersion:        0,
		IndexState:          commonpb.IndexState_Unissued,
		FailReason:          "",
		IsDeleted:           false,
		CreatedUTCTime:      1111,
		IndexFileKeys:       nil,
		IndexSerializedSize: 1,
	})
	segIndexes.Insert(segID, segIdx0)
	segIndexes.Insert(segID+1, segIdx1)
	segIndexes.Insert(segID+2, segIdx2)
	segIndexes.Insert(segID+3, segIdx3)
	segIndexes.Insert(segID+4, segIdx4)
	segIndexes.Insert(segID+5, segIdx5)
	segIndexes.Insert(segID+6, segIdx6)
	segIndexes.Insert(segID+7, segIdx7)
	segIndexes.Insert(segID+8, segIdx8)
	segIndexes.Insert(segID+9, segIdx9)
	segIndexes.Insert(segID+10, segIdx10)

	return &indexMeta{
		catalog: catalog,
		keyLock: lock.NewKeyLock[UniqueID](),
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    false,
					CreateTime:   1,
					TypeParams: []*commonpb.KeyValuePair{
						{
							Key:   common.DimKey,
							Value: "128",
						},
					},
					IndexParams: []*commonpb.KeyValuePair{
						{
							Key:   common.IndexTypeKey,
							Value: "HNSW",
						},
						{
							Key:   common.MetricTypeKey,
							Value: "L2",
						},
					},
				},
			},
		},
		segmentIndexes:   segIndexes,
		segmentBuildInfo: indexBuildInfo,
	}
}

type testMetaOption func(*meta)

func withAnalyzeMeta(am *analyzeMeta) testMetaOption {
	return func(mt *meta) {
		mt.analyzeMeta = am
	}
}

func withIndexMeta(im *indexMeta) testMetaOption {
	return func(mt *meta) {
		mt.indexMeta = im
	}
}

func withStatsTaskMeta(stm *statsTaskMeta) testMetaOption {
	return func(mt *meta) {
		mt.statsTaskMeta = stm
	}
}

func createMeta(catalog metastore.DataCoordCatalog, opts ...testMetaOption) *meta {
	mt := &meta{
		catalog:     catalog,
		collections: typeutil.NewConcurrentMap[UniqueID, *collectionInfo](),
		segments: &SegmentsInfo{
			segments: map[UniqueID]*SegmentInfo{
				1000: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1000,
						CollectionID: 10000,
						PartitionID:  10001,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: 10002, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				1001: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1001,
						CollectionID: 10000,
						PartitionID:  10001,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: 10002, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				1002: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:           1002,
						CollectionID: 10000,
						PartitionID:  10001,
						NumOfRows:    3000,
						State:        commonpb.SegmentState_Flushed,
						Binlogs:      []*datapb.FieldBinlog{{FieldID: 10002, Binlogs: []*datapb.Binlog{{LogID: 1}, {LogID: 2}, {LogID: 3}}}},
					},
				},
				segID: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1025,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 1: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 1,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 2: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 2,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 3: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 3,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 4: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 4,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 5: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 5,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 6: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 6,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 7: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 7,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 8: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 8,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      1026,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 9: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 9,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
				segID + 10: {
					SegmentInfo: &datapb.SegmentInfo{
						ID:             segID + 10,
						CollectionID:   collID,
						PartitionID:    partID,
						InsertChannel:  "",
						NumOfRows:      500,
						State:          commonpb.SegmentState_Flushed,
						MaxRowNum:      65536,
						LastExpireTime: 10,
					},
				},
			},
		},
	}

	for _, opt := range opts {
		opt(mt)
	}
	return mt
}

func createIndexMetaWithSegment(catalog metastore.DataCoordCatalog,
	collID, partID, segID, indexID, fieldID, buildID UniqueID,
) *indexMeta {
	tasks := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
	indexTask := &model.SegmentIndex{
		CollectionID: collID,
		PartitionID:  partID,
		SegmentID:    segID,
		IndexID:      indexID,
		BuildID:      buildID,
		IndexState:   commonpb.IndexState_Unissued,
		NodeID:       0,
		FailReason:   "",
		NumRows:      1025,
	}
	tasks.Insert(indexID, indexTask)
	im := &indexMeta{
		catalog: catalog,
		keyLock: lock.NewKeyLock[UniqueID](),
		indexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {indexID: {
				IndexID:   indexID,
				FieldID:   fieldID,
				IndexName: "default_index",
				IndexParams: []*commonpb.KeyValuePair{
					{Key: "index_type", Value: "HNSW"},
				},
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "128"},
				},
			}},
		},
		segmentIndexes:   typeutil.NewConcurrentMap[UniqueID, *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]](),
		segmentBuildInfo: newSegmentIndexBuildInfo(),
	}
	im.segmentIndexes.Insert(segID, tasks)
	im.segmentBuildInfo.Add(indexTask)
	return im
}
