// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package indexcoord

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/api/commonpb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/proto/datapb"
)

func createGarbageCollectorMetaTable(catalog metastore.IndexCoordCatalog) *metaTable {
	return &metaTable{
		catalog:          catalog,
		indexLock:        sync.RWMutex{},
		segmentIndexLock: sync.RWMutex{},
		collectionIndexes: map[UniqueID]map[UniqueID]*model.Index{
			collID: {
				indexID: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID,
					IndexID:      indexID,
					IndexName:    indexName,
					IsDeleted:    true,
					CreateTime:   1,
					TypeParams:   nil,
					IndexParams:  nil,
				},
				indexID + 1: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID + 1,
					IndexID:      indexID + 1,
					IndexName:    "indexName2",
					IsDeleted:    true,
					CreateTime:   0,
					TypeParams:   nil,
					IndexParams:  nil,
				},
				indexID + 2: {
					TenantID:     "",
					CollectionID: collID,
					FieldID:      fieldID + 2,
					IndexID:      indexID + 2,
					IndexName:    "indexName3",
					IsDeleted:    false,
					CreateTime:   0,
					TypeParams:   nil,
					IndexParams:  nil,
				},
			},
		},
		segmentIndexes: map[UniqueID]map[UniqueID]*model.SegmentIndex{
			segID: {
				indexID: {
					SegmentID:      segID,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1025,
					IndexID:        indexID,
					BuildID:        buildID,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     3,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1,
					IndexFilePaths: nil,
					IndexSize:      100,
					WriteHandoff:   false,
				},
			},
			segID + 1: {
				indexID: {
					SegmentID:      segID + 1,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        1025,
					IndexID:        indexID,
					BuildID:        buildID + 1,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     2,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1,
					IndexFilePaths: nil,
					IndexSize:      100,
					WriteHandoff:   false,
				},
			},
			segID + 2: {
				indexID + 2: {
					SegmentID:      segID + 2,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        10000,
					IndexID:        indexID + 2,
					BuildID:        buildID + 2,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     1,
					FailReason:     "",
					IsDeleted:      true,
					CreateTime:     1,
					IndexFilePaths: nil,
					IndexSize:      0,
					WriteHandoff:   false,
				},
			},
			segID + 3: {
				indexID + 2: {
					SegmentID:      segID + 3,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        10000,
					IndexID:        indexID + 2,
					BuildID:        buildID + 3,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     1,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1,
					IndexFilePaths: []string{"file1", "file2"},
					IndexSize:      0,
					WriteHandoff:   false,
				},
			},
			segID + 4: {
				indexID + 2: {
					SegmentID:      segID + 4,
					CollectionID:   collID,
					PartitionID:    partID,
					NumRows:        10000,
					IndexID:        indexID + 2,
					BuildID:        buildID + 4,
					NodeID:         0,
					IndexVersion:   1,
					IndexState:     2,
					FailReason:     "",
					IsDeleted:      false,
					CreateTime:     1,
					IndexFilePaths: []string{},
					IndexSize:      0,
					WriteHandoff:   false,
				},
			},
		},
		buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
			buildID: {
				SegmentID:      segID,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1025,
				IndexID:        indexID,
				BuildID:        buildID,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     3,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1,
				IndexFilePaths: nil,
				IndexSize:      100,
				WriteHandoff:   false,
			},
			buildID + 1: {
				SegmentID:      segID + 1,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        1025,
				IndexID:        indexID,
				BuildID:        buildID + 1,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     2,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1,
				IndexFilePaths: nil,
				IndexSize:      100,
				WriteHandoff:   false,
			},
			buildID + 2: {
				SegmentID:      segID + 2,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        10000,
				IndexID:        indexID + 2,
				BuildID:        buildID + 2,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     1,
				FailReason:     "",
				IsDeleted:      true,
				CreateTime:     1,
				IndexFilePaths: nil,
				IndexSize:      0,
				WriteHandoff:   false,
			},
			buildID + 3: {
				SegmentID:      segID + 3,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        10000,
				IndexID:        indexID + 2,
				BuildID:        buildID + 3,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     3,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1,
				IndexFilePaths: []string{"file1", "file2"},
				IndexSize:      0,
				WriteHandoff:   false,
			},
			buildID + 4: {
				SegmentID:      segID + 4,
				CollectionID:   collID,
				PartitionID:    partID,
				NumRows:        10000,
				IndexID:        indexID + 2,
				BuildID:        buildID + 4,
				NodeID:         0,
				IndexVersion:   1,
				IndexState:     2,
				FailReason:     "",
				IsDeleted:      false,
				CreateTime:     1,
				IndexFilePaths: []string{},
				IndexSize:      0,
				WriteHandoff:   false,
			},
		},
	}
}

func TestGarbageCollector(t *testing.T) {
	meta := createGarbageCollectorMetaTable(&indexcoord.Catalog{Txn: NewMockEtcdKV()})
	gc := newGarbageCollector(context.Background(), meta, &chunkManagerMock{
		removeWithPrefix: func(s string) error {
			return nil
		},
		listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
			return []string{strconv.FormatInt(buildID, 10), strconv.FormatInt(buildID+1, 10),
				strconv.FormatInt(buildID+3, 10), strconv.FormatInt(buildID+4, 10)}, []time.Time{}, nil
		},
		remove: func(s string) error {
			return nil
		},
	}, &IndexCoord{
		dataCoordClient: &DataCoordMock{
			CallGetFlushedSegment: func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
				return &datapb.GetFlushedSegmentsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
					},
					Segments: []int64{segID, segID + 1, segID + 2, segID + 3, segID + 4},
				}, nil
			},
		},
	})

	gc.gcMetaDuration = time.Millisecond * 300
	gc.gcFileDuration = time.Millisecond * 300

	gc.Start()
	time.Sleep(time.Second * 2)
	err := gc.metaTable.MarkSegmentsIndexAsDeletedByBuildID([]UniqueID{buildID + 3, buildID + 4})
	assert.NoError(t, err)
	segIndexes := gc.metaTable.GetAllSegIndexes()
	for len(segIndexes) != 0 {
		time.Sleep(time.Second)
		segIndexes = gc.metaTable.GetAllSegIndexes()
	}
	gc.Stop()
}

func TestGarbageCollector_error(t *testing.T) {
	meta := createGarbageCollectorMetaTable(&indexcoord.Catalog{
		Txn: &mockETCDKV{
			multiSave: func(m map[string]string) error {
				return errors.New("error")
			},
			remove: func(s string) error {
				return errors.New("error")
			},
		},
	})
	gc := newGarbageCollector(context.Background(), meta, &chunkManagerMock{
		removeWithPrefix: func(s string) error {
			return errors.New("error")
		},
		listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
			return nil, nil, errors.New("error")
		},
		remove: func(s string) error {
			return errors.New("error")
		},
	}, &IndexCoord{
		dataCoordClient: &DataCoordMock{
			CallGetFlushedSegment: func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
				return &datapb.GetFlushedSegmentsResponse{
					Status: &commonpb.Status{
						ErrorCode: commonpb.ErrorCode_Success,
					},
					Segments: []int64{segID, segID + 1, segID + 2, segID + 3, segID + 4},
				}, nil
			},
		},
	})

	gc.gcMetaDuration = time.Millisecond * 300
	gc.gcFileDuration = time.Millisecond * 300

	gc.Start()
	time.Sleep(time.Second * 3)
	gc.Stop()
}

func TestGarbageCollectorGetFlushedSegment_error(t *testing.T) {
	t.Run("error", func(t *testing.T) {
		meta := createGarbageCollectorMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
				remove: func(s string) error {
					return errors.New("error")
				},
			},
		})
		gc := newGarbageCollector(context.Background(), meta, &chunkManagerMock{
			removeWithPrefix: func(s string) error {
				return errors.New("error")
			},
			listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
				return nil, nil, errors.New("error")
			},
			remove: func(s string) error {
				return errors.New("error")
			},
		}, &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetFlushedSegment: func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
					return &datapb.GetFlushedSegmentsResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_Success,
						},
						Segments: []int64{},
					}, errors.New("error")
				},
			},
		})

		gc.recycleSegIndexesMeta()
	})

	t.Run("fail", func(t *testing.T) {
		meta := createGarbageCollectorMetaTable(&indexcoord.Catalog{
			Txn: &mockETCDKV{
				multiSave: func(m map[string]string) error {
					return errors.New("error")
				},
				remove: func(s string) error {
					return errors.New("error")
				},
			},
		})
		gc := newGarbageCollector(context.Background(), meta, &chunkManagerMock{
			removeWithPrefix: func(s string) error {
				return errors.New("error")
			},
			listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
				return nil, nil, errors.New("error")
			},
			remove: func(s string) error {
				return errors.New("error")
			},
		}, &IndexCoord{
			dataCoordClient: &DataCoordMock{
				CallGetFlushedSegment: func(ctx context.Context, req *datapb.GetFlushedSegmentsRequest) (*datapb.GetFlushedSegmentsResponse, error) {
					return &datapb.GetFlushedSegmentsResponse{
						Status: &commonpb.Status{
							ErrorCode: commonpb.ErrorCode_UnexpectedError,
							Reason:    "fail reason",
						},
						Segments: []int64{},
					}, nil
				},
			},
		})

		gc.recycleSegIndexesMeta()
	})

}

func TestGarbageCollector_recycleUnusedIndexFiles(t *testing.T) {
	t.Run("index not in meta and remove with prefix failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			}),
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return fmt.Errorf("error")
				},
				listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
					if !recursive {
						return []string{"a/b/1/", "a/b/2/"}, []time.Time{{}, {}}, nil
					}
					return []string{"a/b/1/c", "a/b/2/d"}, []time.Time{{}, {}}, nil
				},
				remove: func(s string) error {
					return nil
				},
			},
		}

		gc.wg.Add(1)
		go gc.recycleUnusedIndexFiles()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})

	t.Run("load dir failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			}),
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
					if !recursive {
						return nil, nil, fmt.Errorf("error")
					}
					return []string{"a/b/1/c", "a/b/2/d"}, []time.Time{{}, {}}, nil
				},
				remove: func(s string) error {
					return nil
				},
			},
		}

		gc.wg.Add(1)
		go gc.recycleUnusedIndexFiles()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})

	t.Run("parse failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			}),
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
					if !recursive {
						return []string{"a/b/c/"}, []time.Time{{}}, nil
					}
					return []string{"a/b/1/c", "a/b/2/d"}, []time.Time{{}, {}}, nil
				},
				remove: func(s string) error {
					return nil
				},
			},
		}

		gc.wg.Add(1)
		go gc.recycleUnusedIndexFiles()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})

	t.Run("ListWithPrefix failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			}),
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
					if !recursive {
						return []string{"a/b/1/"}, []time.Time{{}}, nil
					}
					return nil, nil, fmt.Errorf("error")
				},
				remove: func(s string) error {
					return nil
				},
			},
		}

		gc.wg.Add(1)
		go gc.recycleUnusedIndexFiles()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})

	t.Run("remove failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			}),
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
					if !recursive {
						return []string{"a/b/1/"}, []time.Time{{}}, nil
					}
					return []string{"a/b/1/c"}, []time.Time{{}}, nil
				},
				remove: func(s string) error {
					return fmt.Errorf("error")
				},
			},
		}

		gc.wg.Add(1)
		go gc.recycleUnusedIndexFiles()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})

	t.Run("meta mark deleted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			}),
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
					if !recursive {
						return []string{"a/b/1/"}, []time.Time{{}}, nil
					}
					return []string{"a/b/1/c"}, []time.Time{{}}, nil
				},
				remove: func(s string) error {
					return fmt.Errorf("error")
				},
			},
		}

		gc.wg.Add(1)
		go gc.recycleUnusedIndexFiles()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})
}

func TestIndexCoord_recycleUnusedMetaLoop(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: constructMetaTable(&indexcoord.Catalog{
				Txn: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
					multiSave: func(m map[string]string) error {
						return nil
					},
				},
			}),
			indexCoordClient: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
		}
		gc.wg.Add(1)
		go gc.recycleUnusedSegIndexes()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})

	t.Run("remove meta failed", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		gc := &garbageCollector{
			ctx:            ctx,
			cancel:         cancel,
			wg:             sync.WaitGroup{},
			gcFileDuration: time.Millisecond * 300,
			gcMetaDuration: time.Millisecond * 300,
			metaTable: &metaTable{
				buildID2SegmentIndex: map[UniqueID]*model.SegmentIndex{
					1: {
						SegmentID:      0,
						CollectionID:   0,
						PartitionID:    0,
						NumRows:        0,
						IndexID:        0,
						BuildID:        0,
						NodeID:         0,
						IndexVersion:   0,
						IndexState:     0,
						FailReason:     "",
						IsDeleted:      true,
						CreateTime:     0,
						IndexFilePaths: nil,
						IndexSize:      0,
					},
				},
				catalog: &indexcoord.Catalog{
					Txn: &mockETCDKV{
						remove: func(s string) error {
							return fmt.Errorf("error")
						},
					},
				},
			},
			indexCoordClient: &IndexCoord{
				dataCoordClient: NewDataCoordMock(),
			},
		}
		gc.wg.Add(1)
		go gc.recycleUnusedSegIndexes()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})
}

func TestGarbageCollector_Recycle(t *testing.T) {
	ctx, canel := context.WithCancel(context.Background())
	gc := &garbageCollector{
		ctx:            ctx,
		cancel:         canel,
		wg:             sync.WaitGroup{},
		gcFileDuration: 300 * time.Millisecond,
		gcMetaDuration: 300 * time.Millisecond,
		metaTable:      createMetaTable(&indexcoord.Catalog{Txn: NewMockEtcdKV()}),
		chunkManager: &chunkManagerMock{
			listWithPrefix: func(s string, b bool) ([]string, []time.Time, error) {
				return nil, nil, nil
			},
		},
		indexCoordClient: &IndexCoord{
			dataCoordClient: NewDataCoordMock(),
		},
	}

	gc.Start()
	time.Sleep(time.Second)
	err := gc.metaTable.MarkIndexAsDeleted(collID, []int64{indexID})
	assert.NoError(t, err)
	time.Sleep(time.Second * 10)
	gc.Stop()
}
