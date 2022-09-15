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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/metastore/kv/indexcoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
)

func TestGarbageCollector_Start(t *testing.T) {
	gc := newGarbageCollector(context.Background(), &metaTable{}, &chunkManagerMock{
		removeWithPrefix: func(s string) error {
			return nil
		},
		listWithPrefix: func(s string, recursive bool) ([]string, []time.Time, error) {
			return []string{}, []time.Time{}, nil
		},
		remove: func(s string) error {
			return nil
		},
	}, &IndexCoord{})

	gc.gcMetaDuration = time.Millisecond * 300
	gc.gcFileDuration = time.Millisecond * 300

	gc.Start()
	gc.Stop()
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
