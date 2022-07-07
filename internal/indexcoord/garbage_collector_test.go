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

	"github.com/milvus-io/milvus/internal/proto/commonpb"
	"github.com/milvus-io/milvus/internal/proto/indexpb"
)

func TestGarbageCollector_Start(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	gc := &garbageCollector{
		ctx:            ctx,
		cancel:         cancel,
		wg:             sync.WaitGroup{},
		gcFileDuration: time.Millisecond * 300,
		gcMetaDuration: time.Millisecond * 300,
		metaTable:      &metaTable{},
		chunkManager: &chunkManagerMock{
			removeWithPrefix: func(s string) error {
				return nil
			},
			listWithPrefix: func(s string, recursive bool) ([]string, error) {
				return []string{}, nil
			},
			remove: func(s string) error {
				return nil
			},
		},
	}

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
			metaTable: &metaTable{
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID:   1,
							IndexFilePaths: []string{"file1", "file2", "file3"},
							State:          commonpb.IndexState_Finished,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			},
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return fmt.Errorf("error")
				},
				listWithPrefix: func(s string, recursive bool) ([]string, error) {
					if !recursive {
						return []string{"a/b/1/", "a/b/2/"}, nil
					}
					return []string{"a/b/1/c", "a/b/2/d"}, nil
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
			metaTable: &metaTable{
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID:   1,
							IndexFilePaths: []string{"file1", "file2", "file3"},
							State:          commonpb.IndexState_Finished,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			},
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, error) {
					if !recursive {
						return nil, fmt.Errorf("error")
					}
					return []string{"a/b/1/c", "a/b/2/d"}, nil
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
			metaTable: &metaTable{
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID:   1,
							IndexFilePaths: []string{"file1", "file2", "file3"},
							State:          commonpb.IndexState_Finished,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			},
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, error) {
					if !recursive {
						return []string{"a/b/c/"}, nil
					}
					return []string{"a/b/1/c", "a/b/2/d"}, nil
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
			metaTable: &metaTable{
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID:   1,
							IndexFilePaths: []string{"file1", "file2", "file3"},
							State:          commonpb.IndexState_Finished,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			},
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, error) {
					if !recursive {
						return []string{"a/b/1/"}, nil
					}
					return nil, fmt.Errorf("error")
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
			metaTable: &metaTable{
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID:   1,
							IndexFilePaths: []string{"file1", "file2", "file3"},
							State:          commonpb.IndexState_Finished,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			},
			chunkManager: &chunkManagerMock{
				removeWithPrefix: func(s string) error {
					return nil
				},
				listWithPrefix: func(s string, recursive bool) ([]string, error) {
					if !recursive {
						return []string{"a/b/1/"}, nil
					}
					return []string{"a/b/1/c"}, nil
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
			metaTable: &metaTable{
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID: 1,
							MarkDeleted:  true,
							NodeID:       0,
						},
					},
					2: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID: 2,
							MarkDeleted:  false,
							State:        commonpb.IndexState_Finished,
							NodeID:       0,
						},
					},
					3: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID: 3,
							MarkDeleted:  false,
							State:        commonpb.IndexState_Finished,
							NodeID:       1,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return nil
					},
				},
			},
		}
		gc.wg.Add(1)
		go gc.recycleUnusedMeta()
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
				indexBuildID2Meta: map[UniqueID]*Meta{
					1: {
						indexMeta: &indexpb.IndexMeta{
							IndexBuildID: 1,
							MarkDeleted:  true,
							NodeID:       0,
						},
					},
				},
				client: &mockETCDKV{
					remove: func(s string) error {
						return fmt.Errorf("error")
					},
				},
			},
		}
		gc.wg.Add(1)
		go gc.recycleUnusedMeta()
		time.Sleep(time.Second)
		cancel()
		gc.wg.Wait()
	})
}
