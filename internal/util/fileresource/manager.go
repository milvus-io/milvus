/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package fileresource

import (
	"context"
	"fmt"
	"io"
	"os"
	"path"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

var (
	GlobalFileManager Manager
	once              sync.Once
)

func InitManager(storage storage.ChunkManager, mode Mode) {
	once.Do(func() {
		m := NewManager(storage, mode)
		GlobalFileManager = m
	})
}

func Sync(resourceList []*internalpb.FileResourceInfo) error {
	if GlobalFileManager == nil {
		log.Error("sync file resource to file manager not init")
		return nil
	}

	return GlobalFileManager.Sync(resourceList)
}

// Manager manage file resource
type Manager interface {
	// sync resource to local
	Sync(resourceList []*internalpb.FileResourceInfo) error

	Download(downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error
	Release(resources ...*internalpb.FileResourceInfo)
	Mode() Mode
}

type Mode int

// manager mode
// Sync: sync when file resource list changed and download all file resource to local.
// Ref: install before use and delete local file if no one own it.
// Close: skip all action but don't return error.
const (
	SyncMode Mode = iota + 1
	RefMode
	CloseMode
)

type BaseManager struct {
	localPath string
}

func (m *BaseManager) Sync(resourceList []*internalpb.FileResourceInfo) error { return nil }
func (m *BaseManager) Download(downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error {
	return nil
}
func (m *BaseManager) Release(resources ...*internalpb.FileResourceInfo) {}
func (m *BaseManager) Mode() Mode                                        { return CloseMode }

// Manager with Sync Mode
// mixcoord should sync all node after add or remove file resource.
type SyncManager struct {
	BaseManager
	sync.RWMutex
	downloader  storage.ChunkManager
	resourceSet map[int64]struct{}
}

// sync file to local if file mode was Sync
func (m *SyncManager) Sync(resourceList []*internalpb.FileResourceInfo) error {
	m.Lock()
	defer m.Unlock()

	ctx := context.Background()
	newSet := make(map[int64]struct{})
	for _, resource := range resourceList {
		newSet[resource.GetId()] = struct{}{}
		if _, ok := m.resourceSet[resource.GetId()]; ok {
			continue
		}

		localResourcePath := path.Join(m.localPath, fmt.Sprint(resource.GetId()))

		err := os.MkdirAll(localResourcePath, os.ModePerm)
		if err != nil {
			return err
		}

		reader, err := m.downloader.Reader(ctx, resource.GetPath())
		if err != nil {
			log.Info("download resource failed", zap.String("path", resource.GetPath()), zap.Error(err))
			return err
		}
		defer reader.Close()

		fileName := path.Join(localResourcePath, path.Base(resource.GetPath()))
		file, err := os.Create(fileName)
		if err != nil {
			return err
		}
		defer file.Close()

		if _, err = io.Copy(file, reader); err != nil {
			log.Info("download resource failed", zap.String("path", resource.GetPath()), zap.Error(err))
			return err
		}
		log.Info("sync file to local", zap.String("name", fileName))
	}

	for resourceId := range m.resourceSet {
		if _, ok := newSet[resourceId]; !ok {
			err := os.RemoveAll(path.Join(m.localPath, fmt.Sprint(resourceId)))
			if err != nil {
				log.Warn("remove local resource failed", zap.Error(err))
				newSet[resourceId] = struct{}{}
			}
		}
	}
	m.resourceSet = newSet
	return nil
}

func (m *SyncManager) Mode() Mode { return SyncMode }

func NewSyncManager(downloader storage.ChunkManager) *SyncManager {
	return &SyncManager{
		BaseManager: BaseManager{localPath: pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())},
		downloader:  downloader,
		resourceSet: make(map[int64]struct{}),
	}
}

type RefManager struct {
	BaseManager
	sync.RWMutex
	ref map[string]int

	finished *typeutil.ConcurrentMap[string, bool]
	sf       *conc.Singleflight[interface{}]
}

func (m *RefManager) Download(downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error {
	m.Lock()
	// inc ref count and set storage name with storage root path
	for _, resource := range resources {
		key := fmt.Sprintf("%s/%d", downloader.RootPath(), resource.GetId())
		resource.StorageName = downloader.RootPath()
		m.ref[key] += 1
	}
	m.Unlock()

	ctx := context.Background()
	for _, r := range resources {
		resource := r
		key := fmt.Sprintf("%s/%d", downloader.RootPath(), resource.GetId())
		if ok, exist := m.finished.Get(key); exist && ok {
			continue
		}

		_, err, _ := m.sf.Do(key, func() (interface{}, error) {
			if ok, exist := m.finished.Get(key); exist && ok {
				return nil, nil
			}

			localResourcePath := path.Join(m.localPath, key)

			err := os.MkdirAll(localResourcePath, os.ModePerm)
			if err != nil {
				return nil, err
			}

			reader, err := downloader.Reader(ctx, resource.GetPath())
			if err != nil {
				log.Info("download resource failed", zap.String("path", resource.GetPath()), zap.Error(err))
				return nil, err
			}
			defer reader.Close()

			fileName := path.Join(localResourcePath, path.Base(resource.GetPath()))
			file, err := os.Create(fileName)
			if err != nil {
				return nil, err
			}
			defer file.Close()

			if _, err = io.Copy(file, reader); err != nil {
				return nil, err
			}
			m.finished.Insert(key, true)
			return nil, nil
		})

		if err != nil {
			return err
		}
	}
	return nil
}

func (m *RefManager) Release(resources ...*internalpb.FileResourceInfo) {
	m.Lock()
	defer m.Unlock()
	// dec ref
	for _, resource := range resources {
		key := fmt.Sprintf("%s/%d", resource.GetStorageName(), resource.GetId())
		m.ref[key] -= 1
	}
}

func (m *RefManager) Mode() Mode { return RefMode }

// clean file resource with no ref.
func (m *RefManager) CleanResource() {
	m.Lock()
	defer m.Unlock()

	for key, cnt := range m.ref {
		if cnt <= 0 {
			localResourcePath := path.Join(m.localPath, key)
			os.RemoveAll(localResourcePath)
			delete(m.ref, key)
			m.finished.Remove(key)
		}
	}
}

func (m *RefManager) GcLoop() {
	ticker := time.NewTicker(15 * time.Minute)

	for range ticker.C {
		m.CleanResource()
	}
}

func NewRefManger() *RefManager {
	return &RefManager{
		BaseManager: BaseManager{localPath: pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())},
		ref:         map[string]int{},
		finished:    typeutil.NewConcurrentMap[string, bool](),
		sf:          &conc.Singleflight[interface{}]{},
	}
}

func NewManager(storage storage.ChunkManager, mode Mode) Manager {
	switch mode {
	case CloseMode:
		return &BaseManager{}
	case SyncMode:
		return NewSyncManager(storage)
	default:
		panic(fmt.Sprintf("Unknown file resource mananger mod: %v", mode))
	}
}
