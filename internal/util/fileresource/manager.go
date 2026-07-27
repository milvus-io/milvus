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

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	GlobalFileManager Manager
	once              sync.Once
	listeners         = make(map[string]Listener)
	listenerMu        sync.RWMutex
)

func InitManager(storage storage.ChunkManager, mode Mode) {
	once.Do(func() {
		m := NewManager(storage, mode)
		GlobalFileManager = m
	})
}

func Sync(version uint64, resourceList []*internalpb.FileResourceInfo) error {
	if GlobalFileManager == nil {
		mlog.Error(context.TODO(), "sync file resource to file manager not init")
		return nil
	}

	return GlobalFileManager.Sync(version, resourceList)
}

func RegisterListener(name string, listener Listener) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	listeners[name] = listener
}

func UnregisterListener(name string) {
	listenerMu.Lock()
	defer listenerMu.Unlock()
	delete(listeners, name)
}

func notifyListeners(event SyncEvent) {
	listenerMu.RLock()
	cloned := make(map[string]Listener, len(listeners))
	for name, listener := range listeners {
		cloned[name] = listener
	}
	listenerMu.RUnlock()

	for name, listener := range cloned {
		if listener == nil {
			continue
		}
		if err := listener.OnFileResourceSync(event); err != nil {
			mlog.Warn(context.TODO(), "file resource sync listener failed", mlog.String("listener", name), mlog.Err(err))
		}
	}
}

// Manager manage file resource
type Manager interface {
	GetVersion() uint64
	// sync resource to local
	Sync(version uint64, resourceList []*internalpb.FileResourceInfo) error

	Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error
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

func (m *BaseManager) Sync(version uint64, resourceList []*internalpb.FileResourceInfo) error {
	return nil
}

func (m *BaseManager) Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error {
	return nil
}
func (m *BaseManager) Release(resources ...*internalpb.FileResourceInfo) {}
func (m *BaseManager) Mode() Mode                                        { return CloseMode }
func (m *BaseManager) GetVersion() uint64                                { return 0 }

// Manager with Sync Mode
// mixcoord should sync all node after add or remove file resource.
// file will download to /<local_resource_path>>/<resource_id>/<file_name>
type SyncManager struct {
	BaseManager
	sync.RWMutex
	downloader storage.ChunkManager

	version     *atomic.Uint64
	resourceMap map[string]int64 // resource name -> resource id
}

func (m *SyncManager) GetVersion() uint64 {
	return m.version.Load()
}

// sync file to local if file mode was Sync
func (m *SyncManager) Sync(version uint64, resourceList []*internalpb.FileResourceInfo) error {
	m.Lock()
	defer m.Unlock()

	// skip if version is not changed
	if version <= m.version.Load() {
		return nil
	}

	newResourceMap := make(map[string]int64)
	resolvedResources := make([]*ResolvedFileResource, 0, len(resourceList))
	removes := []int64{}
	ctx := context.Background()
	for _, resource := range resourceList {
		newResourceMap[resource.GetName()] = resource.GetId()
		localResourcePath := path.Join(m.localPath, fmt.Sprint(resource.GetId()))
		localFilePath := path.Join(localResourcePath, path.Base(resource.GetPath()))
		if id, ok := m.resourceMap[resource.GetName()]; ok {
			if id == resource.GetId() {
				resolvedResources = append(resolvedResources, &ResolvedFileResource{
					ID:        resource.GetId(),
					Name:      resource.GetName(),
					Path:      resource.GetPath(),
					LocalPath: localFilePath,
				})
				continue
			}
		}

		// download new resource

		// remove old file if exist
		err := os.RemoveAll(localResourcePath)
		if err != nil {
			mlog.Warn(context.TODO(), "remove invalid local resource failed", mlog.String("path", localResourcePath), mlog.Err(err))
		}

		err = os.MkdirAll(localResourcePath, os.ModePerm)
		if err != nil {
			return err
		}

		if err := func() error {
			reader, err := m.downloader.Reader(ctx, resource.GetPath())
			if err != nil {
				mlog.Info(context.TODO(), "download resource failed", mlog.String("path", resource.GetPath()), mlog.Err(err))
				return err
			}
			defer reader.Close()

			file, err := os.Create(localFilePath)
			if err != nil {
				return err
			}
			defer file.Close()

			if _, err = io.Copy(file, reader); err != nil {
				mlog.Info(context.TODO(), "download resource failed", mlog.String("path", resource.GetPath()), mlog.Err(err))
				return err
			}
			mlog.Info(context.TODO(), "sync file to local", mlog.String("name", localFilePath), mlog.Int64("id", resource.GetId()))
			return nil
		}(); err != nil {
			return err
		}
		resolvedResources = append(resolvedResources, &ResolvedFileResource{
			ID:        resource.GetId(),
			Name:      resource.GetName(),
			Path:      resource.GetPath(),
			LocalPath: localFilePath,
		})
	}

	for name, id := range m.resourceMap {
		if newId, ok := newResourceMap[name]; ok {
			if newId != id {
				// remove old resource with same name
				removes = append(removes, id)
			}
		} else {
			// remove old resource not exist in new resource list
			removes = append(removes, id)
		}
	}

	for _, resourceID := range removes {
		err := os.RemoveAll(path.Join(m.localPath, fmt.Sprint(resourceID)))
		if err != nil {
			mlog.Warn(context.TODO(), "remove local resource failed", mlog.Int64("id", resourceID), mlog.Err(err))
		}
	}
	m.resourceMap = newResourceMap
	m.version.Store(version)
	if err := analyzer.UpdateGlobalResourceInfo(newResourceMap); err != nil {
		return err
	}
	notifyListeners(SyncEvent{Version: version, Resources: resolvedResources})
	return nil
}

func (m *SyncManager) Mode() Mode { return SyncMode }

func NewSyncManager(downloader storage.ChunkManager) *SyncManager {
	return &SyncManager{
		BaseManager: BaseManager{localPath: pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())},
		downloader:  downloader,
		resourceMap: make(map[string]int64),
		version:     atomic.NewUint64(0),
	}
}

// RefManager only used for datanode.
// only download file will some one will use it.
// Should Download before use and Release after use.
// file will download to /<local_resource_path>>/<storage_name>/<resource_id>/<file_name>
// and delete file if no one own it for interval times.
type RefManager struct {
	BaseManager
	sync.RWMutex
	ref map[string]int

	finished *typeutil.ConcurrentMap[string, bool]
	sf       *conc.Singleflight[interface{}]
}

func (m *RefManager) Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error {
	m.Lock()
	// inc ref count and set storage name with storage root path
	for _, resource := range resources {
		key := fmt.Sprintf("%s/%d", downloader.RootPath(), resource.GetId())
		resource.StorageName = downloader.RootPath()
		m.ref[key] += 1
	}
	m.Unlock()
	downloaded := false
	defer func() {
		if !downloaded {
			m.Release(resources...)
		}
	}()

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
				mlog.Info(ctx, "download resource failed", mlog.String("path", resource.GetPath()), mlog.Err(err))
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
	downloaded = true
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

func (m *RefManager) Start() {
	go m.GcLoop()
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
	case RefMode:
		manager := NewRefManger()
		manager.Start()
		return manager
	default:
		panic(fmt.Sprintf("Unknown file resource mananger mod: %v", mode))
	}
}
