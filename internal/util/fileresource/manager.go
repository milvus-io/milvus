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
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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
		GlobalFileManager = NewManager(storage, mode)
	})
}

func Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error {
	if GlobalFileManager == nil {
		mlog.Error(ctx, "sync file resource to file manager not init")
		return nil
	}

	return GlobalFileManager.Sync(ctx, version, resourceList)
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
	Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error

	Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error
	Release(resources ...*internalpb.FileResourceInfo)
	Close()
	Mode() Mode
}

type Mode int

func (m Mode) String() string {
	switch m {
	case SyncMode:
		return SyncModeStr
	case RefMode:
		return RefModeStr
	case CloseMode:
		return CloseModeStr
	default:
		return fmt.Sprintf("unknown(%d)", m)
	}
}

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

func newBaseManager() BaseManager {
	return BaseManager{
		localPath: pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID()),
	}
}

func (m *BaseManager) Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error {
	return nil
}

func (m *BaseManager) Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error {
	return nil
}
func (m *BaseManager) Release(resources ...*internalpb.FileResourceInfo) {}
func (m *BaseManager) Close()                                            {}
func (m *BaseManager) Mode() Mode                                        { return CloseMode }
func (m *BaseManager) GetVersion() uint64                                { return 0 }

func (m *BaseManager) downloadFile(ctx context.Context, downloader storage.ChunkManager, resource *internalpb.FileResourceInfo, destination string, downloadTimeout time.Duration) error {
	if downloader == nil {
		return merr.WrapErrServiceNotReadyMsg("file resource downloader is not initialized")
	}
	if resource == nil {
		return merr.WrapErrServiceInternalMsg("file resource is nil")
	}

	downloadCtx, cancel := context.WithTimeout(ctx, downloadTimeout)
	defer cancel()

	size, err := downloader.Size(downloadCtx, resource.GetPath())
	if err != nil {
		return merr.Wrapf(err, "get file resource size for %s", resource.GetPath())
	}
	if size < 0 {
		return merr.WrapErrIoFailedMsg("file resource %s reports negative size %d", resource.GetPath(), size)
	}
	reader, err := downloader.Reader(downloadCtx, resource.GetPath())
	if err != nil {
		return merr.Wrapf(err, "open file resource %s", resource.GetPath())
	}
	readerClosed := false
	defer func() {
		if !readerClosed {
			if err := reader.Close(); err != nil {
				mlog.Warn(ctx, "close file resource reader failed", mlog.String("path", resource.GetPath()), mlog.Err(err))
			}
		}
	}()

	destinationDir := path.Dir(destination)
	if err := os.MkdirAll(destinationDir, os.ModePerm); err != nil {
		return merr.WrapErrIoFailed(destinationDir, err)
	}
	temporary, err := os.CreateTemp(destinationDir, ".file-resource-*")
	if err != nil {
		return merr.WrapErrIoFailed(destination, err)
	}
	temporaryPath := temporary.Name()
	committed := false
	defer func() {
		if !committed {
			temporary.Close()
			if err := os.Remove(temporaryPath); err != nil && !errors.Is(err, os.ErrNotExist) {
				mlog.Warn(ctx, "remove incomplete file resource failed", mlog.String("path", temporaryPath), mlog.Err(err))
			}
		}
	}()

	written, err := io.Copy(temporary, reader)
	if err != nil {
		return merr.Wrapf(err, "download file resource %s", resource.GetPath())
	}
	if written != size {
		return merr.WrapErrIoUnexpectEOF(resource.GetPath(), errors.Newf("expected %d bytes, downloaded %d", size, written))
	}
	if err := reader.Close(); err != nil {
		return merr.WrapErrIoFailed(resource.GetPath(), err)
	}
	readerClosed = true
	if err := downloadCtx.Err(); err != nil {
		return merr.Wrapf(err, "download file resource %s", resource.GetPath())
	}
	if err := temporary.Sync(); err != nil {
		return merr.WrapErrIoFailed(destination, err)
	}
	if err := temporary.Close(); err != nil {
		return merr.WrapErrIoFailed(destination, err)
	}
	if err := os.Rename(temporaryPath, destination); err != nil {
		return merr.WrapErrIoFailed(destination, err)
	}
	committed = true
	return nil
}

// Manager with Sync Mode
// mixcoord should sync all node after add or remove file resource.
// file will download to /<local_resource_path>>/<resource_id>/<file_name>
type SyncManager struct {
	BaseManager
	sync.RWMutex
	downloader storage.ChunkManager

	version *atomic.Uint64

	localResourcesLoaded bool
	localResourceIDs     map[int64]struct{}
	restoredResourceIDs  map[int64]struct{}
}

func (m *SyncManager) GetVersion() uint64 {
	return m.version.Load()
}

// loadLocalResources restores the resource IDs persisted in the manager's
// local directory. The resource names and source paths are recovered from the
// next Sync request, which remains the source of truth.
func (m *SyncManager) loadLocalResources() error {
	if m.localResourcesLoaded {
		return nil
	}

	entries, err := os.ReadDir(m.localPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return merr.WrapErrIoFailed(m.localPath, err)
	}

	m.localResourceIDs = make(map[int64]struct{})
	m.restoredResourceIDs = make(map[int64]struct{})
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		resourceID, err := strconv.ParseInt(entry.Name(), 10, 64)
		if err != nil || resourceID <= 0 {
			continue
		}
		m.localResourceIDs[resourceID] = struct{}{}
		m.restoredResourceIDs[resourceID] = struct{}{}
	}
	m.localResourcesLoaded = true
	return nil
}

func (m *SyncManager) hasLocalResourceFile(resourceID int64, localFilePath string) (bool, error) {
	if _, ok := m.localResourceIDs[resourceID]; !ok {
		return false, nil
	}
	if _, restored := m.restoredResourceIDs[resourceID]; !restored {
		return true, nil
	}

	info, err := os.Stat(localFilePath)
	if errors.Is(err, os.ErrNotExist) {
		delete(m.localResourceIDs, resourceID)
		delete(m.restoredResourceIDs, resourceID)
		return false, nil
	}
	if err != nil {
		return false, merr.WrapErrIoFailed(localFilePath, err)
	}
	if !info.Mode().IsRegular() {
		delete(m.localResourceIDs, resourceID)
		delete(m.restoredResourceIDs, resourceID)
		return false, nil
	}

	delete(m.restoredResourceIDs, resourceID)
	return true, nil
}

// sync file to local if file mode was Sync
func (m *SyncManager) Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error {
	m.Lock()
	defer m.Unlock()

	if version <= m.version.Load() {
		return nil
	}
	if err := m.loadLocalResources(); err != nil {
		return err
	}

	newResourceMap := make(map[string]int64)
	resolvedResources := make([]*ResolvedFileResource, 0, len(resourceList))

	for _, resource := range resourceList {
		newResourceMap[resource.GetName()] = resource.GetId()
		localResourcePath := path.Join(m.localPath, fmt.Sprint(resource.GetId()))
		localFilePath := path.Join(localResourcePath, path.Base(resource.GetPath()))
		localFileReady, err := m.hasLocalResourceFile(resource.GetId(), localFilePath)
		if err != nil {
			return err
		}
		if localFileReady {
			resolvedResources = append(resolvedResources, &ResolvedFileResource{
				ID:        resource.GetId(),
				Name:      resource.GetName(),
				Path:      resource.GetPath(),
				LocalPath: localFilePath,
			})
			continue
		}

		downloadTimeout := paramtable.Get().CommonCfg.FileResourceDownloadTimeout.GetAsDurationByParse()
		if err := m.downloadFile(ctx, m.downloader, resource, localFilePath, downloadTimeout); err != nil {
			mlog.Warn(ctx, "download file resource failed",
				mlog.String("name", resource.GetName()),
				mlog.String("path", resource.GetPath()),
				mlog.Int64("resourceID", resource.GetId()),
				mlog.Duration("timeout", downloadTimeout),
				mlog.Err(err))
			return err
		}
		m.localResourceIDs[resource.GetId()] = struct{}{}
		delete(m.restoredResourceIDs, resource.GetId())
		mlog.Info(ctx, "sync file resource to local", mlog.String("path", localResourcePath), mlog.Int64("resourceID", resource.GetId()))
		resolvedResources = append(resolvedResources, &ResolvedFileResource{
			ID:        resource.GetId(),
			Name:      resource.GetName(),
			Path:      resource.GetPath(),
			LocalPath: localFilePath,
		})
	}

	if err := analyzer.UpdateGlobalResourceInfo(newResourceMap); err != nil {
		return err
	}

	activeIDs := make(map[int64]struct{}, len(newResourceMap))
	for _, id := range newResourceMap {
		activeIDs[id] = struct{}{}
	}
	for id := range m.localResourceIDs {
		if _, ok := activeIDs[id]; ok {
			continue
		}
		if err := os.RemoveAll(path.Join(m.localPath, fmt.Sprint(id))); err != nil {
			mlog.Warn(ctx, "remove local file resource failed", mlog.Int64("resourceID", id), mlog.Err(err))
			continue
		}
		delete(m.localResourceIDs, id)
		delete(m.restoredResourceIDs, id)
	}

	m.version.Store(version)
	notifyListeners(SyncEvent{Version: version, Resources: resolvedResources})
	return nil
}

func (m *SyncManager) Mode() Mode { return SyncMode }

func NewSyncManager(downloader storage.ChunkManager) *SyncManager {
	manager := &SyncManager{
		BaseManager: newBaseManager(),
		downloader:  downloader,
		version:     atomic.NewUint64(0),
	}
	if err := manager.loadLocalResources(); err != nil {
		mlog.Warn(context.TODO(), "load local file resources failed", mlog.String("path", manager.localPath), mlog.Err(err))
	}
	return manager
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
	cancel   context.CancelFunc
	wg       sync.WaitGroup
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
			fileName := path.Join(localResourcePath, path.Base(resource.GetPath()))
			downloadTimeout := paramtable.Get().CommonCfg.FileResourceDownloadTimeout.GetAsDurationByParse()
			if err := m.downloadFile(ctx, downloader, resource, fileName, downloadTimeout); err != nil {
				mlog.Warn(ctx, "download file resource failed",
					mlog.String("name", resource.GetName()),
					mlog.String("path", resource.GetPath()),
					mlog.Int64("resourceID", resource.GetId()),
					mlog.Duration("timeout", downloadTimeout),
					mlog.Err(err))
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
	ctx, cancel := context.WithCancel(context.Background())
	m.cancel = cancel
	m.wg.Add(1)
	go m.GcLoop(ctx)
}

func (m *RefManager) GcLoop(ctx context.Context) {
	defer m.wg.Done()
	ticker := time.NewTicker(15 * time.Minute)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			m.CleanResource()
		case <-ctx.Done():
			return
		}
	}
}

func (m *RefManager) Close() {
	if m.cancel != nil {
		m.cancel()
		m.wg.Wait()
	}
}

func NewRefManger() *RefManager {
	return &RefManager{
		BaseManager: newBaseManager(),
		ref:         map[string]int{},
		finished:    typeutil.NewConcurrentMap[string, bool](),
		sf:          &conc.Singleflight[interface{}]{},
	}
}

func NewManager(storage storage.ChunkManager, mode Mode) Manager {
	switch mode {
	case CloseMode:
		manager := newBaseManager()
		return &manager
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
