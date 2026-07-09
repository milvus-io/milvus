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
	"strings"
	"sync"
	"time"

	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/internal/util/pathutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

var (
	GlobalFileManager Manager
	managerMu         sync.Mutex
	listeners         = make(map[string]Listener)
	listenerMu        sync.RWMutex
)

func InitManager(storage storage.ChunkManager, mode Mode) error {
	managerMu.Lock()
	defer managerMu.Unlock()

	if GlobalFileManager != nil {
		if GlobalFileManager.Mode() != mode {
			return merr.WrapErrServiceInternalMsg(
				"file resource manager already initialized in %s mode, cannot initialize in %s mode",
				GlobalFileManager.Mode(), mode,
			)
		}
		return nil
	}

	GlobalFileManager = NewManager(storage, mode)
	return nil
}

func getManager() Manager {
	managerMu.Lock()
	defer managerMu.Unlock()
	return GlobalFileManager
}

func Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error {
	manager := getManager()
	if manager == nil {
		return merr.WrapErrServiceNotReadyMsg("file resource manager is not initialized")
	}

	return manager.Sync(ctx, version, resourceList)
}

func IsReady() bool {
	manager := getManager()
	return manager != nil && manager.IsReady()
}

func CheckReady() error {
	return checkReady(getManager())
}

func checkReady(manager Manager) error {
	if manager == nil {
		return merr.WrapErrServiceNotReadyMsg("file resource manager is not initialized")
	}
	if manager.Mode() != SyncMode {
		return merr.WrapErrServiceUnavailableMsg("file resource snapshot synchronization is disabled")
	}
	if manager.IsReady() {
		return nil
	}
	return merr.WrapErrServiceUnavailableMsg("file resource snapshot is not ready")
}

func Prepare(ctx context.Context, downloader storage.ChunkManager, resources []*internalpb.FileResourceInfo) (string, func(), error) {
	if len(resources) == 0 {
		return "", func() {}, nil
	}
	manager := getManager()
	if manager == nil {
		return "", nil, merr.WrapErrServiceNotReadyMsg("file resource manager is not initialized")
	}

	switch manager.Mode() {
	case RefMode:
		if err := manager.Download(ctx, downloader, resources...); err != nil {
			return "", nil, err
		}
		extraInfo, err := analyzer.BuildExtraResourceInfo(downloader.RootPath(), resources)
		if err != nil {
			manager.Release(resources...)
			return "", nil, err
		}
		return extraInfo, func() { manager.Release(resources...) }, nil
	case SyncMode:
		syncManager, ok := manager.(*SyncManager)
		if !ok {
			return "", nil, merr.WrapErrServiceInternalMsg("file resource sync manager has unexpected type %T", manager)
		}
		if err := syncManager.syncSem.Acquire(ctx); err != nil {
			return "", nil, err
		}
		if !syncManager.IsReady() {
			syncManager.syncSem.Release()
			return "", nil, merr.WrapErrServiceUnavailableMsg("file resource snapshot is not ready")
		}
		for _, resource := range resources {
			id, ok := syncManager.resourceMap[resource.GetName()]
			if !ok || id != resource.GetId() || !syncManager.resourceReady(resource) {
				syncManager.syncSem.Release()
				return "", nil, merr.WrapErrServiceUnavailableMsg(
					"file resource %q is not available in the synchronized snapshot",
					resource.GetName(),
				)
			}
		}
		extraInfo, err := analyzer.BuildExtraResourceInfo("", resources)
		if err != nil {
			syncManager.syncSem.Release()
			return "", nil, err
		}
		return extraInfo, syncManager.syncSem.Release, nil
	case CloseMode:
		return "", nil, merr.WrapErrServiceUnavailableMsg("file resource is required but synchronization is disabled")
	default:
		return "", nil, merr.WrapErrServiceInternalMsg("unknown file resource manager mode %d", manager.Mode())
	}
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

func notifyListeners(ctx context.Context, event SyncEvent) {
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
			mlog.Warn(ctx, "file resource sync listener failed", mlog.String("listener", name), mlog.Err(err))
		}
	}
}

// Manager manage file resource
type Manager interface {
	GetVersion() uint64
	// IsReady reports whether the manager has applied its first complete snapshot
	// since process startup. Later asynchronous updates do not reset this state.
	IsReady() bool
	// sync resource to local
	Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error

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

func (m *BaseManager) Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error {
	return merr.WrapErrServiceUnavailableMsg("file resource sync is disabled")
}

func (m *BaseManager) Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) error {
	return nil
}
func (m *BaseManager) Release(resources ...*internalpb.FileResourceInfo) {}
func (m *BaseManager) Mode() Mode                                        { return CloseMode }
func (m *BaseManager) GetVersion() uint64                                { return 0 }
func (m *BaseManager) IsReady() bool                                     { return false }

// Manager with Sync Mode
// mixcoord should sync all node after add or remove file resource.
// file will download to /<local_resource_path>>/<resource_id>/<file_name>
type SyncManager struct {
	BaseManager
	downloader storage.ChunkManager
	syncSem    *syncutil.Semaphore

	version            *atomic.Uint64
	ready              *atomic.Bool
	resourceMap        map[string]int64 // resource name -> resource id
	updateResourceInfo func(map[string]int64) error
}

func (m *SyncManager) GetVersion() uint64 {
	return m.version.Load()
}

func (m *SyncManager) IsReady() bool {
	return m.ready != nil && m.ready.Load()
}

// sync file to local if file mode was Sync
func (m *SyncManager) Sync(ctx context.Context, version uint64, resourceList []*internalpb.FileResourceInfo) error {
	maxDuration := paramtable.Get().CommonCfg.FileResourceSyncMaxDuration.GetAsDurationByParse()
	if maxDuration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, maxDuration)
		defer cancel()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	if err := m.syncSem.Acquire(ctx); err != nil {
		return err
	}
	defer m.syncSem.Release()

	if m.IsReady() && version <= m.version.Load() {
		return nil
	}

	newResourceMap := make(map[string]int64, len(resourceList))
	resolvedResources := make([]*ResolvedFileResource, 0, len(resourceList))
	currentResourceIDs := make(map[int64]struct{}, len(m.resourceMap))
	for _, id := range m.resourceMap {
		currentResourceIDs[id] = struct{}{}
	}

	if err := os.MkdirAll(m.localPath, os.ModePerm); err != nil {
		return merr.WrapErrIoFailed(m.localPath, err)
	}

	newResourceIDs := make(map[int64]struct{}, len(resourceList))
	for _, resource := range resourceList {
		newResourceMap[resource.GetName()] = resource.GetId()
		newResourceIDs[resource.GetId()] = struct{}{}
		resolvedResources = append(resolvedResources, &ResolvedFileResource{
			ID:        resource.GetId(),
			Name:      resource.GetName(),
			Path:      resource.GetPath(),
			LocalPath: m.resourceFilePath(resource),
		})
		if id, ok := m.resourceMap[resource.GetName()]; ok && id == resource.GetId() {
			continue
		}
		if _, ok := currentResourceIDs[resource.GetId()]; ok {
			continue
		}
		if m.resourceReady(resource) {
			continue
		}
		if err := m.prepareResource(ctx, resource); err != nil {
			return err
		}
	}

	if err := m.updateResourceInfo(newResourceMap); err != nil {
		return merr.Wrap(err, "update analyzer file resource info")
	}

	m.resourceMap = newResourceMap
	m.version.Store(version)
	notifyListeners(ctx, SyncEvent{Version: version, Resources: resolvedResources})
	m.ready.Store(true)
	mlog.Info(ctx, "file resource snapshot applied",
		mlog.Uint64("version", version),
		mlog.Int("resourceCount", len(resolvedResources)))
	m.cleanObsoleteResources(ctx, newResourceIDs)
	return nil
}

func (m *SyncManager) resourcePath(resourceID int64) string {
	return path.Join(m.localPath, strconv.FormatInt(resourceID, 10))
}

func (m *SyncManager) resourceFilePath(resource *internalpb.FileResourceInfo) string {
	return path.Join(m.resourcePath(resource.GetId()), path.Base(resource.GetPath()))
}

func (m *SyncManager) tempResourcePath(resourceID int64) string {
	return path.Join(m.localPath, fmt.Sprintf(".download-%d", resourceID))
}

func (m *SyncManager) resourceReady(resource *internalpb.FileResourceInfo) bool {
	info, err := os.Stat(m.resourceFilePath(resource))
	return err == nil && info.Mode().IsRegular()
}

func (m *SyncManager) prepareResource(ctx context.Context, resource *internalpb.FileResourceInfo) error {
	tempResourcePath := m.tempResourcePath(resource.GetId())
	defer os.RemoveAll(tempResourcePath)

	if err := os.RemoveAll(tempResourcePath); err != nil {
		return merr.WrapErrIoFailed(tempResourcePath, err)
	}
	if err := os.MkdirAll(tempResourcePath, os.ModePerm); err != nil {
		return merr.WrapErrIoFailed(tempResourcePath, err)
	}
	if err := m.downloadResource(ctx, resource, tempResourcePath); err != nil {
		return err
	}

	localResourcePath := m.resourcePath(resource.GetId())
	if err := os.RemoveAll(localResourcePath); err != nil {
		return merr.WrapErrIoFailed(localResourcePath, err)
	}
	if err := os.Rename(tempResourcePath, localResourcePath); err != nil {
		return merr.WrapErrIoFailed(localResourcePath, err)
	}
	return nil
}

func (m *SyncManager) cleanObsoleteResources(ctx context.Context, resourceIDs map[int64]struct{}) {
	entries, err := os.ReadDir(m.localPath)
	if err != nil {
		mlog.Warn(ctx, "list local file resources failed", mlog.String("path", m.localPath), mlog.Err(err))
		return
	}

	for _, entry := range entries {
		name := entry.Name()
		if strings.HasPrefix(name, ".download-") || strings.HasPrefix(name, ".sync-") {
			localResourcePath := path.Join(m.localPath, name)
			if err := os.RemoveAll(localResourcePath); err != nil {
				mlog.Warn(ctx, "remove stale local file resource failed", mlog.String("path", localResourcePath), mlog.Err(err))
			}
			continue
		}
		if !entry.IsDir() {
			continue
		}
		resourceID, err := strconv.ParseInt(name, 10, 64)
		if err != nil {
			continue
		}
		if _, ok := resourceIDs[resourceID]; ok {
			continue
		}
		localResourcePath := path.Join(m.localPath, name)
		if err := os.RemoveAll(localResourcePath); err != nil {
			mlog.Warn(ctx, "remove obsolete local resource failed", mlog.Int64("id", resourceID), mlog.Err(err))
		}
	}
}

func (m *SyncManager) downloadResource(ctx context.Context, resource *internalpb.FileResourceInfo, localResourcePath string) error {
	if m.downloader == nil {
		return merr.WrapErrServiceNotReadyMsg("file resource downloader is not initialized")
	}

	idleTimeout := paramtable.Get().CommonCfg.FileResourceSyncIdleTimeout.GetAsDurationByParse()
	downloadCtx, watchdog := newDownloadWatchdog(ctx, idleTimeout)
	defer watchdog.Stop()

	reader, err := m.downloader.Reader(downloadCtx, resource.GetPath())
	if err != nil {
		return watchdog.WrapError(err, resource.GetPath())
	}
	defer reader.Close()
	stopClose := context.AfterFunc(downloadCtx, func() {
		_ = reader.Close()
	})
	defer stopClose()

	maxFileSize := paramtable.Get().CommonCfg.FileResourceMaxFileSize.GetAsSize()
	if err := checkFileResourceSize(downloadCtx, m.downloader, resource.GetPath(), maxFileSize); err != nil {
		return err
	}

	fileName := path.Join(localResourcePath, path.Base(resource.GetPath()))
	file, err := os.Create(fileName)
	if err != nil {
		return merr.WrapErrIoFailed(fileName, err)
	}
	fileClosed := false
	defer func() {
		if !fileClosed {
			_ = file.Close()
		}
	}()

	downloadReader := io.Reader(&progressReader{reader: reader, progress: watchdog.Progress})
	if maxFileSize > 0 {
		downloadReader = &maxFileSizeReader{
			reader:   downloadReader,
			path:     resource.GetPath(),
			maxBytes: maxFileSize,
		}
	}
	_, err = io.Copy(file, downloadReader)
	if err != nil {
		return watchdog.WrapError(err, resource.GetPath())
	}
	watchdog.Stop()
	if err := file.Sync(); err != nil {
		return merr.WrapErrIoFailed(fileName, err)
	}
	if err := file.Close(); err != nil {
		return merr.WrapErrIoFailed(fileName, err)
	}
	fileClosed = true
	mllog := mlog.With(mlog.String("name", fileName), mlog.Int64("id", resource.GetId()))
	mllog.Info(ctx, "sync file to local")
	return nil
}

func checkFileResourceSize(ctx context.Context, downloader storage.ChunkManager, resourcePath string, maxFileSize int64) error {
	if maxFileSize <= 0 {
		return nil
	}

	fileSize, err := downloader.Size(ctx, resourcePath)
	if err != nil {
		return merr.Wrapf(err, "get file resource %q size", resourcePath)
	}
	if fileSize > maxFileSize {
		return merr.Wrap(merr.ErrServiceResourceInsufficient,
			fmt.Sprintf("file resource %q size %d exceeds maximum %d", resourcePath, fileSize, maxFileSize))
	}
	return nil
}

type maxFileSizeReader struct {
	reader   io.Reader
	path     string
	maxBytes int64
	read     int64
}

func (r *maxFileSizeReader) Read(p []byte) (int, error) {
	remaining := r.maxBytes - r.read
	if remaining < 0 {
		return 0, merr.Wrap(merr.ErrServiceResourceInsufficient,
			fmt.Sprintf("file resource %q exceeds maximum size %d", r.path, r.maxBytes))
	}

	readLimit := int64(len(p))
	if remaining < int64(len(p)) {
		readLimit = remaining + 1
	}
	n, err := r.reader.Read(p[:int(readLimit)])
	r.read += int64(n)
	if r.read > r.maxBytes {
		return n, merr.Wrap(merr.ErrServiceResourceInsufficient,
			fmt.Sprintf("file resource %q exceeds maximum size %d", r.path, r.maxBytes))
	}
	return n, err
}

type progressReader struct {
	reader   io.Reader
	progress func()
}

func (r *progressReader) Read(p []byte) (int, error) {
	n, err := r.reader.Read(p)
	if n > 0 {
		r.progress()
	}
	return n, err
}

type downloadWatchdog struct {
	mu        sync.Mutex
	cancel    context.CancelFunc
	timer     *time.Timer
	timeout   time.Duration
	timedOut  bool
	isStopped bool
}

func newDownloadWatchdog(ctx context.Context, idleTimeout time.Duration) (context.Context, *downloadWatchdog) {
	ctx, cancel := context.WithCancel(ctx)
	watchdog := &downloadWatchdog{
		cancel:  cancel,
		timeout: idleTimeout,
	}
	if idleTimeout > 0 {
		watchdog.timer = time.AfterFunc(idleTimeout, watchdog.timeoutDownload)
	}
	return ctx, watchdog
}

func (w *downloadWatchdog) timeoutDownload() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isStopped {
		return
	}
	w.timedOut = true
	w.cancel()
}

func (w *downloadWatchdog) Progress() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isStopped || w.timer == nil {
		return
	}
	w.timer.Stop()
	w.timer.Reset(w.timeout)
}

func (w *downloadWatchdog) Stop() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.isStopped {
		return
	}
	w.isStopped = true
	if w.timer != nil {
		w.timer.Stop()
	}
	w.cancel()
}

func (w *downloadWatchdog) WrapError(err error, resourcePath string) error {
	w.mu.Lock()
	timedOut := w.timedOut
	w.mu.Unlock()
	if timedOut {
		return merr.WrapErrServiceUnavailableMsg("file resource %q download made no progress for %s", resourcePath, w.timeout)
	}
	return merr.Wrapf(err, "download file resource %q", resourcePath)
}

func (m *SyncManager) Mode() Mode { return SyncMode }

func NewSyncManager(downloader storage.ChunkManager) *SyncManager {
	return &SyncManager{
		BaseManager:        BaseManager{localPath: pathutil.GetPath(pathutil.FileResourcePath, paramtable.GetNodeID())},
		downloader:         downloader,
		syncSem:            syncutil.NewSemaphore(1),
		resourceMap:        make(map[string]int64),
		version:            atomic.NewUint64(0),
		ready:              atomic.NewBool(false),
		updateResourceInfo: analyzer.UpdateGlobalResourceInfo,
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

func (m *RefManager) Download(ctx context.Context, downloader storage.ChunkManager, resources ...*internalpb.FileResourceInfo) (err error) {
	m.Lock()
	// inc ref count and set storage name with storage root path
	for _, resource := range resources {
		key := fmt.Sprintf("%s/%d", downloader.RootPath(), resource.GetId())
		resource.StorageName = downloader.RootPath()
		m.ref[key] += 1
	}
	m.Unlock()

	succeeded := false
	defer func() {
		if succeeded {
			return
		}
		m.Lock()
		defer m.Unlock()
		for _, resource := range resources {
			key := fmt.Sprintf("%s/%d", downloader.RootPath(), resource.GetId())
			m.ref[key] -= 1
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

			maxFileSize := paramtable.Get().CommonCfg.FileResourceMaxFileSize.GetAsSize()
			if err := checkFileResourceSize(ctx, downloader, resource.GetPath(), maxFileSize); err != nil {
				return nil, err
			}

			fileName := path.Join(localResourcePath, path.Base(resource.GetPath()))
			file, err := os.CreateTemp(localResourcePath, ".download-*")
			if err != nil {
				return nil, merr.WrapErrIoFailed(localResourcePath, err)
			}
			tempFileName := file.Name()
			defer os.Remove(tempFileName)
			defer file.Close()

			downloadReader := io.Reader(reader)
			if maxFileSize > 0 {
				downloadReader = &maxFileSizeReader{
					reader:   reader,
					path:     resource.GetPath(),
					maxBytes: maxFileSize,
				}
			}
			if _, err = io.Copy(file, downloadReader); err != nil {
				return nil, err
			}
			if err := file.Sync(); err != nil {
				return nil, merr.WrapErrIoFailed(tempFileName, err)
			}
			if err := file.Close(); err != nil {
				return nil, merr.WrapErrIoFailed(tempFileName, err)
			}
			if err := os.Rename(tempFileName, fileName); err != nil {
				return nil, merr.WrapErrIoFailed(fileName, err)
			}
			m.finished.Insert(key, true)
			return nil, nil
		})

		if err != nil {
			return err
		}
	}
	succeeded = true
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

func (m *RefManager) Mode() Mode    { return RefMode }
func (m *RefManager) IsReady() bool { return false }

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
