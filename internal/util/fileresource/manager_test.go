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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// MockReader implements storage.FileReader using composition
type MockReader struct {
	io.Reader
	io.Closer
	io.ReaderAt
	io.Seeker
	size int64
}

func (mr *MockReader) Size() (int64, error) {
	return mr.size, nil
}

func newMockReader(s string) *MockReader {
	ioReader := strings.NewReader(s)
	return &MockReader{Reader: ioReader, Closer: io.NopCloser(ioReader), size: int64(len(s))}
}

// BaseManagerSuite tests BaseManager
type BaseManagerSuite struct {
	suite.Suite
	manager *BaseManager
}

func (suite *BaseManagerSuite) SetupTest() {
	suite.manager = &BaseManager{localPath: "/tmp/test"}
}

func (suite *BaseManagerSuite) TestSync() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.Error(err)
}

func (suite *BaseManagerSuite) TestDownload() {
	mockStorage := mocks.NewChunkManager(suite.T())
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := suite.manager.Download(context.Background(), mockStorage, resources...)
	suite.NoError(err)
}

func (suite *BaseManagerSuite) TestRelease() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	suite.manager.Release(resources...)
	// Should not panic
}

func (suite *BaseManagerSuite) TestMode() {
	mode := suite.manager.Mode()
	suite.Equal(CloseMode, mode)
}

func TestBaseManagerSuite(t *testing.T) {
	suite.Run(t, new(BaseManagerSuite))
}

// SyncManagerSuite tests SyncManager
type SyncManagerSuite struct {
	suite.Suite
	manager     *SyncManager
	mockStorage *mocks.ChunkManager
	tempDir     string
}

func (suite *SyncManagerSuite) SetupTest() {
	listenerMu.Lock()
	listeners = make(map[string]Listener)
	listenerMu.Unlock()

	// Create temporary directory for tests
	var err error
	suite.tempDir, err = os.MkdirTemp(os.TempDir(), "fileresource_test_sync_*")
	suite.NoError(err)

	suite.mockStorage = mocks.NewChunkManager(suite.T())
	suite.manager = &SyncManager{
		BaseManager:        BaseManager{localPath: suite.tempDir},
		downloader:         suite.mockStorage,
		syncSem:            syncutil.NewSemaphore(1),
		version:            atomic.NewUint64(0),
		ready:              atomic.NewBool(false),
		resourceMap:        make(map[string]int64),
		updateResourceInfo: func(map[string]int64) error { return nil },
	}
	paramtable.Get().CommonCfg.FileResourceMaxFileSize.SwapTempValue("0")
}

func (suite *SyncManagerSuite) TearDownTest() {
	paramtable.Get().CommonCfg.FileResourceMaxFileSize.SwapTempValue("0")
	if suite.tempDir != "" {
		os.RemoveAll(suite.tempDir)
	}
}

func (suite *SyncManagerSuite) TestSync_InitialState() {
	suite.False(suite.manager.IsReady())
	suite.Equal(uint64(0), suite.manager.GetVersion())
}

func (suite *SyncManagerSuite) TestSync_InitialEmptySnapshotAtVersionZero() {
	err := suite.manager.Sync(context.Background(), 0, nil)
	suite.NoError(err)
	suite.True(suite.manager.IsReady())
	suite.Equal(uint64(0), suite.manager.GetVersion())
	suite.Empty(suite.manager.resourceMap)
}

func (suite *SyncManagerSuite) TestSync_Success() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test1.file", Path: "/storage/test1.file"},
		{Id: 2, Name: "test2.file", Path: "/storage/test2.file"},
	}

	// Mock the Reader calls
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test1.file").Return(newMockReader("test content 1"), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test2.file").Return(newMockReader("test content 2"), nil)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.NoError(err)

	// Verify files were created
	file1Path := path.Join(suite.tempDir, "1", "test1.file")
	file2Path := path.Join(suite.tempDir, "2", "test2.file")

	suite.FileExists(file1Path)
	suite.FileExists(file2Path)

	// Verify content
	content1, err := os.ReadFile(file1Path)
	suite.NoError(err)
	suite.Equal("test content 1", string(content1))

	content2, err := os.ReadFile(file2Path)
	suite.NoError(err)
	suite.Equal("test content 2", string(content2))
}

func (suite *SyncManagerSuite) TestSync_FileSizeLimit() {
	resource := &internalpb.FileResourceInfo{Id: 1, Name: "test.file", Path: "/storage/test.file"}
	paramtable.Get().CommonCfg.FileResourceMaxFileSize.SwapTempValue("4")

	suite.Run("reported size exceeds limit", func() {
		reader := newMockReader("12345")
		suite.mockStorage.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(reader, nil).Once()
		suite.mockStorage.EXPECT().Size(mock.Anything, resource.GetPath()).Return(int64(5), nil).Once()

		err := suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resource})
		suite.ErrorIs(err, merr.ErrServiceResourceInsufficient)
		suite.Equal(uint64(0), suite.manager.GetVersion())
		suite.NoFileExists(path.Join(suite.tempDir, "1", "test.file"))
	})

	suite.Run("stream exceeds reported size", func() {
		reader := newMockReader("12345")
		suite.mockStorage.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(reader, nil).Once()
		suite.mockStorage.EXPECT().Size(mock.Anything, resource.GetPath()).Return(int64(4), nil).Once()

		err := suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resource})
		suite.ErrorIs(err, merr.ErrServiceResourceInsufficient)
		suite.Equal(uint64(0), suite.manager.GetVersion())
		suite.NoFileExists(path.Join(suite.tempDir, "1", "test.file"))
	})

	suite.Run("size lookup error", func() {
		reader := newMockReader("1234")
		suite.mockStorage.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(reader, nil).Once()
		sizeErr := merr.WrapErrIoTooManyRequests(resource.GetPath(), io.ErrUnexpectedEOF)
		suite.mockStorage.EXPECT().Size(mock.Anything, resource.GetPath()).Return(int64(0), sizeErr).Once()

		err := suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resource})
		suite.ErrorIs(err, merr.ErrIoTooManyRequests)
		suite.Equal(uint64(0), suite.manager.GetVersion())
	})

	suite.Run("size equals limit", func() {
		reader := newMockReader("1234")
		suite.mockStorage.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(reader, nil).Once()
		suite.mockStorage.EXPECT().Size(mock.Anything, resource.GetPath()).Return(int64(4), nil).Once()

		err := suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resource})
		suite.NoError(err)
		suite.Equal(uint64(1), suite.manager.GetVersion())
	})
}

func (suite *SyncManagerSuite) TestSync_ReaderError() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/nonexistent.file"},
	}

	// Mock reader to return error
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/nonexistent.file").Return(nil, io.ErrUnexpectedEOF)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.Error(err)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
}

func (suite *SyncManagerSuite) TestSync_RetryReusesPreparedResource() {
	listener := &mockFileResourceListener{}
	RegisterListener("test", listener)
	defer UnregisterListener("test")

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "ready.file", Path: "/storage/ready.file"},
		{Id: 2, Name: "retry.file", Path: "/storage/retry.file"},
	}

	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader("ready"), nil).Once()
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[1].GetPath()).Return(nil, io.ErrUnexpectedEOF).Once()

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
	suite.False(suite.manager.IsReady())
	suite.Equal(uint64(0), suite.manager.GetVersion())
	suite.Empty(suite.manager.resourceMap)
	suite.Empty(listener.events)
	suite.FileExists(path.Join(suite.tempDir, "1", "ready.file"))
	suite.NoDirExists(path.Join(suite.tempDir, ".download-1"))
	suite.NoDirExists(path.Join(suite.tempDir, ".download-2"))

	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[1].GetPath()).Return(newMockReader("retry"), nil).Once()
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, resources))
	suite.True(suite.manager.IsReady())
	suite.Equal(uint64(1), suite.manager.GetVersion())
	suite.Equal(map[string]int64{"ready.file": 1, "retry.file": 2}, suite.manager.resourceMap)
	suite.Require().Len(listener.events, 1)
	suite.FileExists(path.Join(suite.tempDir, "2", "retry.file"))
}

func (suite *SyncManagerSuite) TestSync_ReusesPreparedResourceAfterRestart() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "ready.file", Path: "/storage/ready.file"},
		{Id: 2, Name: "retry.file", Path: "/storage/retry.file"},
	}

	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader("ready"), nil).Once()
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[1].GetPath()).Return(nil, io.ErrUnexpectedEOF).Once()
	suite.Error(suite.manager.Sync(context.Background(), 1, resources))

	restartedStorage := mocks.NewChunkManager(suite.T())
	restartedStorage.EXPECT().Reader(mock.Anything, resources[1].GetPath()).Return(newMockReader("retry"), nil).Once()
	restartedManager := &SyncManager{
		BaseManager:        BaseManager{localPath: suite.tempDir},
		downloader:         restartedStorage,
		syncSem:            syncutil.NewSemaphore(1),
		version:            atomic.NewUint64(0),
		ready:              atomic.NewBool(false),
		resourceMap:        make(map[string]int64),
		updateResourceInfo: func(map[string]int64) error { return nil },
	}

	suite.Require().NoError(restartedManager.Sync(context.Background(), 1, resources))
	suite.Equal(uint64(1), restartedManager.GetVersion())
	suite.FileExists(path.Join(suite.tempDir, "1", "ready.file"))
	suite.FileExists(path.Join(suite.tempDir, "2", "retry.file"))
}

func (suite *SyncManagerSuite) TestSync_InvalidFinalDirectoryIsDownloaded() {
	resource := &internalpb.FileResourceInfo{Id: 1, Name: "test.file", Path: "/storage/test.file"}
	invalidPath := path.Join(suite.tempDir, "1")
	suite.Require().NoError(os.MkdirAll(invalidPath, os.ModePerm))
	suite.Require().NoError(os.WriteFile(path.Join(invalidPath, "other.file"), []byte("stale"), 0o600))

	suite.mockStorage.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(newMockReader("test content"), nil).Once()
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resource}))

	filePath := path.Join(invalidPath, "test.file")
	suite.FileExists(filePath)
	suite.NoFileExists(path.Join(invalidPath, "other.file"))
}

func (suite *SyncManagerSuite) TestSync_CleansOrphanResourcesAfterCommit() {
	resource := &internalpb.FileResourceInfo{Id: 1, Name: "test.file", Path: "/storage/test.file"}
	suite.Require().NoError(os.MkdirAll(path.Join(suite.tempDir, "2"), os.ModePerm))
	suite.Require().NoError(os.WriteFile(path.Join(suite.tempDir, "2", "orphan.file"), []byte("orphan"), 0o600))
	suite.Require().NoError(os.MkdirAll(path.Join(suite.tempDir, ".sync-old"), os.ModePerm))
	suite.Require().NoError(os.MkdirAll(path.Join(suite.tempDir, "unrelated"), os.ModePerm))

	suite.mockStorage.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(newMockReader("test content"), nil).Once()
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resource}))

	suite.DirExists(path.Join(suite.tempDir, "1"))
	suite.NoDirExists(path.Join(suite.tempDir, "2"))
	suite.NoDirExists(path.Join(suite.tempDir, ".sync-old"))
	suite.DirExists(path.Join(suite.tempDir, "unrelated"))
}

func (suite *SyncManagerSuite) TestSync_NotifyListener() {
	listener := &mockFileResourceListener{}
	RegisterListener("test", listener)
	defer UnregisterListener("test")

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/test.file"},
	}
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test.file").Return(newMockReader("test content"), nil)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.Require().NoError(err)

	suite.Require().Len(listener.events, 1)
	event := listener.events[0]
	suite.Equal(uint64(1), event.Version)
	suite.Require().Len(event.Resources, 1)
	suite.Equal(int64(1), event.Resources[0].ID)
	suite.Equal("test.file", event.Resources[0].Name)
	suite.Equal("/storage/test.file", event.Resources[0].Path)
	suite.Equal(path.Join(suite.tempDir, "1", "test.file"), event.Resources[0].LocalPath)
}

func (suite *SyncManagerSuite) TestSync_UpdateAndRemoveNotifyListener() {
	listener := &mockFileResourceListener{}
	RegisterListener("test", listener)
	defer UnregisterListener("test")

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/test.file"},
	}
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test.file").Return(newMockReader("test content"), nil)
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, resources))

	updated := []*internalpb.FileResourceInfo{
		{Id: 2, Name: "test.file", Path: "/storage/test_v2.file"},
	}
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test_v2.file").Return(newMockReader("test content v2"), nil)
	suite.Require().NoError(suite.manager.Sync(context.Background(), 2, updated))

	suite.Require().Len(listener.events, 2)
	suite.Require().Len(listener.events[1].Resources, 1)
	suite.Equal(int64(2), listener.events[1].Resources[0].ID)
	suite.Equal("/storage/test_v2.file", listener.events[1].Resources[0].Path)
	suite.Equal(path.Join(suite.tempDir, "2", "test_v2.file"), listener.events[1].Resources[0].LocalPath)
	suite.NoDirExists(path.Join(suite.tempDir, "1"))

	suite.Require().NoError(suite.manager.Sync(context.Background(), 3, nil))
	suite.Require().Len(listener.events, 3)
	suite.Empty(listener.events[2].Resources)
	suite.NoDirExists(path.Join(suite.tempDir, "2"))
}

func (suite *SyncManagerSuite) TestSync_ListenerReceivesCompleteSnapshot() {
	listener := &mockFileResourceListener{}
	RegisterListener("test", listener)
	defer UnregisterListener("test")

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "unchanged.file", Path: "/storage/unchanged.file"},
	}
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/unchanged.file").Return(newMockReader("unchanged"), nil)
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, resources))

	updated := []*internalpb.FileResourceInfo{
		resources[0],
		{Id: 2, Name: "new.file", Path: "/storage/new.file"},
	}
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/new.file").Return(newMockReader("new"), nil)
	suite.Require().NoError(suite.manager.Sync(context.Background(), 2, updated))

	suite.Require().Len(listener.events, 2)
	suite.Require().Len(listener.events[1].Resources, 2)
	suite.Equal("unchanged.file", listener.events[1].Resources[0].Name)
	suite.Equal("new.file", listener.events[1].Resources[1].Name)
}

type mockFileResourceListener struct {
	events []SyncEvent
	err    error
}

func (m *mockFileResourceListener) OnFileResourceSync(event SyncEvent) error {
	m.events = append(m.events, event)
	return m.err
}

func (suite *SyncManagerSuite) TestSync_AnalyzerUpdateFailureCanRetry() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/test.file"},
	}

	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test.file").Return(newMockReader("test content"), nil).Once()
	suite.manager.updateResourceInfo = func(map[string]int64) error {
		return io.ErrUnexpectedEOF
	}

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
	suite.False(suite.manager.IsReady())
	suite.Equal(uint64(0), suite.manager.GetVersion())
	suite.Empty(suite.manager.resourceMap)

	suite.manager.updateResourceInfo = func(map[string]int64) error { return nil }
	err = suite.manager.Sync(context.Background(), 1, resources)
	suite.NoError(err)
	suite.True(suite.manager.IsReady())
	suite.Equal(uint64(1), suite.manager.GetVersion())
	suite.Equal(map[string]int64{"test.file": 1}, suite.manager.resourceMap)
}

func (suite *SyncManagerSuite) TestSync_FailedUpdateKeepsLastSnapshotReady() {
	resourceV1 := &internalpb.FileResourceInfo{Id: 1, Name: "test.file", Path: "/storage/test-v1.file"}
	suite.mockStorage.EXPECT().Reader(mock.Anything, resourceV1.GetPath()).Return(newMockReader("v1"), nil).Once()
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, []*internalpb.FileResourceInfo{resourceV1}))

	suite.manager.updateResourceInfo = func(map[string]int64) error { return io.ErrUnexpectedEOF }
	resourceV2 := &internalpb.FileResourceInfo{Id: 2, Name: "test.file", Path: "/storage/test-v2.file"}
	suite.mockStorage.EXPECT().Reader(mock.Anything, resourceV2.GetPath()).Return(newMockReader("v2"), nil).Once()

	err := suite.manager.Sync(context.Background(), 2, []*internalpb.FileResourceInfo{resourceV2})
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
	suite.True(suite.manager.IsReady())
	suite.Equal(uint64(1), suite.manager.GetVersion())
	suite.Equal(map[string]int64{"test.file": 1}, suite.manager.resourceMap)
	suite.FileExists(path.Join(suite.tempDir, "1", "test-v1.file"))
}

func (suite *SyncManagerSuite) TestSync_CanceledContext() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := suite.manager.Sync(ctx, 1, []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/test.file"},
	})
	suite.ErrorIs(err, context.Canceled)
	suite.Equal(uint64(0), suite.manager.GetVersion())
}

func (suite *SyncManagerSuite) TestDownloadWatchdog() {
	ctx, watchdog := newDownloadWatchdog(context.Background(), 10*time.Millisecond)
	defer watchdog.Stop()

	<-ctx.Done()
	err := watchdog.WrapError(ctx.Err(), "/storage/test.file")
	suite.Error(err)
}

func (suite *SyncManagerSuite) TestMode() {
	mode := suite.manager.Mode()
	suite.Equal(SyncMode, mode)
}

func TestSyncManagerSuite(t *testing.T) {
	suite.Run(t, new(SyncManagerSuite))
}

// RefManagerSuite tests RefManager
type RefManagerSuite struct {
	suite.Suite
	manager     *RefManager
	mockStorage *mocks.ChunkManager
	tempDir     string
}

func (suite *RefManagerSuite) SetupTest() {
	// Create temporary directory for tests
	var err error
	suite.tempDir, err = os.MkdirTemp(os.TempDir(), "fileresource_test_ref_*")
	suite.NoError(err)

	suite.mockStorage = mocks.NewChunkManager(suite.T())
	suite.manager = &RefManager{
		BaseManager: BaseManager{localPath: suite.tempDir},
		ref:         map[string]int{},
		finished:    typeutil.NewConcurrentMap[string, bool](),
		sf:          &conc.Singleflight[interface{}]{},
	}
	paramtable.Get().CommonCfg.FileResourceMaxFileSize.SwapTempValue("0")
}

func (suite *RefManagerSuite) TearDownTest() {
	paramtable.Get().CommonCfg.FileResourceMaxFileSize.SwapTempValue("0")
	if suite.tempDir != "" {
		os.RemoveAll(suite.tempDir)
	}
}

func (suite *RefManagerSuite) TestFileSizeLimitRollsBackRef() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test", Path: "/storage/test.file"},
	}
	paramtable.Get().CommonCfg.FileResourceMaxFileSize.SwapTempValue("4")

	suite.mockStorage.EXPECT().RootPath().Return("/test/storage")
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader("12345"), nil)
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[0].GetPath()).Return(int64(4), nil)

	err := suite.manager.Download(context.Background(), suite.mockStorage, resources...)
	suite.ErrorIs(err, merr.ErrServiceResourceInsufficient)
	suite.Equal(0, suite.manager.ref["/test/storage/1"])
	_, finished := suite.manager.finished.Get("/test/storage/1")
	suite.False(finished)
	suite.NoFileExists(path.Join(suite.tempDir, "/test/storage/1/test.file"))
}

func (suite *RefManagerSuite) TestNormal() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test", Path: "/storage/test.file"},
	}

	// Set up mock
	suite.mockStorage.EXPECT().RootPath().Return("/test/storage")
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test.file").Return(newMockReader("test content"), nil)

	err := suite.manager.Download(context.Background(), suite.mockStorage, resources...)
	suite.Require().NoError(err)

	// Verify ref count
	key := "/test/storage/1"
	suite.Equal(1, suite.manager.ref[key])

	// Verify storage name is set
	suite.Equal("/test/storage", resources[0].StorageName)

	// Verify file was downloaded
	// {local_path}/{storage_name}/{resource_id}/{file_name}
	filePath := path.Join(suite.tempDir, "/test/storage", fmt.Sprint(1), path.Base(resources[0].GetPath()))
	suite.FileExists(filePath)

	content, err := os.ReadFile(filePath)
	suite.NoError(err)
	suite.Equal("test content", string(content))

	// release and clean all file
	suite.manager.Release(resources...)
	suite.manager.CleanResource()
	suite.NoFileExists(filePath)
}

func (suite *RefManagerSuite) TestMode() {
	mode := suite.manager.Mode()
	suite.Equal(RefMode, mode)
}

func TestRefManagerSuite(t *testing.T) {
	suite.Run(t, new(RefManagerSuite))
}

// ManagerFactorySuite tests NewManager factory function
type ManagerFactorySuite struct {
	suite.Suite
	mockStorage *mocks.ChunkManager
}

func (suite *ManagerFactorySuite) SetupTest() {
	suite.mockStorage = mocks.NewChunkManager(suite.T())
}

func (suite *ManagerFactorySuite) TestNewManager_BaseManager() {
	manager := NewManager(suite.mockStorage, CloseMode)
	suite.IsType(&BaseManager{}, manager)
	suite.Equal(CloseMode, manager.Mode())
}

func (suite *ManagerFactorySuite) TestNewManager_SyncManager() {
	manager := NewManager(suite.mockStorage, SyncMode)
	suite.IsType(&SyncManager{}, manager)
	suite.Equal(SyncMode, manager.Mode())
}

func (suite *ManagerFactorySuite) TestNewManager_InvalidMode() {
	suite.Panics(func() {
		NewManager(suite.mockStorage, Mode(999))
	})
}

func TestManagerFactorySuite(t *testing.T) {
	suite.Run(t, new(ManagerFactorySuite))
}

func TestPrepareSyncPinsSnapshot(t *testing.T) {
	localPath := t.TempDir()
	resourceA := &internalpb.FileResourceInfo{Id: 1, Name: "resource-a", Path: "/remote/a.file"}
	resourceB := &internalpb.FileResourceInfo{Id: 2, Name: "resource-b", Path: "/remote/b.file"}
	resourceAPath := path.Join(localPath, "1")
	resourceBPath := path.Join(localPath, "2")
	require.NoError(t, os.MkdirAll(resourceAPath, os.ModePerm))
	require.NoError(t, os.MkdirAll(resourceBPath, os.ModePerm))
	require.NoError(t, os.WriteFile(path.Join(resourceAPath, "a.file"), []byte("a"), 0o600))
	require.NoError(t, os.WriteFile(path.Join(resourceBPath, "b.file"), []byte("b"), 0o600))

	manager := &SyncManager{
		BaseManager:        BaseManager{localPath: localPath},
		syncSem:            syncutil.NewSemaphore(1),
		resourceMap:        map[string]int64{resourceA.GetName(): resourceA.GetId()},
		version:            atomic.NewUint64(1),
		ready:              atomic.NewBool(true),
		updateResourceInfo: func(map[string]int64) error { return nil },
	}
	oldManager := GlobalFileManager
	GlobalFileManager = manager
	defer func() { GlobalFileManager = oldManager }()

	_, release, err := Prepare(context.Background(), nil, []*internalpb.FileResourceInfo{resourceA})
	require.NoError(t, err)

	blockedCtx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, manager.Sync(blockedCtx, 2, []*internalpb.FileResourceInfo{resourceB}), context.DeadlineExceeded)
	require.Equal(t, uint64(1), manager.version.Load())
	require.Equal(t, map[string]int64{resourceA.GetName(): resourceA.GetId()}, manager.resourceMap)

	release()
	require.NoError(t, manager.Sync(context.Background(), 2, []*internalpb.FileResourceInfo{resourceB}))
	require.Equal(t, uint64(2), manager.version.Load())
	require.Equal(t, map[string]int64{resourceB.GetName(): resourceB.GetId()}, manager.resourceMap)
}

func TestCheckReady(t *testing.T) {
	oldManager := GlobalFileManager
	defer func() { GlobalFileManager = oldManager }()

	t.Run("manager not initialized", func(t *testing.T) {
		GlobalFileManager = nil
		require.ErrorIs(t, CheckReady(), merr.ErrServiceNotReady)
	})

	t.Run("close mode", func(t *testing.T) {
		GlobalFileManager = &BaseManager{}
		require.ErrorIs(t, CheckReady(), merr.ErrServiceUnavailable)
	})

	t.Run("ref mode", func(t *testing.T) {
		GlobalFileManager = &RefManager{}
		require.ErrorIs(t, CheckReady(), merr.ErrServiceUnavailable)
	})

	t.Run("sync mode before snapshot", func(t *testing.T) {
		GlobalFileManager = &SyncManager{ready: atomic.NewBool(false)}
		require.ErrorIs(t, CheckReady(), merr.ErrServiceUnavailable)
	})

	t.Run("sync mode after snapshot", func(t *testing.T) {
		GlobalFileManager = &SyncManager{ready: atomic.NewBool(true)}
		require.NoError(t, CheckReady())
	})
}

func TestPrepare(t *testing.T) {
	oldManager := GlobalFileManager
	defer func() { GlobalFileManager = oldManager }()

	t.Run("sync mode rejects resources before first snapshot", func(t *testing.T) {
		localPath := t.TempDir()
		resource := &internalpb.FileResourceInfo{Id: 1, Name: "resource", Path: "/remote/resource.file"}
		resourcePath := path.Join(localPath, "1")
		require.NoError(t, os.MkdirAll(resourcePath, os.ModePerm))
		require.NoError(t, os.WriteFile(path.Join(resourcePath, "resource.file"), []byte("content"), 0o600))

		GlobalFileManager = &SyncManager{
			BaseManager: BaseManager{localPath: localPath},
			syncSem:     syncutil.NewSemaphore(1),
			resourceMap: map[string]int64{"resource": 1},
			version:     atomic.NewUint64(0),
			ready:       atomic.NewBool(false),
		}

		_, _, err := Prepare(context.Background(), nil, []*internalpb.FileResourceInfo{resource})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
		require.Contains(t, err.Error(), "snapshot is not ready")
	})

	t.Run("close mode rejects resources", func(t *testing.T) {
		GlobalFileManager = &BaseManager{}
		_, _, err := Prepare(context.Background(), nil, []*internalpb.FileResourceInfo{{Id: 1, Name: "resource"}})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})

	t.Run("ref mode downloads and releases resource", func(t *testing.T) {
		localPath := t.TempDir()
		resource := &internalpb.FileResourceInfo{Id: 1, Name: "resource", Path: "/remote/resource.file"}
		downloader := mocks.NewChunkManager(t)
		downloader.EXPECT().RootPath().Return("task-root")
		downloader.EXPECT().Reader(mock.Anything, resource.GetPath()).Return(newMockReader("content"), nil)

		manager := &RefManager{
			BaseManager: BaseManager{localPath: localPath},
			ref:         map[string]int{},
			finished:    typeutil.NewConcurrentMap[string, bool](),
			sf:          &conc.Singleflight[interface{}]{},
		}
		GlobalFileManager = manager

		extraInfo, release, err := Prepare(context.Background(), downloader, []*internalpb.FileResourceInfo{resource})
		require.NoError(t, err)
		require.Contains(t, extraInfo, "task-root")
		require.FileExists(t, path.Join(localPath, "task-root", "1", "resource.file"))
		require.Equal(t, 1, manager.ref["task-root/1"])
		release()
		require.Zero(t, manager.ref["task-root/1"])
	})

	t.Run("sync mode reuses synchronized resource", func(t *testing.T) {
		localPath := t.TempDir()
		resource := &internalpb.FileResourceInfo{Id: 1, Name: "resource", Path: "/remote/resource.file"}
		resourcePath := path.Join(localPath, "1")
		require.NoError(t, os.MkdirAll(resourcePath, os.ModePerm))
		require.NoError(t, os.WriteFile(path.Join(resourcePath, "resource.file"), []byte("content"), 0o600))

		manager := &SyncManager{
			BaseManager: BaseManager{localPath: localPath},
			syncSem:     syncutil.NewSemaphore(1),
			resourceMap: map[string]int64{"resource": 1},
			version:     atomic.NewUint64(1),
			ready:       atomic.NewBool(true),
		}
		GlobalFileManager = manager

		extraInfo, release, err := Prepare(context.Background(), nil, []*internalpb.FileResourceInfo{resource})
		require.NoError(t, err)
		require.NotEmpty(t, extraInfo)
		release()
	})

	t.Run("sync mode rejects missing resource", func(t *testing.T) {
		GlobalFileManager = &SyncManager{
			BaseManager: BaseManager{localPath: t.TempDir()},
			syncSem:     syncutil.NewSemaphore(1),
			resourceMap: map[string]int64{},
			version:     atomic.NewUint64(1),
			ready:       atomic.NewBool(true),
		}
		_, _, err := Prepare(context.Background(), nil, []*internalpb.FileResourceInfo{{Id: 1, Name: "resource", Path: "/resource.file"}})
		require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	})
}

// GlobalFunctionsSuite tests global functions
type GlobalFunctionsSuite struct {
	suite.Suite
	mockStorage *mocks.ChunkManager
}

func (suite *GlobalFunctionsSuite) SetupTest() {
	suite.mockStorage = mocks.NewChunkManager(suite.T())
	// Reset global state
	managerMu.Lock()
	GlobalFileManager = nil
	managerMu.Unlock()
	listenerMu.Lock()
	listeners = make(map[string]Listener)
	listenerMu.Unlock()
}

func (suite *GlobalFunctionsSuite) TestInitManager() {
	suite.NoError(InitManager(suite.mockStorage, SyncMode))

	suite.NotNil(GlobalFileManager)
	suite.Equal(SyncMode, GlobalFileManager.Mode())

	oldManager := GlobalFileManager
	suite.NoError(InitManager(suite.mockStorage, SyncMode))
	suite.Equal(oldManager, GlobalFileManager)

	err := InitManager(suite.mockStorage, RefMode)
	suite.Error(err)
	suite.Equal(oldManager, GlobalFileManager)
	suite.Equal(SyncMode, GlobalFileManager.Mode())
}

func (suite *GlobalFunctionsSuite) TestInitManagerConcurrentSameMode() {
	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			errs <- InitManager(nil, CloseMode)
		}()
	}
	wg.Wait()
	close(errs)

	for err := range errs {
		suite.NoError(err)
	}
	suite.NotNil(GlobalFileManager)
	suite.Equal(CloseMode, GlobalFileManager.Mode())
}

func (suite *GlobalFunctionsSuite) TestInitManagerConcurrentConflictingModes() {
	start := make(chan struct{})
	modes := []Mode{SyncMode, CloseMode}
	type result struct {
		mode Mode
		err  error
	}
	results := make(chan result, len(modes))
	var wg sync.WaitGroup
	wg.Add(len(modes))
	for _, mode := range modes {
		mode := mode
		go func() {
			defer wg.Done()
			<-start
			results <- result{mode: mode, err: InitManager(suite.mockStorage, mode)}
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	ownerMode := GlobalFileManager.Mode()
	for result := range results {
		if result.mode == ownerMode {
			suite.NoError(result.err)
		} else {
			suite.Error(result.err)
		}
	}
	suite.Equal(ownerMode, GlobalFileManager.Mode())
}

func (suite *GlobalFunctionsSuite) TestSync_NotInitialized() {
	GlobalFileManager = nil

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := Sync(context.Background(), 1, resources)
	suite.Error(err)
}

func (suite *GlobalFunctionsSuite) TestSync_Initialized() {
	suite.NoError(InitManager(suite.mockStorage, CloseMode))

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := Sync(context.Background(), 1, resources)
	suite.Error(err)
}

func TestGlobalFunctionsSuite(t *testing.T) {
	suite.Run(t, new(GlobalFunctionsSuite))
}
