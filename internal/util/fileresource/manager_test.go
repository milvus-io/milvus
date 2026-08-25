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

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"
	"go.uber.org/atomic"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/util/analyzer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/conc"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	suite.NoError(err)
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
	// Create temporary directory for tests
	var err error
	suite.tempDir, err = os.MkdirTemp(os.TempDir(), "fileresource_test_sync_*")
	suite.NoError(err)

	suite.mockStorage = mocks.NewChunkManager(suite.T())
	suite.manager = &SyncManager{
		BaseManager: BaseManager{localPath: suite.tempDir},
		downloader:  suite.mockStorage,
		version:     atomic.NewUint64(0),
		resourceMap: make(map[string]int64),
	}
}

func (suite *SyncManagerSuite) TearDownTest() {
	if suite.tempDir != "" {
		os.RemoveAll(suite.tempDir)
	}
}

func (suite *SyncManagerSuite) TestSync_Success() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test1.file", Path: "/storage/test1.file"},
		{Id: 2, Name: "test2.file", Path: "/storage/test2.file"},
	}

	// Mock the Size and Reader calls
	suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/test1.file").Return(int64(len("test content 1")), nil)
	suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/test2.file").Return(int64(len("test content 2")), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test1.file").Return(newMockReader("test content 1"), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test2.file").Return(newMockReader("test content 2"), nil)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.NoError(err)

	// Verify files were created directly in their resource directories.
	file1Path := path.Join(suite.tempDir, "1", "test1.file")
	file2Path := path.Join(suite.tempDir, "2", "test2.file")

	suite.FileExists(file1Path)
	suite.FileExists(file2Path)
	suite.NoDirExists(path.Join(suite.tempDir, "1.downloading-1"))
	suite.NoDirExists(path.Join(suite.tempDir, "2.downloading-1"))

	// Verify content
	content1, err := os.ReadFile(file1Path)
	suite.NoError(err)
	suite.Equal("test content 1", string(content1))

	content2, err := os.ReadFile(file2Path)
	suite.NoError(err)
	suite.Equal("test content 2", string(content2))
}

func (suite *SyncManagerSuite) TestSync_RestoreLocalResourcesAfterRestart() {
	activeDir := path.Join(suite.tempDir, "1")
	orphanDir := path.Join(suite.tempDir, "2")
	suite.Require().NoError(os.MkdirAll(activeDir, os.ModePerm))
	suite.Require().NoError(os.MkdirAll(orphanDir, os.ModePerm))
	suite.Require().NoError(os.WriteFile(path.Join(activeDir, "active.file"), []byte("active content"), 0o600))
	suite.Require().NoError(os.WriteFile(path.Join(orphanDir, "orphan.file"), []byte("orphan content"), 0o600))

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "active", Path: "/storage/active.file"},
	}
	suite.Require().NoError(suite.manager.Sync(context.Background(), 3, resources))

	suite.Equal(uint64(3), suite.manager.GetVersion())
	suite.Equal(map[string]int64{"active": 1}, suite.manager.resourceMap)
	suite.Equal(map[int64]struct{}{1: {}}, suite.manager.localResourceIDs)
	suite.FileExists(path.Join(activeDir, "active.file"))
	suite.NoDirExists(orphanDir)
}

func (suite *SyncManagerSuite) TestSync_ClearRestoredResourcesWithEmptyList() {
	orphanDir := path.Join(suite.tempDir, "1")
	suite.Require().NoError(os.MkdirAll(orphanDir, os.ModePerm))
	suite.Require().NoError(os.WriteFile(path.Join(orphanDir, "orphan.file"), []byte("orphan content"), 0o600))

	suite.Require().NoError(suite.manager.Sync(context.Background(), 2, nil))

	suite.Equal(uint64(2), suite.manager.GetVersion())
	suite.Empty(suite.manager.localResourceIDs)
	suite.NoDirExists(orphanDir)
}

func (suite *SyncManagerSuite) TestSync_LoadLocalResourcesFailure() {
	filePath := path.Join(suite.tempDir, "not-a-directory")
	suite.Require().NoError(os.WriteFile(filePath, []byte("content"), 0o600))
	suite.manager.localPath = filePath

	err := suite.manager.Sync(context.Background(), 1, nil)
	suite.ErrorIs(err, merr.ErrIoFailed)
	suite.Equal(uint64(0), suite.manager.GetVersion())
}

func (suite *SyncManagerSuite) TestSync_LargeFile() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "large.file", Path: "/storage/large.file"},
	}
	content := strings.Repeat("x", 2048)
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[0].GetPath()).Return(int64(len(content)), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader(content), nil)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.NoError(err)
	suite.Equal(uint64(1), suite.manager.GetVersion())
	filePath := path.Join(suite.tempDir, "1", "large.file")
	suite.FileExists(filePath)
	info, err := os.Stat(filePath)
	suite.NoError(err)
	suite.Equal(int64(len(content)), info.Size())
}

func (suite *SyncManagerSuite) TestSync_TimeoutUsesLatestConfig() {
	params := paramtable.Get()
	key := params.CommonCfg.FileResourceDownloadTimeout.Key
	suite.Require().NoError(params.Save(key, "1s"))
	suite.T().Cleanup(func() {
		suite.NoError(params.Reset(key))
	})

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "slow.file", Path: "/storage/slow.file"},
	}
	suite.Require().NoError(params.Save(key, "10ms"))
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[0].GetPath()).RunAndReturn(func(ctx context.Context, _ string) (int64, error) {
		<-ctx.Done()
		return 0, ctx.Err()
	})

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.ErrorIs(err, context.DeadlineExceeded)
	suite.Equal(uint64(0), suite.manager.GetVersion())
	suite.NoFileExists(path.Join(suite.tempDir, "1", "slow.file"))
}

func (suite *SyncManagerSuite) TestSync_ReaderError() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/nonexistent.file"},
	}

	// Mock reader to return error
	suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/nonexistent.file").Return(int64(1), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/nonexistent.file").Return(nil, io.ErrUnexpectedEOF)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.Error(err)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
}

func (suite *SyncManagerSuite) TestSync_NotifyListener() {
	listener := &mockFileResourceListener{}
	RegisterListener("test", listener)
	defer UnregisterListener("test")

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/test.file"},
	}
	suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/test.file").Return(int64(len("test content")), nil)
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
	suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/test.file").Return(int64(len("test content")), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test.file").Return(newMockReader("test content"), nil)
	suite.Require().NoError(suite.manager.Sync(context.Background(), 1, resources))

	updated := []*internalpb.FileResourceInfo{
		{Id: 2, Name: "test.file", Path: "/storage/test_v2.file"},
	}
	suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/test_v2.file").Return(int64(len("test content v2")), nil)
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

func (suite *SyncManagerSuite) TestSync_MultiResourceFailureDoesNotPublishVersion() {
	listener := &mockFileResourceListener{}
	RegisterListener("atomic", listener)
	defer UnregisterListener("atomic")

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "first", Path: "/storage/first.file"},
		{Id: 2, Name: "second", Path: "/storage/second.file"},
	}
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[0].GetPath()).Return(int64(5), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader("first"), nil)
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[1].GetPath()).Return(int64(1), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[1].GetPath()).Return(nil, io.ErrUnexpectedEOF)

	err := suite.manager.Sync(context.Background(), 1, resources)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
	suite.Equal(uint64(0), suite.manager.GetVersion())
	suite.Empty(listener.events)
	suite.FileExists(path.Join(suite.tempDir, "1", "first.file"))
	suite.NoFileExists(path.Join(suite.tempDir, "2", "second.file"))
	suite.NoDirExists(path.Join(suite.tempDir, "1.downloading-1"))
	suite.NoDirExists(path.Join(suite.tempDir, "2.downloading-1"))
}

func (suite *SyncManagerSuite) TestSync_AnalyzerUpdateAfterFileActivation() {
	mockey.PatchConvey("analyzer update runs after file activation", suite.T(), func() {
		resources := []*internalpb.FileResourceInfo{
			{Id: 1, Name: "test.file", Path: "/storage/test.file"},
		}
		suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/test.file").Return(int64(len("test content")), nil)
		suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/test.file").Return(newMockReader("test content"), nil)
		mockey.Mock(analyzer.UpdateGlobalResourceInfo).To(func(resourceMap map[string]int64) error {
			suite.Equal(map[string]int64{"test.file": 1}, resourceMap)
			content, err := os.ReadFile(path.Join(suite.tempDir, "1", "test.file"))
			suite.Require().NoError(err)
			suite.Equal("test content", string(content))
			return nil
		}).Build()

		suite.Require().NoError(suite.manager.Sync(context.Background(), 1, resources))
		suite.Equal(uint64(1), suite.manager.GetVersion())
	})
}

func (suite *SyncManagerSuite) TestSync_AnalyzerUpdateFailureDoesNotAdvanceVersion() {
	mockey.PatchConvey("failed analyzer update keeps previous state retryable", suite.T(), func() {
		oldResourcePath := path.Join(suite.tempDir, "1")
		suite.Require().NoError(os.MkdirAll(oldResourcePath, os.ModePerm))
		suite.Require().NoError(os.WriteFile(path.Join(oldResourcePath, "old.file"), []byte("old content"), 0o600))
		suite.manager.resourceMap = map[string]int64{"test.file": 1}
		suite.manager.version.Store(1)

		listener := &mockFileResourceListener{}
		RegisterListener("analyzer-failure", listener)
		defer UnregisterListener("analyzer-failure")

		expectedErr := errors.New("mock analyzer update failed")
		mocker := mockey.Mock(analyzer.UpdateGlobalResourceInfo).Return(expectedErr).Build()
		resources := []*internalpb.FileResourceInfo{
			{Id: 2, Name: "test.file", Path: "/storage/new.file"},
		}
		suite.mockStorage.EXPECT().Size(mock.Anything, "/storage/new.file").Return(int64(len("new content")), nil).Once()
		suite.mockStorage.EXPECT().Reader(mock.Anything, "/storage/new.file").Return(newMockReader("new content"), nil).Once()

		err := suite.manager.Sync(context.Background(), 2, resources)
		suite.ErrorIs(err, expectedErr)
		suite.Equal(uint64(1), suite.manager.GetVersion())
		suite.Equal(map[string]int64{"test.file": 1}, suite.manager.resourceMap)
		suite.Empty(listener.events)
		suite.FileExists(path.Join(suite.tempDir, "1", "old.file"))
		suite.FileExists(path.Join(suite.tempDir, "2", "new.file"))

		// The same version retries the Analyzer update and reuses the complete
		// unpublished file downloaded by the previous attempt.
		mocker.Return(nil)

		suite.Require().NoError(suite.manager.Sync(context.Background(), 2, resources))
		suite.Equal(uint64(2), suite.manager.GetVersion())
		suite.Equal(map[string]int64{"test.file": 2}, suite.manager.resourceMap)
		suite.Require().Len(listener.events, 1)
		suite.NoDirExists(path.Join(suite.tempDir, "1"))
		content, err := os.ReadFile(path.Join(suite.tempDir, "2", "new.file"))
		suite.NoError(err)
		suite.Equal("new content", string(content))
	})
}

func (suite *SyncManagerSuite) TestMode() {
	mode := suite.manager.Mode()
	suite.Equal(SyncMode, mode)
}

func TestSyncManagerSuite(t *testing.T) {
	suite.Run(t, new(SyncManagerSuite))
}

type mockFileResourceListener struct {
	events []SyncEvent
	err    error
}

func (m *mockFileResourceListener) OnFileResourceSync(event SyncEvent) error {
	m.events = append(m.events, event)
	return m.err
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
}

func (suite *RefManagerSuite) TearDownTest() {
	if suite.tempDir != "" {
		os.RemoveAll(suite.tempDir)
	}
}

func (suite *RefManagerSuite) TestDownload_LargeFile() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "large", Path: "/storage/large.file"},
	}
	content := strings.Repeat("x", 2048)

	// Set up mock
	suite.mockStorage.EXPECT().RootPath().Return("/test/storage")
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[0].GetPath()).Return(int64(len(content)), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader(content), nil)

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

	downloaded, err := os.ReadFile(filePath)
	suite.NoError(err)
	suite.Equal(content, string(downloaded))

	// release and clean all file
	suite.manager.Release(resources...)
	suite.manager.CleanResource()
	suite.NoFileExists(filePath)
}

func (suite *RefManagerSuite) TestDownloadErrorRollsBackReferences() {
	const rootPath = "/test/storage"
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test1", Path: "/storage/test1.file"},
		{Id: 2, Name: "test2", Path: "/storage/test2.file"},
	}

	suite.mockStorage.EXPECT().RootPath().Return(rootPath)
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[0].GetPath()).Return(int64(len("test content")), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[0].GetPath()).Return(newMockReader("test content"), nil)
	suite.mockStorage.EXPECT().Size(mock.Anything, resources[1].GetPath()).Return(int64(1), nil)
	suite.mockStorage.EXPECT().Reader(mock.Anything, resources[1].GetPath()).Return(nil, io.ErrUnexpectedEOF)

	err := suite.manager.Download(context.Background(), suite.mockStorage, resources...)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)

	firstKey := fmt.Sprintf("%s/%d", rootPath, resources[0].GetId())
	secondKey := fmt.Sprintf("%s/%d", rootPath, resources[1].GetId())
	suite.Equal(0, suite.manager.ref[firstKey])
	suite.Equal(0, suite.manager.ref[secondKey])

	firstFilePath := path.Join(suite.tempDir, firstKey, path.Base(resources[0].GetPath()))
	suite.FileExists(firstFilePath)

	suite.manager.CleanResource()
	suite.NotContains(suite.manager.ref, firstKey)
	suite.NotContains(suite.manager.ref, secondKey)
	suite.NoFileExists(firstFilePath)
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

// GlobalFunctionsSuite tests global functions
type GlobalFunctionsSuite struct {
	suite.Suite
	mockStorage *mocks.ChunkManager
}

func (suite *GlobalFunctionsSuite) SetupTest() {
	suite.mockStorage = mocks.NewChunkManager(suite.T())
	// Reset global state
	GlobalFileManager = nil
	once = sync.Once{}
	listeners = make(map[string]Listener)
}

func (suite *GlobalFunctionsSuite) TestInitManager() {
	InitManager(suite.mockStorage, SyncMode)

	suite.NotNil(GlobalFileManager)
	suite.Equal(SyncMode, GlobalFileManager.Mode())

	oldManager := GlobalFileManager
	InitManager(suite.mockStorage, RefMode)
	suite.Same(oldManager, GlobalFileManager)
	suite.Equal(SyncMode, GlobalFileManager.Mode())
}

func (suite *GlobalFunctionsSuite) TestResolveMode() {
	suite.Equal(CloseMode, ResolveMode())
	suite.Equal(RefMode, ResolveMode(CloseMode, RefMode))
	suite.Equal(SyncMode, ResolveMode(RefMode, SyncMode, CloseMode))
}

func (suite *GlobalFunctionsSuite) TestSync_NotInitialized() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := Sync(context.Background(), 1, resources)
	suite.NoError(err)
}

func (suite *GlobalFunctionsSuite) TestSync_Initialized() {
	InitManager(suite.mockStorage, CloseMode)

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := Sync(context.Background(), 1, resources)
	suite.NoError(err)
}

func TestGlobalFunctionsSuite(t *testing.T) {
	suite.Run(t, new(GlobalFunctionsSuite))
}
