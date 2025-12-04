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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/conc"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
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

	err := suite.manager.Sync(resources)
	suite.NoError(err)
}

func (suite *BaseManagerSuite) TestDownload() {
	mockStorage := mocks.NewChunkManager(suite.T())
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := suite.manager.Download(mockStorage, resources...)
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
		resourceSet: make(map[int64]struct{}),
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

	// Mock the Reader calls
	suite.mockStorage.EXPECT().Reader(context.Background(), "/storage/test1.file").Return(newMockReader("test content 1"), nil)
	suite.mockStorage.EXPECT().Reader(context.Background(), "/storage/test2.file").Return(newMockReader("test content 2"), nil)

	err := suite.manager.Sync(resources)
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

func (suite *SyncManagerSuite) TestSync_ReaderError() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/storage/nonexistent.file"},
	}

	// Mock reader to return error
	suite.mockStorage.EXPECT().Reader(context.Background(), "/storage/nonexistent.file").Return(nil, io.ErrUnexpectedEOF)

	err := suite.manager.Sync(resources)
	suite.Error(err)
	suite.ErrorIs(err, io.ErrUnexpectedEOF)
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
}

func (suite *RefManagerSuite) TearDownTest() {
	if suite.tempDir != "" {
		os.RemoveAll(suite.tempDir)
	}
}

func (suite *RefManagerSuite) TestNormal() {
	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test", Path: "/storage/test.file"},
	}

	// Set up mock
	suite.mockStorage.EXPECT().RootPath().Return("/test/storage")
	suite.mockStorage.EXPECT().Reader(context.Background(), "/storage/test.file").Return(newMockReader("test content"), nil)

	err := suite.manager.Download(suite.mockStorage, resources...)
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
}

func (suite *GlobalFunctionsSuite) TestInitManager() {
	InitManager(suite.mockStorage, SyncMode)

	suite.NotNil(GlobalFileManager)
	suite.Equal(SyncMode, GlobalFileManager.Mode())

	// Test that calling InitManager again doesn't change the manager
	oldManager := GlobalFileManager
	InitManager(suite.mockStorage, RefMode)
	suite.Equal(oldManager, GlobalFileManager)
	suite.Equal(SyncMode, GlobalFileManager.Mode()) // Should still be SyncMode
}

func (suite *GlobalFunctionsSuite) TestSync_NotInitialized() {
	GlobalFileManager = nil

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := Sync(resources)
	suite.NoError(err) // Should not error when not initialized
}

func (suite *GlobalFunctionsSuite) TestSync_Initialized() {
	InitManager(suite.mockStorage, CloseMode)

	resources := []*internalpb.FileResourceInfo{
		{Id: 1, Name: "test.file", Path: "/test/test.file"},
	}

	err := Sync(resources)
	suite.NoError(err)
}

func TestGlobalFunctionsSuite(t *testing.T) {
	suite.Run(t, new(GlobalFunctionsSuite))
}
