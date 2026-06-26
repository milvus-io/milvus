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

package storage

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// HDFSChunkManagerSuite requires a real HDFS cluster.
// Set HDFS_ADDRESS (e.g. "localhost:9000") and HDFS_USER to enable.
// Tests are skipped when HDFS_ADDRESS is unset.
type HDFSChunkManagerSuite struct {
	suite.Suite
	cm       *HDFSChunkManager
	rootPath string
}

func (s *HDFSChunkManagerSuite) SetupSuite() {
	addr := os.Getenv("HDFS_ADDRESS")
	if addr == "" {
		s.T().Skip("HDFS_ADDRESS not set; skipping HDFS integration tests")
	}
	user := os.Getenv("HDFS_USER")
	if user == "" {
		user = "hdfs"
	}
	s.rootPath = "/milvus-test"

	var err error
	s.cm, err = NewHDFSChunkManager(addr, user, s.rootPath)
	require.NoError(s.T(), err)
}

func (s *HDFSChunkManagerSuite) TearDownSuite() {
	if s.cm != nil {
		_ = s.cm.RemoveWithPrefix(context.Background(), s.rootPath+"/")
	}
}

func (s *HDFSChunkManagerSuite) TestRootPath() {
	assert.Equal(s.T(), s.rootPath, s.cm.RootPath())
}

func (s *HDFSChunkManagerSuite) TestWriteRead() {
	ctx := context.Background()
	key := "test/write_read"
	data := []byte("hello hdfs")

	err := s.cm.Write(ctx, key, data)
	require.NoError(s.T(), err)

	got, err := s.cm.Read(ctx, key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), data, got)
}

func (s *HDFSChunkManagerSuite) TestExist() {
	ctx := context.Background()
	key := "test/exist_file"

	exists, err := s.cm.Exist(ctx, key)
	require.NoError(s.T(), err)
	assert.False(s.T(), exists)

	require.NoError(s.T(), s.cm.Write(ctx, key, []byte("x")))

	exists, err = s.cm.Exist(ctx, key)
	require.NoError(s.T(), err)
	assert.True(s.T(), exists)
}

func (s *HDFSChunkManagerSuite) TestSize() {
	ctx := context.Background()
	key := "test/size_file"
	data := []byte("hello")

	require.NoError(s.T(), s.cm.Write(ctx, key, data))
	size, err := s.cm.Size(ctx, key)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(len(data)), size)
}

func (s *HDFSChunkManagerSuite) TestMultiWriteMultiRead() {
	ctx := context.Background()
	contents := map[string][]byte{
		"test/multi/a": []byte("aaa"),
		"test/multi/b": []byte("bbb"),
		"test/multi/c": []byte("ccc"),
	}

	require.NoError(s.T(), s.cm.MultiWrite(ctx, contents))

	paths := []string{"test/multi/a", "test/multi/b", "test/multi/c"}
	results, err := s.cm.MultiRead(ctx, paths)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []byte("aaa"), results[0])
	assert.Equal(s.T(), []byte("bbb"), results[1])
	assert.Equal(s.T(), []byte("ccc"), results[2])
}

func (s *HDFSChunkManagerSuite) TestReadAt() {
	ctx := context.Background()
	key := "test/readat"
	data := []byte("0123456789")

	require.NoError(s.T(), s.cm.Write(ctx, key, data))

	got, err := s.cm.ReadAt(ctx, key, 3, 5)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []byte("34567"), got)
}

func (s *HDFSChunkManagerSuite) TestWalkWithPrefix() {
	ctx := context.Background()
	files := map[string][]byte{
		"test/walk/x":   []byte("x"),
		"test/walk/y":   []byte("y"),
		"test/walk/z/a": []byte("za"),
	}
	require.NoError(s.T(), s.cm.MultiWrite(ctx, files))

	var found []string
	err := s.cm.WalkWithPrefix(ctx, "test/walk/", true, func(info *ChunkObjectInfo) bool {
		found = append(found, info.FilePath)
		return true
	})
	require.NoError(s.T(), err)
	assert.Len(s.T(), found, 3)
}

func (s *HDFSChunkManagerSuite) TestRemove() {
	ctx := context.Background()
	key := "test/remove_me"
	require.NoError(s.T(), s.cm.Write(ctx, key, []byte("bye")))

	require.NoError(s.T(), s.cm.Remove(ctx, key))

	exists, err := s.cm.Exist(ctx, key)
	require.NoError(s.T(), err)
	assert.False(s.T(), exists)
}

func (s *HDFSChunkManagerSuite) TestRemoveWithPrefix() {
	ctx := context.Background()
	files := map[string][]byte{
		"test/rprefix/p1": []byte("1"),
		"test/rprefix/p2": []byte("2"),
	}
	require.NoError(s.T(), s.cm.MultiWrite(ctx, files))

	require.NoError(s.T(), s.cm.RemoveWithPrefix(ctx, "test/rprefix/"))

	for k := range files {
		exists, err := s.cm.Exist(ctx, k)
		require.NoError(s.T(), err)
		assert.False(s.T(), exists)
	}
}

func (s *HDFSChunkManagerSuite) TestCopy() {
	ctx := context.Background()
	src := "test/copy_src"
	dst := "test/copy_dst"
	data := []byte("copy me")

	require.NoError(s.T(), s.cm.Write(ctx, src, data))
	require.NoError(s.T(), s.cm.Copy(ctx, src, dst))

	got, err := s.cm.Read(ctx, dst)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), data, got)
}

func (s *HDFSChunkManagerSuite) TestMmapUnsupported() {
	ctx := context.Background()
	_, err := s.cm.Mmap(ctx, "any")
	assert.Error(s.T(), err)
}

func TestHDFSChunkManagerSuite(t *testing.T) {
	suite.Run(t, new(HDFSChunkManagerSuite))
}
