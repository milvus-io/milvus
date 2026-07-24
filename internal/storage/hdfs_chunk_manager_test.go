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
	"io"
	"os"
	"path"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// newHDFSChunkManagerForTest constructs an HDFSChunkManager from environment
// variables, or skips the test when HDFS_ADDRESS is not set.
//
//	HDFS_ADDRESS  — NameNode address, e.g. "localhost:9000"  (required to run)
//	HDFS_USER     — HDFS user for simple auth (default: "hdfs")
func newHDFSChunkManagerForTest(t *testing.T, rootPath string) *HDFSChunkManager {
	t.Helper()
	addr := os.Getenv("HDFS_ADDRESS")
	if addr == "" {
		t.Skip("HDFS_ADDRESS not set; skipping HDFS tests. " +
			"Start a local HDFS cluster and set HDFS_ADDRESS=host:9000 to run them.")
	}
	user := os.Getenv("HDFS_USER")
	if user == "" {
		user = "hdfs"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cm, err := NewHDFSChunkManager(ctx, addr, user, rootPath)
	require.NoError(t, err)
	return cm
}

// HDFSChunkManagerSuite runs against a real HDFS cluster.
// Tests are skipped automatically when HDFS_ADDRESS is not set.
type HDFSChunkManagerSuite struct {
	suite.Suite
	cm       *HDFSChunkManager
	rootPath string
}

func (s *HDFSChunkManagerSuite) SetupSuite() {
	s.rootPath = "/milvus-test"
	s.cm = newHDFSChunkManagerForTest(s.T(), s.rootPath)
}

func (s *HDFSChunkManagerSuite) TearDownSuite() {
	if s.cm != nil {
		_ = s.cm.RemoveWithPrefix(context.Background(), s.rootPath+"/")
	}
}

func (s *HDFSChunkManagerSuite) key(parts ...string) string {
	return path.Join(append([]string{s.rootPath}, parts...)...)
}

func (s *HDFSChunkManagerSuite) TestRootPath() {
	assert.Equal(s.T(), s.rootPath, s.cm.RootPath())
}

func (s *HDFSChunkManagerSuite) TestWriteRead() {
	ctx := context.Background()
	k := s.key("write_read")
	data := []byte("hello hdfs")

	require.NoError(s.T(), s.cm.Write(ctx, k, data))
	got, err := s.cm.Read(ctx, k)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), data, got)
}

func (s *HDFSChunkManagerSuite) TestWriteIsAtomic() {
	// A second Write to the same key must overwrite the first completely.
	ctx := context.Background()
	k := s.key("atomic")
	require.NoError(s.T(), s.cm.Write(ctx, k, []byte("v1")))
	require.NoError(s.T(), s.cm.Write(ctx, k, []byte("v2")))
	got, err := s.cm.Read(ctx, k)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []byte("v2"), got)
}

func (s *HDFSChunkManagerSuite) TestExist() {
	ctx := context.Background()
	k := s.key("exist_file")

	exists, err := s.cm.Exist(ctx, k)
	require.NoError(s.T(), err)
	assert.False(s.T(), exists)

	require.NoError(s.T(), s.cm.Write(ctx, k, []byte("x")))
	exists, err = s.cm.Exist(ctx, k)
	require.NoError(s.T(), err)
	assert.True(s.T(), exists)
}

func (s *HDFSChunkManagerSuite) TestSize() {
	ctx := context.Background()
	k := s.key("size_file")
	data := []byte("hello")
	require.NoError(s.T(), s.cm.Write(ctx, k, data))
	size, err := s.cm.Size(ctx, k)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(len(data)), size)
}

func (s *HDFSChunkManagerSuite) TestMultiWriteMultiRead() {
	ctx := context.Background()
	ka, kb, kc := s.key("multi/a"), s.key("multi/b"), s.key("multi/c")
	contents := map[string][]byte{ka: []byte("aaa"), kb: []byte("bbb"), kc: []byte("ccc")}
	require.NoError(s.T(), s.cm.MultiWrite(ctx, contents))

	results, err := s.cm.MultiRead(ctx, []string{ka, kb, kc})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []byte("aaa"), results[0])
	assert.Equal(s.T(), []byte("bbb"), results[1])
	assert.Equal(s.T(), []byte("ccc"), results[2])
}

func (s *HDFSChunkManagerSuite) TestReadAt() {
	ctx := context.Background()
	k := s.key("readat")
	require.NoError(s.T(), s.cm.Write(ctx, k, []byte("0123456789")))

	got, err := s.cm.ReadAt(ctx, k, 3, 5)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []byte("34567"), got)
}

func (s *HDFSChunkManagerSuite) TestReadAtPastEOF() {
	// ReadAt must NOT zero-pad — returned slice contains only bytes that exist.
	ctx := context.Background()
	k := s.key("readat_eof")
	require.NoError(s.T(), s.cm.Write(ctx, k, []byte("abcde"))) // 5 bytes

	got, err := s.cm.ReadAt(ctx, k, 3, 100) // request 100 bytes from offset 3
	require.NoError(s.T(), err)
	assert.Equal(s.T(), []byte("de"), got) // only 2 bytes available
}

func (s *HDFSChunkManagerSuite) TestWalkRecursive() {
	ctx := context.Background()
	require.NoError(s.T(), s.cm.MultiWrite(ctx, map[string][]byte{
		s.key("walk/445/a"): []byte("a"),
		s.key("walk/445/b"): []byte("b"),
		s.key("walk/446/c"): []byte("c"),
	}))

	var found []string
	err := s.cm.WalkWithPrefix(ctx, s.key("walk/445"), true, func(info *ChunkObjectInfo) bool {
		found = append(found, info.FilePath)
		return true
	})
	require.NoError(s.T(), err)
	sort.Strings(found)
	assert.Equal(s.T(), []string{s.key("walk/445/a"), s.key("walk/445/b")}, found)
}

func (s *HDFSChunkManagerSuite) TestWalkNonRecursiveYieldsDirs() {
	// Non-recursive walk must yield directory entries (common prefixes) so that
	// callers like datacoord/import_util.go can enumerate segment sub-IDs.
	ctx := context.Background()
	require.NoError(s.T(), s.cm.MultiWrite(ctx, map[string][]byte{
		s.key("import/445/binlog"): []byte("x"),
		s.key("import/446/binlog"): []byte("y"),
	}))

	var found []string
	err := s.cm.WalkWithPrefix(ctx, s.key("import/"), false, func(info *ChunkObjectInfo) bool {
		found = append(found, info.FilePath)
		return true
	})
	require.NoError(s.T(), err)
	sort.Strings(found)
	assert.Equal(s.T(), []string{s.key("import/445"), s.key("import/446")}, found)
}

func (s *HDFSChunkManagerSuite) TestWalkStopSignalPropagates() {
	// walkFunc returning false must halt the walk immediately at every depth.
	ctx := context.Background()
	for _, k := range []string{"stop/1", "stop/2", "stop/3"} {
		require.NoError(s.T(), s.cm.Write(ctx, s.key(k), []byte(k)))
	}

	count := 0
	err := s.cm.WalkWithPrefix(ctx, s.key("stop/"), true, func(_ *ChunkObjectInfo) bool {
		count++
		return false
	})
	require.NoError(s.T(), err)
	assert.Equal(s.T(), 1, count)
}

func (s *HDFSChunkManagerSuite) TestRemove() {
	ctx := context.Background()
	k := s.key("remove_me")
	require.NoError(s.T(), s.cm.Write(ctx, k, []byte("bye")))
	require.NoError(s.T(), s.cm.Remove(ctx, k))

	exists, err := s.cm.Exist(ctx, k)
	require.NoError(s.T(), err)
	assert.False(s.T(), exists)
}

func (s *HDFSChunkManagerSuite) TestRemoveWithPrefix() {
	ctx := context.Background()
	files := map[string][]byte{
		s.key("rprefix/p1"): []byte("1"),
		s.key("rprefix/p2"): []byte("2"),
	}
	require.NoError(s.T(), s.cm.MultiWrite(ctx, files))
	require.NoError(s.T(), s.cm.RemoveWithPrefix(ctx, s.key("rprefix/")))

	for k := range files {
		exists, err := s.cm.Exist(ctx, k)
		require.NoError(s.T(), err)
		assert.False(s.T(), exists, "expected %s to be removed", k)
	}
}

func (s *HDFSChunkManagerSuite) TestRemoveWithPrefixEmptyGuard() {
	err := s.cm.RemoveWithPrefix(context.Background(), "")
	assert.Error(s.T(), err)
}

func (s *HDFSChunkManagerSuite) TestCopy() {
	ctx := context.Background()
	src := s.key("copy_src")
	dst := s.key("copy_dst")
	data := []byte("copy me")
	require.NoError(s.T(), s.cm.Write(ctx, src, data))
	require.NoError(s.T(), s.cm.Copy(ctx, src, dst))

	got, err := s.cm.Read(ctx, dst)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), data, got)
}

func (s *HDFSChunkManagerSuite) TestReader() {
	ctx := context.Background()
	k := s.key("reader_file")
	data := []byte("stream me")
	require.NoError(s.T(), s.cm.Write(ctx, k, data))

	r, err := s.cm.Reader(ctx, k)
	require.NoError(s.T(), err)
	defer r.Close()

	got, err := io.ReadAll(r)
	require.NoError(s.T(), err)
	assert.Equal(s.T(), data, got)

	size, err := r.Size()
	require.NoError(s.T(), err)
	assert.Equal(s.T(), int64(len(data)), size)
}

func (s *HDFSChunkManagerSuite) TestMmapUnsupported() {
	_, err := s.cm.Mmap(context.Background(), "any")
	assert.Error(s.T(), err)
}

func TestHDFSChunkManagerSuite(t *testing.T) {
	suite.Run(t, new(HDFSChunkManagerSuite))
}
