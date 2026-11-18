// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//     http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package localstorage

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// requireNoTemp asserts that no unpublished write remains in dir.
func requireNoTemp(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, IsTemp(entry.Name()), "leftover %s", entry.Name())
	}
}

func requireContent(t *testing.T, name, expected string) {
	t.Helper()
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, expected, string(data))
}

func TestAtomicReplace(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "manifest")
	require.NoError(t, WriteFileAtomic(name, []byte("old"), 0o640))
	before, err := os.Stat(name)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), before.Mode().Perm())
	reader, err := os.Open(name)
	require.NoError(t, err)
	defer reader.Close()

	require.NoError(t, WriteFileAtomic(name, []byte("new"), 0o600))
	requireContent(t, name, "new")
	after, err := os.Stat(name)
	require.NoError(t, err)
	require.False(t, os.SameFile(before, after), "publish a new file, as an object store PUT does")
	require.Equal(t, os.FileMode(0o600), after.Mode().Perm())
	// A reader of the previous file keeps its content.
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "old", string(data))
	requireNoTemp(t, dir)
}

func TestFailedWriteKeepsPreviousContent(t *testing.T) {
	failures := map[string]func() *mockey.Mocker{
		"write": func() *mockey.Mocker {
			return mockey.Mock((*os.File).Write).Return(0, syscall.ENOSPC).Build()
		},
		"sync": func() *mockey.Mocker {
			return mockey.Mock((*os.File).Sync).Return(syscall.EIO).Build()
		},
		"close": func() *mockey.Mocker {
			var original func(*os.File) error
			return mockey.Mock((*os.File).Close).Origin(&original).To(func(f *os.File) error {
				return errors.CombineErrors(original(f), syscall.EIO)
			}).Build()
		},
		"rename": func() *mockey.Mocker {
			return mockey.Mock(os.Rename).Return(syscall.EIO).Build()
		},
	}
	for step, inject := range failures {
		t.Run(step, func(t *testing.T) {
			dir := t.TempDir()
			name := filepath.Join(dir, "manifest")
			require.NoError(t, WriteFileAtomic(name, []byte("previous"), 0o600))
			patch := inject()
			err := WriteFileAtomic(name, []byte("next"), 0o600)
			patch.UnPatch()
			require.Error(t, err)
			requireContent(t, name, "previous")
			requireNoTemp(t, dir)
		})
	}
}

func TestFailedFirstWritePublishesNothing(t *testing.T) {
	dir := t.TempDir()
	name := filepath.Join(dir, "manifest")
	patch := mockey.Mock((*os.File).Sync).Return(syscall.EIO).Build()
	err := WriteFileAtomic(name, []byte("data"), 0o600)
	patch.UnPatch()
	require.ErrorIs(t, err, syscall.EIO)
	_, err = os.Stat(name)
	require.True(t, os.IsNotExist(err))
	requireNoTemp(t, dir)
}

func TestWriteOntoDirectoryFails(t *testing.T) {
	parent := t.TempDir()
	target := filepath.Join(parent, "dir")
	require.NoError(t, os.Mkdir(target, 0o700))
	require.Error(t, WriteFileAtomic(target, []byte("data"), 0o600))
	requireNoTemp(t, parent)
	require.Error(t, WriteFileAtomic(filepath.Join(parent, "missing", "object"), []byte("data"), 0o600))
}

func TestConcurrentReadersSeeCompleteContent(t *testing.T) {
	name := filepath.Join(t.TempDir(), "manifest")
	contents := []string{strings.Repeat("a", 1<<16), strings.Repeat("b", 1<<16)}
	require.NoError(t, WriteFileAtomic(name, []byte(contents[0]), 0o600))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			assert.NoError(t, WriteFileAtomic(name, []byte(contents[i%2]), 0o600))
		}
	}()
	for i := 0; i < 200; i++ {
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Contains(t, contents, string(data))
	}
	wg.Wait()
}

func TestTempFileIsCreatedInResolvedParent(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(filepath.Join(root, "actual", "child"), 0o700))
	require.NoError(t, os.Symlink(filepath.Join(root, "actual", "child"), filepath.Join(root, "link")))
	// The kernel resolves link/.. to actual, not root.
	name := filepath.Join(root, "link") + "/../object"
	require.NoError(t, WriteFileAtomic(name, []byte("data"), 0o600))
	requireContent(t, filepath.Join(root, "actual", "object"), "data")
	_, err := os.Stat(filepath.Join(root, "object"))
	require.True(t, os.IsNotExist(err))
}

func TestIsTemp(t *testing.T) {
	require.True(t, IsTemp("/root/dir/.milvus-tmp-0123456789abcdef"))
	require.True(t, IsTemp(".milvus-tmp-x"))
	require.False(t, IsTemp("/root/.milvus-tmp-dir/object"))
	require.False(t, IsTemp("/root/dir/object"))
}
