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

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// prepare returns the boundary an owner prepares once for its root.
func prepare(t *testing.T, root string) string {
	t.Helper()
	boundary, err := PrepareRoot(root)
	require.NoError(t, err)
	return boundary
}

// requireNoTemp asserts that no unpublished write remains in dir.
func requireNoTemp(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, entry := range entries {
		require.False(t, IsTemp(entry.Name()), "leftover %s", entry.Name())
	}
}

func TestAtomicReplaceAndRetry(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "parents", "data")
	name := filepath.Join(dir, "object")
	require.NoError(t, WriteFile(name, []byte("old"), 0o640, ""))
	before, err := os.Stat(name)
	require.NoError(t, err)
	require.Equal(t, os.FileMode(0o640), before.Mode().Perm())
	reader, err := os.Open(name)
	require.NoError(t, err)
	defer reader.Close()

	require.NoError(t, Write(name, 0o600, func(w io.Writer) error {
		_, err := w.Write([]byte("new"))
		return err
	}, ""))
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
	after, err := os.Stat(name)
	require.NoError(t, err)
	require.False(t, os.SameFile(before, after), "publish a new file, as an object store PUT does")
	require.Equal(t, os.FileMode(0o600), after.Mode().Perm())
	// A reader of the previous file keeps its content.
	data, err = io.ReadAll(reader)
	require.NoError(t, err)
	require.Equal(t, "old", string(data))

	err = Write(name, 0o640, func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))
		return syscall.EIO
	}, "")
	require.ErrorIs(t, err, syscall.EIO)
	// A failed write leaves the previous content and no temporary file.
	data, err = os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
	requireNoTemp(t, dir)
	require.NoError(t, WriteFile(name, []byte("retry"), 0o600, ""))
	data, err = os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "retry", string(data))
	requireNoTemp(t, dir)
}

func TestConcurrentReadersSeeCompleteContent(t *testing.T) {
	name := filepath.Join(t.TempDir(), "object")
	contents := []string{strings.Repeat("a", 1<<16), strings.Repeat("b", 1<<16)}
	require.NoError(t, WriteFile(name, []byte(contents[0]), 0o600, ""))
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			assert.NoError(t, WriteFile(name, []byte(contents[i%2]), 0o600, ""))
		}
	}()
	for i := 0; i < 200; i++ {
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Contains(t, contents, string(data))
	}
	wg.Wait()
}

func TestIsTemp(t *testing.T) {
	require.True(t, IsTemp("/root/dir/.milvus-tmp-0123456789abcdef"))
	require.True(t, IsTemp(".milvus-tmp-x"))
	require.False(t, IsTemp("/root/.milvus-tmp-dir/object"))
	require.False(t, IsTemp("/root/dir/object"))
}

func TestFileSyncFailureCannotReturnSuccess(t *testing.T) {
	name := filepath.Join(t.TempDir(), "object")
	patch := mockey.Mock((*os.File).Sync).Return(syscall.EIO).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, WriteFile(name, []byte("data"), 0o600, ""), syscall.EIO)
	// Nothing unsynced is published.
	_, err := os.Stat(name)
	require.True(t, os.IsNotExist(err))
	requireNoTemp(t, filepath.Dir(name))
}

func TestDirectorySyncFailureIsNotAcknowledged(t *testing.T) {
	root := t.TempDir()
	boundary := prepare(t, root)
	name := filepath.Join(root, "new", "parents", "object")
	patch := mockey.Mock(syncDirectory).Return(syscall.EIO).Build()
	err := WriteFile(name, []byte("complete"), 0o600, boundary)
	patch.UnPatch()
	require.ErrorIs(t, err, syscall.EIO)
	require.NoError(t, WriteFile(name, []byte("retry"), 0o600, boundary))
}

func TestCloseAndOpenErrors(t *testing.T) {
	t.Run("close", func(t *testing.T) {
		name := filepath.Join(t.TempDir(), "object")
		var original func(*os.File) error
		patch := mockey.Mock((*os.File).Close).Origin(&original).To(func(f *os.File) error {
			return errors.CombineErrors(original(f), syscall.EIO)
		}).Build()
		err := WriteFile(name, []byte("data"), 0o600, "")
		patch.UnPatch()
		require.ErrorIs(t, err, syscall.EIO)
		_, err = os.Stat(name)
		require.True(t, os.IsNotExist(err))
		requireNoTemp(t, filepath.Dir(name))
	})
	t.Run("directory", func(t *testing.T) {
		dir := t.TempDir()
		require.Error(t, WriteFile(dir, []byte("data"), 0o600, ""))
		requireNoTemp(t, filepath.Dir(dir))
	})
}

func TestSymlinkThenDotDotSyncsPhysicalDirectories(t *testing.T) {
	root := t.TempDir()
	require.NoError(t, os.MkdirAll(root+"/actual/child", 0o700))
	require.NoError(t, os.MkdirAll(root+"/actual/dataset", 0o700))
	require.NoError(t, os.Symlink(root+"/actual/child", root+"/link"))
	physicalRoot := prepare(t, root)
	calls := make(map[string]int)
	var original func(string) error
	patch := mockey.Mock(syncDirectory).Origin(&original).To(func(name string) error {
		calls[name]++
		return original(name)
	}).Build()
	defer patch.UnPatch()
	name := root + "/link/../dataset/object"
	require.NoError(t, WriteFile(name, []byte("data"), 0o600, physicalRoot))
	require.Contains(t, calls, physicalRoot+"/actual/dataset")
	require.NotContains(t, calls, physicalRoot+"/dataset")
	require.NotContains(t, calls, filepath.Dir(physicalRoot))
	data, err := os.ReadFile(physicalRoot + "/actual/dataset/object")
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
}

func TestSearchOnlyAncestorAndExistingDescendantRetry(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses directory permissions")
	}
	ancestor := filepath.Join(t.TempDir(), "search")
	root := ancestor + "/data"
	require.NoError(t, os.MkdirAll(root, 0o700))
	require.NoError(t, os.Chmod(ancestor, 0o111))
	defer func() { require.NoError(t, os.Chmod(ancestor, 0o700)) }()
	physical := prepare(t, root)
	require.NoError(t, WriteFile(root+"/new/parents/object", []byte("data"), 0o600, physical))
	require.NoError(t, WriteFile(root+"/new/parents/one", []byte("one"), 0o600, physical))
	require.NoError(t, WriteFile(root+"/other/two", []byte("two"), 0o600, physical))
	patch := mockey.Mock(syncDirectory).Return(syscall.EIO).Build()
	err := WriteFile(root+"/failed/parents/object", []byte("data"), 0o600, physical)
	patch.UnPatch()
	require.ErrorIs(t, err, syscall.EIO)
	calls := make(map[string]int)
	var original func(string) error
	patch = mockey.Mock(syncDirectory).Origin(&original).To(func(name string) error {
		calls[name]++
		return original(name)
	}).Build()
	defer patch.UnPatch()
	require.NoError(t, WriteFile(root+"/failed/parents/object", []byte("retry"), 0o600, physical))
	require.Contains(t, calls, physical+"/failed/parents")
	require.Contains(t, calls, physical+"/failed")
	require.Contains(t, calls, physical)
	require.NotContains(t, calls, filepath.Dir(physical))
}

func TestBoundaryRejectsOutsideDirectory(t *testing.T) {
	root, err := filepath.EvalSymlinks(t.TempDir())
	require.NoError(t, err)
	err = SyncDirectories(root, filepath.Dir(root))
	// A path outside the data root is a Milvus fault, never request content.
	require.ErrorIs(t, err, merr.ErrIoFailed)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestEmptyRootBoundsEachFileByItsParent(t *testing.T) {
	// The working directory is not a boundary: unrelated absolute paths succeed.
	t.Chdir(t.TempDir())

	existing := filepath.Join(t.TempDir(), "existing")
	require.NoError(t, os.MkdirAll(existing, 0o700))
	created := filepath.Join(t.TempDir(), "created", "parents")
	calls := make(map[string]int)
	var original func(string) error
	patch := mockey.Mock(syncDirectory).Origin(&original).To(func(name string) error {
		calls[name]++
		return original(name)
	}).Build()
	defer patch.UnPatch()
	for _, name := range []string{filepath.Join(existing, "one"), filepath.Join(created, "two")} {
		require.NoError(t, WriteFile(name, []byte("data"), 0o600, ""))
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, "data", string(data))
	}
	physicalExisting, err := filepath.EvalSymlinks(existing)
	require.NoError(t, err)
	require.Contains(t, calls, physicalExisting)
	require.NotContains(t, calls, filepath.Dir(physicalExisting), "an existing parent bounds its own write")
	physicalCreated, err := filepath.EvalSymlinks(created)
	require.NoError(t, err)
	// Directories created by the write are persisted through the existing ancestor.
	require.Contains(t, calls, physicalCreated)
	require.Contains(t, calls, filepath.Dir(physicalCreated))
	require.Contains(t, calls, filepath.Dir(filepath.Dir(physicalCreated)))
}

func TestRelativePathUnderSymlinkedWorkingDirectory(t *testing.T) {
	base := t.TempDir()
	actual := filepath.Join(base, "actual")
	require.NoError(t, os.MkdirAll(actual, 0o700))
	link := filepath.Join(base, "link")
	require.NoError(t, os.Symlink(actual, link))
	// t.Chdir publishes the unresolved name as PWD, which os.Getwd returns.
	t.Chdir(link)
	require.NoError(t, WriteFile("relative/object", []byte("data"), 0o600, prepare(t, link)))
	data, err := os.ReadFile(filepath.Join(actual, "relative", "object"))
	require.NoError(t, err)
	require.Equal(t, "data", string(data))
}
