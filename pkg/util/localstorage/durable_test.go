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
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestDirectWriteAndRetry(t *testing.T) {
	name := filepath.Join(t.TempDir(), "parents", "data", "object")
	require.NoError(t, WriteFile(name, []byte("old"), 0o640))
	before, err := os.Stat(name)
	require.NoError(t, err)
	require.NoError(t, Write(name, 0o777, func(w io.Writer) error {
		_, err := w.Write([]byte("new"))
		return err
	}))
	data, err := os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "new", string(data))
	after, err := os.Stat(name)
	require.NoError(t, err)
	require.True(t, os.SameFile(before, after), "write directly to the existing file")
	require.Equal(t, before.Mode().Perm(), after.Mode().Perm())

	err = Write(name, 0o640, func(w io.Writer) error {
		_, _ = w.Write([]byte("partial"))
		return syscall.EIO
	})
	require.ErrorIs(t, err, syscall.EIO)
	// Failed writes may remain. Callers must not commit their metadata.
	require.NoError(t, WriteFile(name, []byte("retry"), 0o600))
	data, err = os.ReadFile(name)
	require.NoError(t, err)
	require.Equal(t, "retry", string(data))
}

func TestFileSyncFailureCannotReturnSuccess(t *testing.T) {
	name := filepath.Join(t.TempDir(), "object")
	patch := mockey.Mock((*os.File).Sync).Return(syscall.EIO).Build()
	defer patch.UnPatch()
	require.ErrorIs(t, WriteFile(name, []byte("data"), 0o600), syscall.EIO)
}

func TestDirectorySyncFailureIsNotAcknowledged(t *testing.T) {
	name := filepath.Join(t.TempDir(), "new", "parents", "object")
	patch := mockey.Mock(syncDirectory).Return(syscall.EIO).Build()
	err := WriteFile(name, []byte("complete"), 0o600)
	patch.UnPatch()
	require.ErrorIs(t, err, syscall.EIO)
	require.NoError(t, WriteFile(name, []byte("retry"), 0o600))
}

func TestCloseAndOpenErrors(t *testing.T) {
	t.Run("close", func(t *testing.T) {
		name := filepath.Join(t.TempDir(), "object")
		var original func(*os.File) error
		patch := mockey.Mock((*os.File).Close).Origin(&original).To(func(f *os.File) error {
			return errors.CombineErrors(original(f), syscall.EIO)
		}).Build()
		err := WriteFile(name, []byte("data"), 0o600)
		patch.UnPatch()
		require.ErrorIs(t, err, syscall.EIO)
	})
	t.Run("directory", func(t *testing.T) {
		require.Error(t, WriteFile(t.TempDir(), []byte("data"), 0o600))
	})
}

func TestBatchCoalescesDirectorySync(t *testing.T) {
	root := t.TempDir()
	calls := make(map[string]int)
	patch := mockey.Mock(syncDirectory).To(func(name string) error {
		calls[name]++
		return nil
	}).Build()
	defer patch.UnPatch()
	var syncFile func(*os.File) error
	fileSyncs := 0
	filePatch := mockey.Mock((*os.File).Sync).Origin(&syncFile).To(func(f *os.File) error {
		fileSyncs++
		return syncFile(f)
	}).Build()
	defer filePatch.UnPatch()
	files := make(map[string][]byte)
	for i := 0; i < 16; i++ {
		files[filepath.Join(root, "same", "parent", fmt.Sprint(i))] = []byte("value")
	}
	require.NoError(t, WriteFiles(files, 0o600))
	require.Equal(t, len(files), fileSyncs, "synchronize each completed file exactly once")
	for dir, count := range calls {
		require.Equal(t, 1, count, "directory %s synchronized more than once in one batch", dir)
	}
	require.Contains(t, calls, filepath.Join(root, "same", "parent"))
	require.Contains(t, calls, root)
	for name, expected := range files {
		data, err := os.ReadFile(name)
		require.NoError(t, err)
		require.Equal(t, expected, data)
	}
}

func BenchmarkDurableBatch(b *testing.B) {
	for _, batch := range []bool{false, true} {
		b.Run(fmt.Sprintf("batch=%t", batch), func(b *testing.B) {
			root := b.TempDir()
			files := make(map[string][]byte)
			for i := 0; i < 16; i++ {
				files[filepath.Join(root, "objects", fmt.Sprint(i))] = bytes.Repeat([]byte("x"), 4096)
			}
			b.SetBytes(16 * 4096)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if batch {
					if err := WriteFiles(files, 0o600); err != nil {
						b.Fatal(err)
					}
				} else {
					for name, data := range files {
						if err := WriteFile(name, data, 0o600); err != nil {
							b.Fatal(err)
						}
					}
				}
			}
		})
	}
}

func BenchmarkDurableCompleteObject(b *testing.B) {
	for _, chunks := range []int{1, 64} {
		b.Run(fmt.Sprintf("writes=%d", chunks), func(b *testing.B) {
			name := filepath.Join(b.TempDir(), "object")
			data := bytes.Repeat([]byte("x"), 64*1024/chunks)
			b.SetBytes(64 * 1024)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if err := Write(name, 0o600, func(w io.Writer) error {
					for j := 0; j < chunks; j++ {
						if _, err := w.Write(data); err != nil {
							return err
						}
					}
					return nil
				}); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
