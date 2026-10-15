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

package index

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex"
)

var _ TableStore = (*FakeStore)(nil)

func TestFakeStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := filepath.Join(dir, "src.sst")
	require.NoError(t, os.WriteFile(src, []byte("table"), 0o600))

	s := NewFakeStore()
	require.NoError(t, s.UploadFile(ctx, "b", src))
	require.NoError(t, s.UploadFile(ctx, "a", src))
	assert.Equal(t, []string{"a", "b"}, s.Keys())
	assert.Equal(t, 2, s.Len())

	dst := filepath.Join(dir, "dst.sst")
	require.NoError(t, s.DownloadFile(ctx, "a", dst))
	data, err := os.ReadFile(dst)
	require.NoError(t, err)
	assert.Equal(t, []byte("table"), data)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 2)
}

func TestFakeStoreMissingKey(t *testing.T) {
	dst := filepath.Join(t.TempDir(), "dst.sst")
	err := NewFakeStore().DownloadFile(context.Background(), "missing", dst)
	assert.True(t, errors.Is(err, pkindex.ErrCorrupted))
	assert.NoFileExists(t, dst)
}

func TestFakeStoreInjectedFailures(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	src := filepath.Join(dir, "src.sst")
	require.NoError(t, os.WriteFile(src, []byte("table"), 0o600))
	dst := filepath.Join(dir, "dst.sst")

	s := NewFakeStore()
	s.FailUploads(1)
	assert.True(t, errors.Is(s.UploadFile(ctx, "a", src), pkindex.ErrIO))
	assert.Equal(t, 0, s.Len())
	require.NoError(t, s.UploadFile(ctx, "a", src))

	s.FailDownloads(2)
	assert.True(t, errors.Is(s.DownloadFile(ctx, "a", dst), pkindex.ErrIO))
	assert.True(t, errors.Is(s.DownloadFile(ctx, "a", dst), pkindex.ErrIO))
	assert.NoFileExists(t, dst)
	require.NoError(t, s.DownloadFile(ctx, "a", dst))
	assert.FileExists(t, dst)
}
