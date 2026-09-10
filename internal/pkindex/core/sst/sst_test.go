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

package sst

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

const fixtureID = ID(4242)

// writeFixture writes n int64 PKs (0..n-1) with segmentID=pk*10 and returns
// the finalized Info together with the file it landed in.
func writeFixture(t *testing.T, dir string, n int) (Info, string) {
	return writeFixtureID(t, dir, fixtureID, n)
}

func writeFixtureID(t *testing.T, dir string, id ID, n int) (Info, string) {
	t.Helper()
	w, err := NewWriter(dir, id)
	require.NoError(t, err)
	for i := 0; i < n; i++ {
		err := w.Add(codec.EncodeInt64PK(int64(i)), codec.EncodePKEntry(codec.PKEntry{SegmentID: int64(i * 10)}))
		require.NoError(t, err)
	}
	info, err := w.Close()
	require.NoError(t, err)
	return info, filepath.Join(dir, FileName(id))
}

func TestWriterReaderRoundtrip(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	info, path := writeFixture(t, dir, n)

	assert.Equal(t, fixtureID, info.ID)
	assert.Equal(t, int64(n), info.NumEntries)
	assert.Equal(t, codec.EncodeInt64PK(0), info.MinKey)
	assert.Equal(t, codec.EncodeInt64PK(n-1), info.MaxKey)
	assert.Equal(t, TableFormat, info.TableFormat)
	st, err := os.Stat(path)
	require.NoError(t, err)
	assert.Equal(t, st.Size(), info.Size)

	r, err := OpenReader(path, nil)
	require.NoError(t, err)
	defer r.Close()

	var i int64
	err = r.Iter(func(key, value []byte) error {
		pk, err := codec.DecodeInt64PK(key)
		require.NoError(t, err)
		require.Equal(t, i, pk)
		e, err := codec.DecodePKEntry(value)
		require.NoError(t, err)
		require.Equal(t, pk*10, e.SegmentID)
		i++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, int64(n), i)
}

// A table produced elsewhere, by a memtable flush, is described by reading it;
// that description must match what a Writer would have reported.
func TestReadInfoMatchesWriter(t *testing.T) {
	dir := t.TempDir()
	written, path := writeFixture(t, dir, 1000)

	read, err := ReadInfo(fixtureID, path)
	require.NoError(t, err)
	assert.Equal(t, written, read)
}

func TestReadInfoOnEmptyTable(t *testing.T) {
	dir := t.TempDir()
	_, path := writeFixture(t, dir, 0)

	info, err := ReadInfo(fixtureID, path)
	require.NoError(t, err)
	assert.Zero(t, info.NumEntries)
	assert.Nil(t, info.MinKey)
	assert.Nil(t, info.MaxKey)
}

// Reaching the wrong object, or a truncated copy of the right one, is caught
// when the table is opened rather than as a puzzling read failure later.
func TestOpenReaderExpectSize(t *testing.T) {
	dir := t.TempDir()
	info, path := writeFixture(t, dir, 100)

	r, err := OpenReader(path, nil, ExpectSize(info.Size))
	require.NoError(t, err)
	require.NoError(t, r.Close())

	_, err = OpenReader(path, nil, ExpectSize(info.Size+1))
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "got %v", err)
}

// Describing a table must not read its data. Corrupting a block in the middle
// makes that visible: a scan trips over it, while ReadInfo, which only ever
// touches the metadata and the blocks holding the first and last key, does
// not notice it at all.
func TestReadInfoSkipsDataBlocks(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	_, path := writeFixture(t, dir, n)

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	b[len(b)/2] ^= 0xff
	corrupted := filepath.Join(dir, FileName(ID(99)))
	require.NoError(t, os.WriteFile(corrupted, b, 0o600)) //nolint:gosec // a fixed name under the test's own temp dir

	info, err := ReadInfo(ID(99), corrupted)
	require.NoError(t, err, "describing a table must not read the damaged block")
	assert.Equal(t, int64(n), info.NumEntries)
	assert.Equal(t, codec.EncodeInt64PK(0), info.MinKey)
	assert.Equal(t, codec.EncodeInt64PK(n-1), info.MaxKey)

	// the damage is real: a scan of the same file fails
	r, err := OpenReader(corrupted, nil)
	require.NoError(t, err)
	defer r.Close()
	err = r.Iter(func(key, value []byte) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "got %v", err)
}
