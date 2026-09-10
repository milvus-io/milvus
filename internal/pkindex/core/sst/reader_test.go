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
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex/core/codec"
	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

func TestReaderGet(t *testing.T) {
	dir := t.TempDir()
	_, path := writeFixture(t, dir, 1000)
	r, err := OpenReader(path, nil)
	require.NoError(t, err)
	defer r.Close()

	// hit
	v, ok, err := r.Get(codec.EncodeInt64PK(233))
	require.NoError(t, err)
	require.True(t, ok)
	e, err := codec.DecodePKEntry(v)
	require.NoError(t, err)
	assert.Equal(t, int64(2330), e.SegmentID)

	// miss inside range is impossible with dense fixture; miss beyond both ends
	_, ok, err = r.Get(codec.EncodeInt64PK(-1))
	require.NoError(t, err)
	assert.False(t, ok)
	_, ok, err = r.Get(codec.EncodeInt64PK(1000))
	require.NoError(t, err)
	assert.False(t, ok)
}

func TestReaderUsesGivenBlockCache(t *testing.T) {
	_, path := writeFixture(t, t.TempDir(), 1000)
	c := pebble.NewCache(8 << 20)
	defer c.Unref()
	r, err := OpenReader(path, c)
	require.NoError(t, err)
	defer r.Close()

	for i := 0; i < 2; i++ {
		_, ok, err := r.Get(codec.EncodeInt64PK(233))
		require.NoError(t, err)
		require.True(t, ok)
	}
	m := c.Metrics()
	assert.Positive(t, m.Count, "probed blocks must land in the given cache")
	assert.Positive(t, m.Hits, "the second probe must be served from the given cache")
}

// A probe for a key inside the table's range but absent from it is the common
// case on the write path; the table's bloom filter must answer it without
// reading data blocks.

// A probe for a key inside the table's range but absent from it is the common
// case on the write path; the table's bloom filter must answer it without
// reading data blocks.
func TestGetMissSkipsDataBlocks(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, fixtureID)
	require.NoError(t, err)
	const n = 100000
	for i := int64(0); i < n; i++ { // even keys only: every odd key is an in-range miss
		require.NoError(t, w.Add(codec.EncodeInt64PK(i*2), codec.EncodePKEntry(codec.PKEntry{SegmentID: i})))
	}
	info, err := w.Close()
	require.NoError(t, err)
	path := filepath.Join(dir, FileName(info.ID))

	c := pebble.NewCache(64 << 20)
	defer c.Unref()
	r, err := OpenReader(path, c)
	require.NoError(t, err)
	defer r.Close()

	// 500 misses spread over the whole key range
	for i := int64(0); i < 500; i++ {
		_, ok, err := r.Get(codec.EncodeInt64PK(i*(2*n/500) + 1))
		require.NoError(t, err)
		require.False(t, ok)
	}
	// without a filter nearly every probe loads a distinct data block
	assert.Less(t, c.Metrics().Misses, int64(50), "misses must be answered by the bloom filter, not by data blocks")

	v, ok, err := r.Get(codec.EncodeInt64PK(4242))
	require.NoError(t, err)
	require.True(t, ok)
	e, err := codec.DecodePKEntry(v)
	require.NoError(t, err)
	assert.Equal(t, int64(2121), e.SegmentID)
}

// A flipped byte inside a data block is only discovered when that block is
// read, so the read path must categorize it the same way Verify does.

// A flipped byte inside a data block is only discovered when that block is
// read, so the read path must categorize it the same way Verify does.
func TestGetOnCorruptedDataBlock(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	_, path := writeFixture(t, dir, n)

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	b[len(b)/10] ^= 0xff
	corrupted := filepath.Join(dir, "corrupted.sst")
	require.NoError(t, os.WriteFile(corrupted, b, 0o600)) //nolint:gosec // a fixed name under the test's own temp dir

	r, err := OpenReader(corrupted, nil)
	require.NoError(t, err, "the flipped byte is in a data block, so opening must still succeed")
	defer r.Close()

	var getErr error
	for i := 0; i < n && getErr == nil; i++ {
		_, _, getErr = r.Get(codec.EncodeInt64PK(int64(i)))
	}
	require.Error(t, getErr, "a flipped byte in a data block must surface on the read path")
	assert.True(t, errors.Is(getErr, pkerr.ErrCorrupted), "got %v", getErr)
}

// Iteration ends on an unreadable block the same way it ends on the last
// entry, so a scan of a damaged file must fail rather than return short.

// Iteration ends on an unreadable block the same way it ends on the last
// entry, so a scan of a damaged file must fail rather than return short.
func TestIterOnCorruptedDataBlock(t *testing.T) {
	dir := t.TempDir()
	const n = 10000
	_, path := writeFixture(t, dir, n)

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	b[len(b)/10] ^= 0xff
	corrupted := filepath.Join(dir, "corrupted-iter.sst")
	require.NoError(t, os.WriteFile(corrupted, b, 0o600)) //nolint:gosec // a fixed name under the test's own temp dir

	r, err := OpenReader(corrupted, nil)
	require.NoError(t, err)
	defer r.Close()

	err = r.Iter(func(key, value []byte) error { return nil })
	require.Error(t, err)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted), "got %v", err)
}

// A lookup on the write path runs while a lock is held, so the index and the
// filter must already be in the cache by then. Preload is what puts them
// there: after it, answering a miss costs no cache miss at all.
func TestPreloadWarmsIndexAndFilter(t *testing.T) {
	dir := t.TempDir()
	_, path := writeFixture(t, dir, 10000)
	c := pebble.NewCache(32 << 20)
	defer c.Unref()

	r, err := OpenReader(path, c)
	require.NoError(t, err)
	defer r.Close()
	require.NoError(t, r.Preload())

	before := c.Metrics().Misses
	// a key outside the table: answered by the filter alone
	_, ok, err := r.Get(codec.EncodeInt64PK(-1))
	require.NoError(t, err)
	require.False(t, ok)
	assert.Equal(t, before, c.Metrics().Misses,
		"a miss after Preload must not fault in the index or the filter")
}
