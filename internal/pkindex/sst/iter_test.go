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
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/pkindex"
)

func TestIterWalksAllEntriesInOrder(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 7)
	require.NoError(t, err)
	for _, k := range []string{"a", "b", "c"} {
		require.NoError(t, w.Add([]byte(k), []byte("v"+k)))
	}
	_, err = w.Close()
	require.NoError(t, err)

	r, err := OpenReader(filepath.Join(dir, FileName(7)), nil)
	require.NoError(t, err)
	defer r.Close()
	it, err := r.NewIter()
	require.NoError(t, err)
	defer it.Close()
	var got []string
	for ok := it.First(); ok; ok = it.Next() {
		got = append(got, string(it.Key())+"="+string(it.Value()))
	}
	require.NoError(t, it.Error())
	require.Equal(t, []string{"a=va", "b=vb", "c=vc"}, got)
	require.False(t, it.Next())
}

func TestIterOnEmptyTable(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 9)
	require.NoError(t, err)
	_, err = w.Close()
	require.NoError(t, err)

	r, err := OpenReader(filepath.Join(dir, FileName(9)), nil)
	require.NoError(t, err)
	defer r.Close()
	it, err := r.NewIter()
	require.NoError(t, err)
	defer it.Close()
	require.False(t, it.First())
	require.NoError(t, it.Error())
}

func TestIterReportsCorruptBlock(t *testing.T) {
	dir := t.TempDir()
	w, err := NewWriter(dir, 8)
	require.NoError(t, err)
	for i := 0; i < 1000; i++ {
		require.NoError(t, w.Add([]byte(fmt.Sprintf("k%06d", i)), []byte("value")))
	}
	_, err = w.Close()
	require.NoError(t, err)
	path := filepath.Join(dir, FileName(8))

	// aim at a data block in the middle, taken from the table layout
	probe, err := OpenReader(path, nil)
	require.NoError(t, err)
	l, err := probe.r.Layout()
	require.NoError(t, err)
	require.Greater(t, len(l.Data), 1)
	off := l.Data[len(l.Data)/2].Offset + 8
	require.NoError(t, probe.Close())

	b, err := os.ReadFile(path)
	require.NoError(t, err)
	for i := off; i < off+64; i++ {
		b[i] ^= 0xff
	}
	require.NoError(t, os.WriteFile(path, b, 0o600))

	r, err := OpenReader(path, nil)
	require.NoError(t, err)
	defer r.Close()
	it, err := r.NewIter()
	require.NoError(t, err)
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
	}
	require.Error(t, it.Error())
	// cockroachdb errors.Is recognizes the mark; the standard library one does not
	require.True(t, errors.Is(it.Error(), pkindex.ErrCorrupted), "got %v", it.Error())
	require.False(t, it.Next())
	require.False(t, it.First())
	require.True(t, errors.Is(it.Error(), pkindex.ErrCorrupted))
}

func writeSmallTable(t *testing.T, id ID) *Reader {
	t.Helper()
	dir := t.TempDir()
	w, err := NewWriter(dir, id)
	require.NoError(t, err)
	for _, k := range []string{"a", "b"} {
		require.NoError(t, w.Add([]byte(k), []byte("v"+k)))
	}
	_, err = w.Close()
	require.NoError(t, err)
	r, err := OpenReader(filepath.Join(dir, FileName(id)), nil)
	require.NoError(t, err)
	return r
}

func TestIterDoubleCloseKeepsOtherIteratorsApart(t *testing.T) {
	r := writeSmallTable(t, 10)
	defer r.Close()
	it, err := r.NewIter()
	require.NoError(t, err)
	require.NoError(t, it.Close())
	require.NoError(t, it.Close())

	// a double release would hand the same pebble iterator to both of these
	a, err := r.NewIter()
	require.NoError(t, err)
	b, err := r.NewIter()
	require.NoError(t, err)
	require.NotSame(t, a.it, b.it)
	require.True(t, a.First())
	require.True(t, b.First())
	require.True(t, a.Next())
	require.Equal(t, "b", string(a.Key()))
	require.Equal(t, "a", string(b.Key()))
	require.NoError(t, a.Close())
	require.NoError(t, b.Close())
}

func TestIterAfterCloseFails(t *testing.T) {
	r := writeSmallTable(t, 11)
	defer r.Close()
	it, err := r.NewIter()
	require.NoError(t, err)
	require.True(t, it.First())
	require.NoError(t, it.Close())
	require.False(t, it.First())
	err = it.Error()
	require.Error(t, err)
	require.Contains(t, err.Error(), "is closed")
	require.False(t, errors.Is(err, pkindex.ErrCorrupted))
	require.False(t, errors.Is(err, pkindex.ErrIO))
	require.False(t, it.Next())
	require.Equal(t, err, it.Error())
}

func TestIterNextAfterEndStaysFalse(t *testing.T) {
	r := writeSmallTable(t, 12)
	defer r.Close()
	it, err := r.NewIter()
	require.NoError(t, err)
	defer it.Close()
	for ok := it.First(); ok; ok = it.Next() {
	}
	require.False(t, it.Next())
	require.False(t, it.Next())
	require.NoError(t, it.Error())
	// First starts the walk again
	require.True(t, it.First())
	require.Equal(t, "a", string(it.Key()))
}

func TestReaderCloseWaitsForIterators(t *testing.T) {
	r := writeSmallTable(t, 13)
	it, err := r.NewIter()
	require.NoError(t, err)
	require.True(t, it.First())

	err = r.Close()
	require.Error(t, err)
	require.Contains(t, err.Error(), "1 iterators are still open")
	// the reader is still usable
	require.True(t, it.Next())
	require.NoError(t, it.Close())
	require.NoError(t, it.Close())
	require.NoError(t, r.Close())
}
