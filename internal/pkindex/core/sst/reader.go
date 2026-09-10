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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// Reader reads one SST for point lookups and ordered iteration. It reads a
// local file; reading a table straight from object storage is later work.
type Reader struct {
	name string
	size int64
	r    *sstable.Reader
}

// ReaderOption adjusts how a table is opened.
type ReaderOption func(*readerConfig)

type readerConfig struct {
	expectedSize int64
	haveExpected bool
}

// ExpectSize makes OpenReader fail unless the source is exactly this many
// bytes. Pass the Size the manifest recorded: reaching the wrong object, or a
// truncated copy of the right one, is then caught at open time rather than as
// a puzzling read failure later. Without it the size is not checked, which is
// what a table being described for the first time needs.
func ExpectSize(n int64) ReaderOption {
	return func(c *readerConfig) {
		c.expectedSize = n
		c.haveExpected = true
	}
}

// OpenReader opens the SST at path. Blocks it reads are cached in cache, which
// the reader references until Close; nil reads without caching.
func OpenReader(path string, cache *pebble.Cache, opts ...ReaderOption) (*Reader, error) {
	var cfg readerConfig
	for _, o := range opts {
		o(&cfg)
	}
	f, err := os.Open(path)
	if err != nil {
		return nil, pkerr.MarkIO(err, "open sst %s", path)
	}
	readable, err := sstable.NewSimpleReadable(f)
	if err != nil {
		f.Close()
		return nil, MarkPebbleErr(errors.Wrapf(err, "read sst %s", path))
	}
	size := readable.Size()
	if cfg.haveExpected && size != cfg.expectedSize {
		readable.Close()
		return nil, errors.Mark(
			errors.Newf("sst %s is %d bytes, want %d", path, size, cfg.expectedSize),
			pkerr.ErrCorrupted)
	}
	ro := PebbleOptions().MakeReaderOptions()
	ro.Cache = cache
	r, err := sstable.NewReader(readable, ro)
	if err != nil {
		readable.Close()
		return nil, MarkPebbleErr(errors.Wrapf(err, "open sst reader %s", path))
	}
	return &Reader{name: path, size: size, r: r}, nil
}

// Preload reads the index and the filter into the block cache the reader was
// opened with, so that a later lookup does not pay for them while a caller
// holds a lock. Building an iterator loads the index; a seek that the filter
// answers loads the filter. Both stay cached because the cache is the one
// shared by the node, not one private to this reader.
func (r *Reader) Preload() error {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "preload sst %s", r.name))
	}
	defer it.Close()
	// a key almost no table holds, so the filter answers and no data block is
	// read; a varchar primary key of "\x00" does encode to it, and then this
	// costs one extra data block read
	var unlikely [1]byte
	it.SeekPrefixGE(unlikely[:], unlikely[:], 0)
	return r.iterErr(it.Error(), "preload sst %s")
}

// TableFormat reports the format version the file was written with.
func (r *Reader) TableFormat() (sstable.TableFormat, error) {
	return r.r.TableFormat()
}

// bounds returns the smallest and largest user key, reading only the blocks
// those two entries live in. Both are nil for an empty table.
func (r *Reader) bounds() (minKey, maxKey []byte, err error) {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return nil, nil, MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.name))
	}
	defer it.Close()
	first, _ := it.First()
	if first == nil {
		// empty, or a block that could not be read
		return nil, nil, r.iterErr(it.Error(), "read sst bounds in %s")
	}
	minKey = append([]byte{}, first.UserKey...)
	last, _ := it.Last()
	if last == nil {
		return nil, nil, r.iterErr(it.Error(), "read sst bounds in %s")
	}
	return minKey, append([]byte{}, last.UserKey...), nil
}

// iterErr turns an iterator error into a categorized one; a nil error means
// the iteration simply ran out of entries.
func (r *Reader) iterErr(err error, format string) error {
	if err == nil {
		return nil
	}
	return MarkPebbleErr(errors.Wrapf(err, format, r.name))
}

// Get returns the value stored for key, or ok=false if the key is absent.
func (r *Reader) Get(key []byte) (value []byte, ok bool, err error) {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return nil, false, MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.name))
	}
	defer it.Close()
	// the prefix argument is what gets checked against the bloom filter; the
	// filter holds whole keys, so the prefix is the key itself
	ik, lv := it.SeekPrefixGE(key, key, 0)
	if ik == nil {
		// a nil key is also how the iterator reports a block it could not read,
		// so skipping Error here would report a corrupt file as a missing key
		// and silently defeat deduplication
		if err := r.iterErr(it.Error(), "read sst %s"); err != nil {
			return nil, false, err
		}
		return nil, false, nil
	}
	if Comparer.Compare(ik.UserKey, key) != 0 {
		return nil, false, nil
	}
	v, callerOwned, err := lv.Value(nil)
	if err != nil {
		return nil, false, MarkPebbleErr(errors.Wrapf(err, "read sst value in %s", r.name))
	}
	if !callerOwned {
		v = append([]byte{}, v...)
	}
	return v, true, nil
}

// Iter iterates all entries in key order, invoking fn per entry; iteration
// stops at the first error. The key/value slices are only valid within fn.
func (r *Reader) Iter(fn func(key, value []byte) error) error {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.name))
	}
	defer it.Close()
	for ik, lv := it.First(); ik != nil; ik, lv = it.Next() {
		v, _, err := lv.Value(nil)
		if err != nil {
			return MarkPebbleErr(errors.Wrapf(err, "read sst value in %s", r.name))
		}
		if err := fn(ik.UserKey, v); err != nil {
			return err
		}
	}
	// the loop also ends on a block the iterator could not read
	return r.iterErr(it.Error(), "scan sst %s")
}

// Close releases the reader and the source it was opened from.
func (r *Reader) Close() error {
	if err := r.r.Close(); err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "close sst %s", r.name))
	}
	return nil
}
