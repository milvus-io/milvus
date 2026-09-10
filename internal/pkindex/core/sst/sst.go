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

// Package sst reads and writes the pkindex SST files.
//
// An SST is a pebble sstable holding order-preserving encoded primary keys
// (see the codec package) mapped to PKEntry values. The same comparer, table
// format and filter are used by every producer (streamingnode memtable flush,
// datanode merge) and consumer, so files are interchangeable across roles.
//
// This package reads and writes local files only. Info describes what a
// table's bytes are, identically on every node and durably in the manifest,
// which is why a local path is not part of it; where the bytes live on this
// node travels alongside, as an argument.
package sst

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// Extension is the file extension of a pkindex SST file.
const Extension = ".sst"

// ID identifies one table. It is a Milvus global ID, the same allocator that
// hands out segment and log IDs, and the caller passes it in; this package
// never invents one. An ID is used once, so an upload that retries under the
// same ID stays idempotent.
type ID int64

// Info is what a table's bytes are: the same on every node, and durable,
// because the manifest records it. Where they are on this node is an argument.
//
// There is no whole-file checksum. Pebble verifies a CRC on every block it
// reads and fails to open a truncated file, and the unique ID plus Size guard
// against reading the wrong file, so hashing the whole file would only add a
// full read on the flush path for no gain.
type Info struct {
	// ID identifies the table and names its object.
	ID ID
	// MinKey and MaxKey are the smallest and largest user keys, for pruning.
	// Both are nil for an empty table.
	MinKey []byte
	MaxKey []byte
	// NumEntries is the number of key/value pairs, for compaction scoring and
	// metrics.
	NumEntries int64
	// Size is the size in bytes, checked when the table is opened.
	Size int64
	// TableFormat is this file's own format version.
	TableFormat sstable.TableFormat
}

// ReadInfo describes a table without scanning it. Everything but the key range
// comes from the table's own metadata, and the key range comes from seeking to
// the first and the last entry, so the cost does not grow with the table.
func ReadInfo(id ID, path string) (Info, error) {
	// metadata only: keep these blocks out of any shared cache
	r, err := OpenReader(path, nil)
	if err != nil {
		return Info{}, err
	}
	defer r.Close()

	format, err := r.TableFormat()
	if err != nil {
		return Info{}, MarkPebbleErr(errors.Wrapf(err, "read sst format in %s", path))
	}
	info := Info{
		ID:          id,
		NumEntries:  int64(r.r.Properties.NumEntries),
		Size:        r.size,
		TableFormat: format,
	}
	minKey, maxKey, err := r.bounds()
	if err != nil {
		return Info{}, err
	}
	info.MinKey, info.MaxKey = minKey, maxKey
	return info, nil
}

// MarkPebbleErr categorizes a failure reported by pebble's sstable reader or
// writer, adding no context of its own: pebble reports a file whose bytes do
// not parse as ErrCorruption, and everything else these calls can fail with is
// a disk failure. The engine package classifies its own pebble errors through
// this, so that one rule covers both.
func MarkPebbleErr(err error) error {
	if errors.Is(err, pebble.ErrCorruption) {
		return errors.Mark(err, pkerr.ErrCorrupted)
	}
	return errors.Mark(err, pkerr.ErrIO)
}
