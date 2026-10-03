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

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/objstorage/objstorageprovider"
	"github.com/cockroachdb/pebble/sstable"
	"github.com/cockroachdb/pebble/vfs"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

// FileName is the file name a table with this ID is written under.
func FileName(id ID) string { return fmt.Sprintf("%d%s", int64(id), Extension) }

// Writer builds one SST under the caller's ID. Keys must be added in strictly
// increasing order. Close publishes the file; Abort discards it.
type Writer struct {
	id      ID
	path    string
	tmpPath string
	w       *sstable.Writer

	minKey  []byte
	maxKey  []byte
	entries int64
}

// NewWriter creates a Writer for table id, producing <dir>/<id>.sst. It writes
// to a temporary name first, so a crash mid-write cannot leave a short file
// under the name the ID promises.
func NewWriter(dir string, id ID) (*Writer, error) {
	tmp, err := os.CreateTemp(dir, FileName(id)+".tmp-*")
	if err != nil {
		return nil, pkerr.MarkIO(err, "create temp sst in %s", dir)
	}
	tmpPath := tmp.Name()
	tmp.Close()
	// reopen through pebble's vfs, which provides the Writable the sstable
	// writer needs
	f, err := vfs.Default.Create(tmpPath)
	if err != nil {
		os.Remove(tmpPath)
		return nil, pkerr.MarkIO(err, "open temp sst %s", tmpPath)
	}
	return &Writer{
		id:      id,
		path:    filepath.Join(dir, FileName(id)),
		tmpPath: tmpPath,
		w: sstable.NewWriter(objstorageprovider.NewFileWritable(f),
			PebbleOptions().MakeWriterOptions(0, TableFormat)),
	}, nil
}

// Add appends one key/value pair; keys must arrive in strictly increasing
// order under Comparer.
func (w *Writer) Add(key, value []byte) error {
	if err := w.w.Set(key, value); err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "write sst %s", w.tmpPath))
	}
	if w.minKey == nil {
		w.minKey = append([]byte{}, key...)
	}
	w.maxKey = append(w.maxKey[:0], key...)
	w.entries++
	return nil
}

// Close finalizes the SST, publishes it under its ID and returns its Info.
func (w *Writer) Close() (Info, error) {
	if err := w.w.Close(); err != nil {
		os.Remove(w.tmpPath)
		return Info{}, MarkPebbleErr(errors.Wrapf(err, "finalize sst %s", w.tmpPath))
	}
	st, err := os.Stat(w.tmpPath)
	if err != nil {
		return Info{}, pkerr.MarkIO(err, "stat sst %s", w.tmpPath)
	}
	if err := os.Rename(w.tmpPath, w.path); err != nil {
		return Info{}, pkerr.MarkIO(err, "publish sst as %s", w.path)
	}
	return Info{
		ID:          w.id,
		MinKey:      w.minKey,
		MaxKey:      w.maxKey,
		NumEntries:  w.entries,
		Size:        st.Size(),
		TableFormat: TableFormat,
	}, nil
}

// Abort discards the staged file.
func (w *Writer) Abort() {
	w.w.Close()
	os.Remove(w.tmpPath)
}
