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
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/sstable"
)

// Iter walks one table in key order. Key and Value alias buffers owned by the
// iterator and stay valid only until the next First, Next or Close on this
// Iter; copy them to keep them, and do not modify them. After First or Next
// returns false, Error tells a read failure from the end of the table.
type Iter struct {
	name      string
	reader    *Reader
	it        sstable.Iterator
	key       []byte
	value     []byte
	err       error
	exhausted bool
}

// NewIter returns an iterator positioned before the first entry. Close every
// Iter before its Reader.
func (r *Reader) NewIter() (*Iter, error) {
	it, err := r.r.NewIter(nil, nil)
	if err != nil {
		return nil, MarkPebbleErr(errors.Wrapf(err, "scan sst %s", r.name))
	}
	r.openIters.Add(1)
	return &Iter{name: r.name, reader: r, it: it}, nil
}

// First moves to the first entry and reports whether there is one. An error is
// final: First does not restart the walk after one.
func (it *Iter) First() bool {
	if it.checkOpen() {
		return false
	}
	it.exhausted = false
	return it.load(it.it.First())
}

// Next moves to the next entry and reports whether there is one. After the
// table has ended it keeps returning false.
func (it *Iter) Next() bool {
	if it.checkOpen() || it.exhausted {
		return false
	}
	return it.load(it.it.Next())
}

// checkOpen reports whether the walk cannot go on: the iterator is closed, or
// an earlier read failed. Using a closed iterator is a caller bug, so its
// error carries no category.
func (it *Iter) checkOpen() bool {
	if it.it == nil {
		if it.err == nil {
			it.err = errors.Newf("sst iterator of %s is closed", it.name)
		}
		return true
	}
	return it.err != nil
}

func (it *Iter) load(ik *sstable.InternalKey, lv pebble.LazyValue) bool {
	if ik == nil {
		it.key, it.value = nil, nil
		it.exhausted = true
		// a nil key is also how the iterator reports a block it could not read
		if err := it.it.Error(); err != nil {
			it.err = MarkPebbleErr(errors.Wrapf(err, "scan sst %s", it.name))
		}
		return false
	}
	v, _, err := lv.Value(nil)
	if err != nil {
		it.key, it.value = nil, nil
		it.err = MarkPebbleErr(errors.Wrapf(err, "read sst value in %s", it.name))
		return false
	}
	it.key, it.value = ik.UserKey, v
	return true
}

// Key returns the key of the current entry.
func (it *Iter) Key() []byte { return it.key }

// Value returns the value of the current entry.
func (it *Iter) Value() []byte { return it.value }

// Error returns the read failure that ended the walk, if any.
func (it *Iter) Error() error { return it.err }

// Close releases the iterator; closing it again does nothing. Close may return
// the error that Error already reported.
func (it *Iter) Close() error {
	if it.it == nil {
		return nil
	}
	err := it.it.Close()
	it.it = nil
	it.key, it.value = nil, nil
	it.reader.openIters.Add(-1)
	if err != nil {
		return MarkPebbleErr(errors.Wrapf(err, "close sst iterator of %s", it.name))
	}
	return nil
}
