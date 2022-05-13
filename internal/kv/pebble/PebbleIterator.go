package pebblekv

import (
	"runtime"

	"github.com/cockroachdb/pebble"
	"github.com/milvus-io/milvus/internal/log"
)

/**
 * A wrapper of go pebble iterator
 * it helps on 1) reserve the upperBound array to avoid garbage collection
 * 2) do a leakage check of iterator
 */
type PebbleIterator struct {
	it         *pebble.Iterator
	upperBound []byte
	close      bool
}

func NewPebbleIterator(db *pebble.DB, opts *pebble.IterOptions) *PebbleIterator {
	iter := db.NewIter(opts)
	it := &PebbleIterator{iter, nil, false}
	runtime.SetFinalizer(it, func(iter *PebbleIterator) {
		if !iter.close {
			log.Error("iterator is leaking.. please check")
		}
	})
	return it
}

func NewPebbleIteratorWithUpperBound(db *pebble.DB, opts *pebble.IterOptions) *PebbleIterator {
	iter := db.NewIter(opts)
	it := &PebbleIterator{iter, opts.GetUpperBound(), false}
	runtime.SetFinalizer(it, func(it *PebbleIterator) {
		if !it.close {
			log.Error("iterator is leaking.. please check")
		}
	})
	return it
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *PebbleIterator) Valid() bool {
	return iter.it.Valid()
}

// Key returns the key the iterator currently holds.
func (iter *PebbleIterator) Key() []byte {
	return iter.it.Key()
}

// Value returns the value in the database the iterator currently holds.
func (iter *PebbleIterator) Value() []byte {
	return iter.it.Value()
}

// Next moves the iterator to the next sequential key in the database.
func (iter *PebbleIterator) Next() {
	iter.it.Next()
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *PebbleIterator) Prev() {
	iter.it.Prev()
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *PebbleIterator) SeekToFirst() {
	iter.it.First()
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *PebbleIterator) SeekToLast() {
	iter.it.Last()
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *PebbleIterator) Seek(key []byte) {
	iter.it.SeekGE(key)
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (iter *PebbleIterator) SeekForPrev(key []byte) {
	iter.it.SeekLT(key)
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (iter *PebbleIterator) Err() error {
	return iter.it.Error()
}

// Close closes the iterator.
func (iter *PebbleIterator) Close() {
	iter.close = true
	iter.it.Close()
}
