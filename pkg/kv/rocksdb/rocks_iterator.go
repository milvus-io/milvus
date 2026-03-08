package rocksdbkv

import (
	"runtime"

	"github.com/tecbot/gorocksdb"

	"github.com/milvus-io/milvus/pkg/v2/log"
)

/**
 * A wrapper of go rocksdb iterator
 * it helps on 1) reserve the upperBound array to avoid garbage collection
 * 2) do a leakage check of iterator
 */
type RocksIterator struct {
	it         *gorocksdb.Iterator
	upperBound []byte
	close      bool
}

func NewRocksIterator(db *gorocksdb.DB, opts *gorocksdb.ReadOptions) *RocksIterator {
	iter := db.NewIterator(opts)
	it := &RocksIterator{iter, nil, false}
	runtime.SetFinalizer(it, func(rocksit *RocksIterator) {
		if !rocksit.close {
			log.Error("iterator is leaking.. please check")
		}
	})
	return it
}

func NewRocksIteratorCF(db *gorocksdb.DB, cf *gorocksdb.ColumnFamilyHandle, opts *gorocksdb.ReadOptions) *RocksIterator {
	iter := db.NewIteratorCF(opts, cf)
	it := &RocksIterator{iter, nil, false}
	runtime.SetFinalizer(it, func(rocksit *RocksIterator) {
		if !rocksit.close {
			log.Error("iterator is leaking.. please check")
		}
	})
	return it
}

func NewRocksIteratorWithUpperBound(db *gorocksdb.DB, upperBoundString string, opts *gorocksdb.ReadOptions) *RocksIterator {
	upperBound := []byte(upperBoundString)
	opts.SetIterateUpperBound(upperBound)
	iter := db.NewIterator(opts)
	it := &RocksIterator{iter, upperBound, false}
	runtime.SetFinalizer(it, func(rocksit *RocksIterator) {
		if !rocksit.close {
			log.Error("iterator is leaking.. please check")
		}
	})
	return it
}

func NewRocksIteratorCFWithUpperBound(db *gorocksdb.DB, cf *gorocksdb.ColumnFamilyHandle, upperBoundString string, opts *gorocksdb.ReadOptions) *RocksIterator {
	upperBound := []byte(upperBoundString)
	opts.SetIterateUpperBound(upperBound)
	iter := db.NewIteratorCF(opts, cf)
	it := &RocksIterator{iter, upperBound, false}
	runtime.SetFinalizer(it, func(rocksit *RocksIterator) {
		if !rocksit.close {
			log.Error("iteratorCF is leaking.. please check")
		}
	})
	return it
}

// Valid returns false only when an Iterator has iterated past either the
// first or the last key in the database.
func (iter *RocksIterator) Valid() bool {
	return iter.it.Valid()
}

// ValidForPrefix returns false only when an Iterator has iterated past the
// first or the last key in the database or the specified prefix.
func (iter *RocksIterator) ValidForPrefix(prefix []byte) bool {
	return iter.it.ValidForPrefix(prefix)
}

// Key returns the key the iterator currently holds.
func (iter *RocksIterator) Key() *gorocksdb.Slice {
	return iter.it.Key()
}

// Value returns the value in the database the iterator currently holds.
func (iter *RocksIterator) Value() *gorocksdb.Slice {
	return iter.it.Value()
}

// Next moves the iterator to the next sequential key in the database.
func (iter *RocksIterator) Next() {
	iter.it.Next()
}

// Prev moves the iterator to the previous sequential key in the database.
func (iter *RocksIterator) Prev() {
	iter.it.Prev()
}

// SeekToFirst moves the iterator to the first key in the database.
func (iter *RocksIterator) SeekToFirst() {
	iter.it.SeekToFirst()
}

// SeekToLast moves the iterator to the last key in the database.
func (iter *RocksIterator) SeekToLast() {
	iter.it.SeekToLast()
}

// Seek moves the iterator to the position greater than or equal to the key.
func (iter *RocksIterator) Seek(key []byte) {
	iter.it.Seek(key)
}

// SeekForPrev moves the iterator to the last key that less than or equal
// to the target key, in contrast with Seek.
func (iter *RocksIterator) SeekForPrev(key []byte) {
	iter.it.SeekForPrev(key)
}

// Err returns nil if no errors happened during iteration, or the actual
// error otherwise.
func (iter *RocksIterator) Err() error {
	return iter.it.Err()
}

// Close closes the iterator.
func (iter *RocksIterator) Close() {
	iter.close = true
	iter.it.Close()
}
