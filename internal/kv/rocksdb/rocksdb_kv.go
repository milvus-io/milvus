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

package rocksdbkv

import (
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/tecbot/gorocksdb"

	"github.com/milvus-io/milvus/internal/kv"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

var _ kv.BaseKV = (*RocksdbKV)(nil)

// RocksdbKV is KV implemented by rocksdb
type RocksdbKV struct {
	Opts         *gorocksdb.Options
	DB           *gorocksdb.DB
	WriteOptions *gorocksdb.WriteOptions
	ReadOptions  *gorocksdb.ReadOptions
	name         string
}

const (
	// LRUCacheSize is the lru cache size of rocksdb, default 0
	LRUCacheSize = 0
)

// NewRocksdbKV returns a rockskv object, only used in test
func NewRocksdbKV(name string) (*RocksdbKV, error) {
	// TODO we should use multiple column family of rocks db rather than init multiple db instance
	if name == "" {
		return nil, errors.New("rocksdb name is nil")
	}
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(LRUCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	// by default there are only 1 thread for flush compaction, which may block each other.
	// increase to a reasonable thread numbers
	opts.IncreaseParallelism(2)
	// enable back ground flush
	opts.SetMaxBackgroundFlushes(1)
	opts.SetCreateIfMissing(true)
	return NewRocksdbKVWithOpts(name, opts)
}

// NewRocksdbKV returns a rockskv object
func NewRocksdbKVWithOpts(name string, opts *gorocksdb.Options) (*RocksdbKV, error) {
	ro := gorocksdb.NewDefaultReadOptions()
	wo := gorocksdb.NewDefaultWriteOptions()

	// only has one columnn families
	db, err := gorocksdb.OpenDb(opts, name)
	if err != nil {
		return nil, err
	}
	return &RocksdbKV{
		Opts:         opts,
		DB:           db,
		WriteOptions: wo,
		ReadOptions:  ro,
		name:         name,
	}, nil
}

// Close free resource of rocksdb
func (kv *RocksdbKV) Close() {
	if kv.DB != nil {
		kv.DB.Close()
	}
}

// GetName returns the name of this object
func (kv *RocksdbKV) GetName() string {
	return kv.name
}

// Load returns the value of specified key
func (kv *RocksdbKV) Load(key string) (string, error) {
	if kv.DB == nil {
		return "", fmt.Errorf("rocksdb instance is nil when load %s", key)
	}
	if key == "" {
		return "", errors.New("rocksdb kv does not support load empty key")
	}
	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()
	value, err := kv.DB.Get(option, []byte(key))
	if err != nil {
		return "", err
	}
	defer value.Free()
	return string(value.Data()), nil
}

func (kv *RocksdbKV) LoadBytes(key string) ([]byte, error) {
	if kv.DB == nil {
		return nil, fmt.Errorf("rocksdb instance is nil when load %s", key)
	}
	if key == "" {
		return nil, errors.New("rocksdb kv does not support load empty key")
	}

	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()

	value, err := kv.DB.Get(option, []byte(key))
	if err != nil {
		return nil, err
	}
	defer value.Free()

	data := value.Data()
	v := make([]byte, len(data))
	copy(v, data)
	return v, nil
}

// LoadWithPrefix returns a batch values of keys with a prefix
// if prefix is "", then load every thing from the database
func (kv *RocksdbKV) LoadWithPrefix(prefix string) ([]string, []string, error) {
	if kv.DB == nil {
		return nil, nil, fmt.Errorf("rocksdb instance is nil when load %s", prefix)
	}
	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()
	iter := NewRocksIteratorWithUpperBound(kv.DB, typeutil.AddOne(prefix), option)
	defer iter.Close()

	var keys, values []string
	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, string(key.Data()))
		values = append(values, string(value.Data()))
		key.Free()
		value.Free()
	}
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

func (kv *RocksdbKV) Has(key string) (bool, error) {
	if kv.DB == nil {
		return false, fmt.Errorf("rocksdb instance is nil when check if has %s", key)
	}

	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()

	value, err := kv.DB.Get(option, []byte(key))
	if err != nil {
		return false, err
	}

	return value.Size() != 0, nil
}

func (kv *RocksdbKV) LoadBytesWithPrefix(prefix string) ([]string, [][]byte, error) {
	if kv.DB == nil {
		return nil, nil, fmt.Errorf("rocksdb instance is nil when load %s", prefix)
	}

	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()

	iter := NewRocksIteratorWithUpperBound(kv.DB, typeutil.AddOne(prefix), option)
	defer iter.Close()

	var (
		keys   []string
		values [][]byte
	)
	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, string(key.Data()))

		data := value.Data()
		v := make([]byte, len(data))
		copy(v, data)
		values = append(values, v)
		key.Free()
		value.Free()
	}
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

// MultiLoad load a batch of values by keys
func (kv *RocksdbKV) MultiLoad(keys []string) ([]string, error) {
	if kv.DB == nil {
		return nil, errors.New("rocksdb instance is nil when do MultiLoad")
	}

	keyInBytes := make([][]byte, 0, len(keys))
	for _, key := range keys {
		keyInBytes = append(keyInBytes, []byte(key))
	}
	values := make([]string, 0, len(keys))
	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()

	valueSlice, err := kv.DB.MultiGet(option, keyInBytes...)
	if err != nil {
		return nil, err
	}
	for i := range valueSlice {
		values = append(values, string(valueSlice[i].Data()))
		valueSlice[i].Free()
	}
	return values, nil
}

func (kv *RocksdbKV) MultiLoadBytes(keys []string) ([][]byte, error) {
	if kv.DB == nil {
		return nil, errors.New("rocksdb instance is nil when do MultiLoad")
	}

	keyInBytes := make([][]byte, 0, len(keys))
	for _, key := range keys {
		keyInBytes = append(keyInBytes, []byte(key))
	}
	values := make([][]byte, 0, len(keys))
	option := gorocksdb.NewDefaultReadOptions()
	defer option.Destroy()

	valueSlice, err := kv.DB.MultiGet(option, keyInBytes...)
	if err != nil {
		return nil, err
	}
	for i := range valueSlice {
		data := valueSlice[i].Data()
		v := make([]byte, len(data))
		copy(v, data)
		values = append(values, v)
		valueSlice[i].Free()
	}
	return values, nil
}

// Save a pair of key-value
func (kv *RocksdbKV) Save(key, value string) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do save")
	}
	if key == "" {
		return errors.New("rocksdb kv does not support empty key")
	}
	if value == "" {
		return errors.New("rocksdb kv does not support empty value")
	}

	return kv.DB.Put(kv.WriteOptions, []byte(key), []byte(value))
}

func (kv *RocksdbKV) SaveBytes(key string, value []byte) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do save")
	}
	if key == "" {
		return errors.New("rocksdb kv does not support empty key")
	}
	if len(value) == 0 {
		return errors.New("rocksdb kv does not support empty value")
	}

	return kv.DB.Put(kv.WriteOptions, []byte(key), value)
}

// MultiSave a batch of key-values
func (kv *RocksdbKV) MultiSave(kvs map[string]string) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do MultiSave")
	}

	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	for k, v := range kvs {
		writeBatch.Put([]byte(k), []byte(v))
	}

	return kv.DB.Write(kv.WriteOptions, writeBatch)
}

func (kv *RocksdbKV) MultiSaveBytes(kvs map[string][]byte) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do MultiSave")
	}

	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	for k, v := range kvs {
		writeBatch.Put([]byte(k), v)
	}

	return kv.DB.Write(kv.WriteOptions, writeBatch)
}

// RemoveWithPrefix removes a batch of key-values with specified prefix
// If prefix is "", then all data in the rocksdb kv will be deleted
func (kv *RocksdbKV) RemoveWithPrefix(prefix string) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do RemoveWithPrefix")
	}
	if len(prefix) == 0 {
		// better to use drop column family, but as we use default column family, we just delete ["",lastKey+1)
		readOpts := gorocksdb.NewDefaultReadOptions()
		defer readOpts.Destroy()
		iter := NewRocksIterator(kv.DB, readOpts)
		defer iter.Close()
		// seek to the last key
		iter.SeekToLast()
		if iter.Valid() {
			return kv.DeleteRange(prefix, typeutil.AddOne(string(iter.Key().Data())))
		}
		// nothing in the range, skip
		return nil
	}
	prefixEnd := typeutil.AddOne(prefix)
	return kv.DeleteRange(prefix, prefixEnd)
}

// Remove is used to remove a pair of key-value
func (kv *RocksdbKV) Remove(key string) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do Remove")
	}
	if key == "" {
		return errors.New("rocksdb kv does not support empty key")
	}
	err := kv.DB.Delete(kv.WriteOptions, []byte(key))
	return err
}

// MultiRemove is used to remove a batch of key-values
func (kv *RocksdbKV) MultiRemove(keys []string) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do MultiRemove")
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	for _, key := range keys {
		writeBatch.Delete([]byte(key))
	}
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// MultiSaveAndRemove provides a transaction to execute a batch of operations
func (kv *RocksdbKV) MultiSaveAndRemove(saves map[string]string, removals []string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do MultiSaveAndRemove")
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	for k, v := range saves {
		writeBatch.Put([]byte(k), []byte(v))
	}
	for _, key := range removals {
		writeBatch.Delete([]byte(key))
	}
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// DeleteRange remove a batch of key-values from startKey to endKey
func (kv *RocksdbKV) DeleteRange(startKey, endKey string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do DeleteRange")
	}
	if startKey >= endKey {
		return fmt.Errorf("rockskv delete range startkey must < endkey, startkey %s, endkey %s", startKey, endKey)
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	writeBatch.DeleteRange([]byte(startKey), []byte(endKey))
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// MultiRemoveWithPrefix is used to remove a batch of key-values with the same prefix
func (kv *RocksdbKV) MultiRemoveWithPrefix(prefixes []string) error {
	if kv.DB == nil {
		return errors.New("rocksdb instance is nil when do RemoveWithPrefix")
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	kv.prepareRemovePrefix(prefixes, writeBatch)
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// MultiSaveAndRemoveWithPrefix is used to execute a batch operators with the same prefix
func (kv *RocksdbKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do MultiSaveAndRemove")
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	for k, v := range saves {
		writeBatch.Put([]byte(k), []byte(v))
	}
	kv.prepareRemovePrefix(removals, writeBatch)
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

func (kv *RocksdbKV) prepareRemovePrefix(prefixes []string, writeBatch *gorocksdb.WriteBatch) {
	// check if any empty prefix, if yes then we delete whole table, no more data should be remained
	for _, prefix := range prefixes {
		if len(prefix) == 0 {
			// better to use drop column family, but as we use default column family, we just delete ["",lastKey+1)
			readOpts := gorocksdb.NewDefaultReadOptions()
			defer readOpts.Destroy()
			iter := NewRocksIterator(kv.DB, readOpts)
			defer iter.Close()
			// seek to the last key
			iter.SeekToLast()
			if iter.Valid() {
				writeBatch.DeleteRange([]byte(prefix), []byte(typeutil.AddOne(string(iter.Key().Data()))))
				return
			}
		}
	}
	// if no empty prefix, prepare range deletion in write batch
	for _, prefix := range prefixes {
		prefixEnd := typeutil.AddOne(prefix)
		writeBatch.DeleteRange([]byte(prefix), []byte(prefixEnd))
	}
}
