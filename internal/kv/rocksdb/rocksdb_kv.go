// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package rocksdbkv

import (
	"errors"
	"fmt"

	"github.com/tecbot/gorocksdb"
)

// RocksdbKV is KV implemented by rocksdb
type RocksdbKV struct {
	Opts         *gorocksdb.Options
	DB           *gorocksdb.DB
	WriteOptions *gorocksdb.WriteOptions
	ReadOptions  *gorocksdb.ReadOptions
	name         string
}

const (
	// LRUCacheSize is the lru cache size of rocksdb, default 3 << 30
	LRUCacheSize = 3 << 30
)

// NewRocksdbKV returns a rockskv object
func NewRocksdbKV(name string) (*RocksdbKV, error) {
	if name == "" {
		return nil, errors.New("rocksdb name is nil")
	}
	bbto := gorocksdb.NewDefaultBlockBasedTableOptions()
	bbto.SetCacheIndexAndFilterBlocks(true)
	bbto.SetPinL0FilterAndIndexBlocksInCache(true)
	bbto.SetBlockCache(gorocksdb.NewLRUCache(LRUCacheSize))
	opts := gorocksdb.NewDefaultOptions()
	opts.SetBlockBasedTableFactory(bbto)
	opts.SetCreateIfMissing(true)

	ro := gorocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	wo := gorocksdb.NewDefaultWriteOptions()
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
		return "", fmt.Errorf("Rocksdb instance is nil when load %s", key)
	}

	value, err := kv.DB.Get(kv.ReadOptions, []byte(key))
	if err != nil {
		return "", err
	}
	defer value.Free()
	return string(value.Data()), nil
}

// LoadWithPrefix returns a batch values of keys with a prefix
func (kv *RocksdbKV) LoadWithPrefix(key string) ([]string, []string, error) {
	if key == "" {
		return nil, nil, errors.New("Key is nil in LoadWithPrefix")
	}
	if kv.DB == nil {
		return nil, nil, fmt.Errorf("Rocksdb instance is nil when load %s", key)
	}
	kv.ReadOptions.SetPrefixSameAsStart(true)
	kv.DB.Close()
	kv.Opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(len(key)))
	var err error
	kv.DB, err = gorocksdb.OpenDb(kv.Opts, kv.GetName())
	if err != nil {
		return nil, nil, err
	}

	iter := kv.DB.NewIterator(kv.ReadOptions)
	defer iter.Close()
	keys := make([]string, 0)
	values := make([]string, 0)
	iter.Seek([]byte(key))
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

// ResetPrefixLength will close rocksdb object and open a new rocksdb with new prefix length
func (kv *RocksdbKV) ResetPrefixLength(len int) error {
	kv.DB.Close()
	kv.Opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(len))
	var err error
	kv.DB, err = gorocksdb.OpenDb(kv.Opts, kv.GetName())
	return err
}

// MultiLoad load a batch of values by keys
func (kv *RocksdbKV) MultiLoad(keys []string) ([]string, error) {
	if kv.DB == nil {
		return nil, errors.New("Rocksdb instance is nil when do MultiLoad")
	}
	values := make([]string, 0, len(keys))
	for _, key := range keys {
		value, err := kv.DB.Get(kv.ReadOptions, []byte(key))
		if err != nil {
			return []string{}, err
		}
		values = append(values, string(value.Data()))
		value.Free()
	}
	return values, nil
}

// Save a pair of key-value
func (kv *RocksdbKV) Save(key, value string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do save")
	}
	err := kv.DB.Put(kv.WriteOptions, []byte(key), []byte(value))
	return err
}

// MultiSave a batch of key-values
func (kv *RocksdbKV) MultiSave(kvs map[string]string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do MultiSave")
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	for k, v := range kvs {
		writeBatch.Put([]byte(k), []byte(v))
	}
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// RemoveWithPrefix removes a batch of key-values with specified prefix
func (kv *RocksdbKV) RemoveWithPrefix(prefix string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do RemoveWithPrefix")
	}
	kv.ReadOptions.SetPrefixSameAsStart(true)
	kv.DB.Close()
	kv.Opts.SetPrefixExtractor(gorocksdb.NewFixedPrefixTransform(len(prefix)))
	var err error
	kv.DB, err = gorocksdb.OpenDb(kv.Opts, kv.GetName())
	if err != nil {
		return err
	}

	iter := kv.DB.NewIterator(kv.ReadOptions)
	defer iter.Close()
	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		err := kv.DB.Delete(kv.WriteOptions, key.Data())
		key.Free()
		if err != nil {
			return nil
		}
	}
	if err := iter.Err(); err != nil {
		return err
	}
	return nil
}

// Remove is used to remove a pair of key-value
func (kv *RocksdbKV) Remove(key string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do Remove")
	}
	err := kv.DB.Delete(kv.WriteOptions, []byte(key))
	return err
}

// MultiRemove is used to remove a batch of key-values
func (kv *RocksdbKV) MultiRemove(keys []string) error {
	if kv.DB == nil {
		return errors.New("Rocksdb instance is nil when do MultiRemove")
	}
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	for _, key := range keys {
		writeBatch.Delete([]byte(key))
	}
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// MultiSaveAndRemove provides a transaction to execute a batch of operators
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
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	if len(startKey) == 0 {
		iter := kv.DB.NewIterator(kv.ReadOptions)
		defer iter.Close()
		iter.SeekToFirst()
		startKey = string(iter.Key().Data())
	}

	writeBatch.DeleteRange([]byte(startKey), []byte(endKey))
	err := kv.DB.Write(kv.WriteOptions, writeBatch)
	return err
}

// MultiRemoveWithPrefix is used to remove a batch of key-values with the same prefix
func (kv *RocksdbKV) MultiRemoveWithPrefix(keys []string) error {
	panic("not implement")
}

// MultiSaveAndRemoveWithPrefix is used to execute a batch operators with the same prefix
func (kv *RocksdbKV) MultiSaveAndRemoveWithPrefix(saves map[string]string, removals []string) error {
	panic("not implement")
}
