package tikv_driver

import (
	"bytes"
	"context"
	"fmt"
	. "github.com/czs007/suvlim/storage/internal/tikv/codec"
	. "github.com/czs007/suvlim/storage/pkg/types"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"sort"
	"testing"
)

//var store TikvStore
var store *TikvStore

func TestMain(m *testing.M) {
	store, _ = NewTikvStore(context.Background())
	exitCode := m.Run()
	_ = store.Close()
	os.Exit(exitCode)
}

func TestTikvEngine_Prefix(t *testing.T) {
	ctx := context.Background()
	prefix := Key("key")
	engine := store.engine
	value := Value("value")

	// Put some key with same prefix
	key := prefix
	err := engine.Put(ctx, key, value)
	assert.Nil(t, err)
	key = EncodeKey(prefix, 0, "")
	err = engine.Put(ctx, key, value)
	assert.Nil(t, err)

	// Get by prefix
	ks, _, err := engine.GetByPrefix(ctx, prefix, true)
	assert.Equal(t, 2, len(ks))

	// Delete by prefix
	err = engine.DeleteByPrefix(ctx, prefix)
	assert.Nil(t, err)
	ks, _, err = engine.GetByPrefix(ctx, prefix, true)
	assert.Equal(t, 0, len(ks))

	//Test large amount keys
	num := engine.conf.Raw.MaxScanLimit + 1
	keys := make([]Key, num)
	values := make([]Value, num)
	for i := 0; i < num; i++ {
		key = EncodeKey(prefix, uint64(i), "")
		keys[i] = key
		values[i] = value
	}
	err = engine.BatchPut(ctx, keys, values)
	assert.Nil(t, err)

	ks, _, err = engine.GetByPrefix(ctx, prefix, true)
	assert.Nil(t, err)
	assert.Equal(t, num, len(ks))
	err = engine.DeleteByPrefix(ctx, prefix)
	assert.Nil(t, err)
}

func TestTikvStore_Row(t *testing.T) {
	ctx := context.Background()
	key := Key("key")

	// Add same row with different timestamp
	err := store.PutRow(ctx, key, Value("value0"), "segment0", 0)
	assert.Nil(t, err)
	err = store.PutRow(ctx, key, Value("value1"), "segment0", 2)
	assert.Nil(t, err)

	// Get most recent row using key and timestamp
	v, err := store.GetRow(ctx, key, 3)
	assert.Nil(t, err)
	assert.Equal(t, Value("value1"), v)
	v, err = store.GetRow(ctx, key, 2)
	assert.Nil(t, err)
	assert.Equal(t, Value("value1"), v)
	v, err = store.GetRow(ctx, key, 1)
	assert.Nil(t, err)
	assert.Equal(t, Value("value0"), v)

	// Add a different row, but with same prefix
	key1 := Key("key_y")
	err = store.PutRow(ctx, key1, Value("valuey"), "segment0", 2)
	assert.Nil(t, err)

	// Get most recent row using key and timestamp
	v, err = store.GetRow(ctx, key, 3)
	assert.Nil(t, err)
	assert.Equal(t, Value("value1"), v)
	v, err = store.GetRow(ctx, key1, 3)
	assert.Nil(t, err)
	assert.Equal(t, Value("valuey"), v)

	// Delete a row
	err = store.DeleteRow(ctx, key, 4)
	assert.Nil(t, nil)
	v, err = store.GetRow(ctx, key, 5)
	assert.Nil(t, err)
	assert.Nil(t, v)

	// Clear test data
	err = store.engine.DeleteByPrefix(ctx, key)
	k, va, err := store.engine.GetByPrefix(ctx, key, false)
	assert.Nil(t, k)
	assert.Nil(t, va)
}

func TestTikvStore_BatchRow(t *testing.T) {
	ctx := context.Background()

	// Prepare test data
	size := 0
	var testKeys []Key
	var testValues []Value
	var segments []string
	for i := 0; size/store.engine.conf.Raw.MaxBatchPutSize < 1; i++ {
		key := fmt.Sprint("key", i)
		size += len(key)
		testKeys = append(testKeys, []byte(key))
		value := fmt.Sprint("value", i)
		size += len(value)
		testValues = append(testValues, []byte(value))
		segments = append(segments, "test")
		v, err := store.GetRow(ctx, Key(key), math.MaxUint64)
		assert.Nil(t, v)
		assert.Nil(t, err)
	}

	// Batch put rows
	err := store.PutRows(ctx, testKeys, testValues, segments, 1)
	assert.Nil(t, err)

	// Batch get rows
	checkValues, err := store.GetRows(ctx, testKeys, 2)
	assert.NotNil(t, checkValues)
	assert.Nil(t, err)
	assert.Equal(t, len(checkValues), len(testValues))
	for i := range testKeys {
		assert.Equal(t, testValues[i], checkValues[i])
	}

	// Delete all test rows
	err = store.DeleteRows(ctx, testKeys, math.MaxUint64)
	assert.Nil(t, err)
	// Ensure all test row is deleted
	checkValues, err = store.GetRows(ctx, testKeys, math.MaxUint64)
	assert.Nil(t, err)
	for _, value := range checkValues {
		assert.Nil(t, value)
	}

	// Clean test data
	err = store.engine.DeleteByPrefix(ctx, Key("key"))
	assert.Nil(t, err)
}

func TestTikvStore_Log(t *testing.T) {
	ctx := context.Background()

	// Put some log
	err := store.PutLog(ctx, Key("key1"), Value("value1"), 1, 1)
	assert.Nil(t, err)
	err = store.PutLog(ctx, Key("key1"), Value("value1_1"), 1, 2)
	assert.Nil(t, err)
	err = store.PutLog(ctx, Key("key2"), Value("value2"), 2, 1)
	assert.Nil(t, err)

	// Check log
	log, err := store.GetLog(ctx, 0, 2, []int{1, 2})
	sort.Slice(log, func(i, j int) bool {
		return bytes.Compare(log[i], log[j]) == -1
	})
	assert.Equal(t, log[0], Value("value1"))
	assert.Equal(t, log[1], Value("value1_1"))
	assert.Equal(t, log[2], Value("value2"))

	// Delete test data
	err = store.engine.DeleteByPrefix(ctx, Key("log"))
	assert.Nil(t, err)
}

//func TestTikvStore_PrefixKey(t *testing.T) {
//	ctx := context.Background()
//	key := Key("key")
//	key1 := Key("key_1")
//
//	// Clean kv data
//	err := store.Delete(ctx, key, math.MaxUint64)
//	assert.Nil(t, err)
//
//	// Ensure test data is not exist
//	v, err := store.Get(ctx, key, math.MaxUint64)
//	assert.Nil(t, v)
//	assert.Nil(t, err)
//	v, err = store.Get(ctx, key1, math.MaxUint64)
//	assert.Nil(t, v)
//	assert.Nil(t, err)
//
//	// Set some value for test key
//	err = store.Set(ctx, key, Value("key_1"), 1)
//	assert.Nil(t, err)
//	err = store.Set(ctx, key1, Value("key1_1"), 1)
//	assert.Nil(t, err)
//
//	// Get Value
//	v, err = store.Get(ctx, key, 1)
//	assert.Nil(t, err)
//	assert.Equal(t, Value("key_1"), v)
//
//	v, err = store.Get(ctx, key1, 1)
//	assert.Nil(t, err)
//	assert.Equal(t, Value("key1_1"), v)
//
//	// Delete key, value for "key" should nil
//	err = store.Delete(ctx, key, 1)
//	v, err = store.Get(ctx, key, 1)
//	assert.Nil(t, v)
//	assert.Nil(t, err)
//
//	// Delete all test data
//	err = store.Delete(ctx, key1, 1)
//	assert.Nil(t, err)
//
//}
//
//func TestTikvStore_Batch(t *testing.T) {
//	ctx := context.Background()
//
//	// Prepare test data
//	size := 0
//	var testKeys []Key
//	var testValues []Value
//	for i := 0; size/conf.Raw.MaxBatchPutSize < 1; i++ {
//		key := fmt.Sprint("key", i)
//		size += len(key)
//		testKeys = append(testKeys, []byte(key))
//		value := fmt.Sprint("value", i)
//		size += len(value)
//		testValues = append(testValues, []byte(value))
//		v, err := store.Get(ctx, Key(key), math.MaxUint64)
//		assert.Nil(t, v)
//		assert.Nil(t, err)
//	}
//
//	// Set kv data
//	err := store.BatchSet(ctx, testKeys, testValues, 1)
//	assert.Nil(t, err)
//
//	// Get value
//	checkValues, err := store.BatchGet(ctx, testKeys, 2)
//	assert.NotNil(t, checkValues)
//	assert.Nil(t, err)
//	assert.Equal(t, len(checkValues), len(testValues))
//	for i := range testKeys {
//		assert.Equal(t, testValues[i], checkValues[i])
//	}
//
//	// Delete test data using multi go routine
//	err = store.BatchDeleteMultiRoutine(ctx, testKeys, math.MaxUint64)
//	assert.Nil(t, err)
//	// Ensure all test key is deleted
//	checkValues, err = store.BatchGet(ctx, testKeys, math.MaxUint64)
//	assert.Nil(t, err)
//	for _, value := range checkValues {
//		assert.Nil(t, value)
//	}
//
//	// Delete test data
//	err = store.BatchDelete(ctx, testKeys, math.MaxUint64)
//	assert.Nil(t, err)
//	// Ensure all test key is deleted
//	checkValues, err = store.BatchGet(ctx, testKeys, math.MaxUint64)
//	assert.Nil(t, err)
//	for _, value := range checkValues {
//		assert.Nil(t, value)
//	}
//}
