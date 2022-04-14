package storage

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewLevelCache(t *testing.T) {
	cm := NewLocalChunkManager(RootPath(defaultLocalTestPath))
	c, err := NewLevelCache(1, 1, cm, true)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	c, err = NewLevelCache(0, 1, cm, true)
	assert.Error(t, err)
	assert.Nil(t, c)

	c, err = NewLevelCache(1, 0, cm, true)
	assert.Error(t, err)
	assert.Nil(t, c)
}

func TestLevelCache_Add(t *testing.T) {
	cm := NewLocalChunkManager(RootPath(defaultLocalTestPath))
	c, err := NewLevelCache(16, 16, cm, false)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	testKey1 := "test_key_1"
	testValue1 := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	testValueExtra := []byte{4, 4, 4, 4, 4, 4, 4, 4}
	testKey2 := "test_key_2"
	testValue2 := []byte{2, 2, 2, 2, 2, 2, 2, 2}

	testKey3 := "test_key_3"
	testValue3 := []byte{3, 3, 3, 3, 3, 3, 3, 3}

	c.Add(testKey1, testValue1)
	exist := c.Contains(testKey1)
	assert.True(t, exist)
	c.Add(testKey2, testValue2)
	exist = c.Contains(testKey2)
	assert.True(t, exist)

	v, ok := c.Get(testKey1)
	assert.True(t, ok)
	assert.EqualValues(t, testValue1, v)
	assert.Equal(t, c.memoryHitsCount, 1)
	assert.Equal(t, c.localCacheHitsCount, 0)

	v, ok = c.Get(testKey2)
	assert.True(t, ok)
	assert.EqualValues(t, testValue2, v)
	assert.Equal(t, c.memoryHitsCount, 2)
	assert.Equal(t, c.localCacheHitsCount, 0)

	c.Add(testKey1, testValueExtra)

	c.Add(testKey3, testValue3)
	v, ok = c.Get(testKey3)
	assert.True(t, ok)
	assert.EqualValues(t, testValue3, v)
	assert.Equal(t, c.memoryHitsCount, 3)
	assert.Equal(t, c.localCacheHitsCount, 0)

	v, ok = c.Get(testKey2)
	assert.False(t, ok)
	assert.Nil(t, v)
	assert.Equal(t, c.memoryHitsCount, 3)
	assert.Equal(t, c.localCacheHitsCount, 0)

	c.Purge()
}
func TestLevelCacheLocalCacheEnable_Add(t *testing.T) {
	cm := NewLocalChunkManager(RootPath(defaultLocalTestPath))
	c, err := NewLevelCache(8, 16, cm, true)
	assert.NoError(t, err)
	assert.NotNil(t, c)

	testKey1 := "test_key_1"
	testValue1 := []byte{1, 1, 1, 1, 1, 1, 1, 1}
	testValueExtra := []byte{4, 4, 4, 4, 4, 4, 4, 4}
	testKey2 := "test_key_2"
	testValue2 := []byte{2, 2, 2, 2, 2, 2, 2, 2}
	testKey3 := "test_key_3"
	testValue3 := []byte{3, 3, 3, 3, 3, 3, 3, 3}

	c.Add(testKey1, testValue1)
	exist := c.Contains(testKey1)
	assert.True(t, exist)
	c.Add(testKey2, testValue2)
	exist = c.Contains(testKey2)
	assert.True(t, exist)

	assert.Eventually(t, func() bool {
		return c.Contains(testKey1)
	}, 100*time.Millisecond, 1*time.Millisecond)
	v, ok := c.Get(testKey1)
	assert.True(t, ok)
	assert.EqualValues(t, testValue1, v)
	assert.Equal(t, c.memoryHitsCount, 0)
	assert.Equal(t, c.localCacheHitsCount, 1)
	time.Sleep(100 * time.Millisecond)

	assert.Eventually(t, func() bool {
		return c.Contains(testKey1)
	}, 100*time.Millisecond, 1*time.Millisecond)
	v, ok = c.Get(testKey2)
	assert.True(t, ok)
	assert.EqualValues(t, testValue2, v)
	assert.Equal(t, c.memoryHitsCount, 0)
	assert.Equal(t, c.localCacheHitsCount, 2)

	assert.Eventually(t, func() bool {
		return c.Contains(testKey2)
	}, 100*time.Millisecond, 1*time.Millisecond)
	v, ok = c.Get(testKey2)
	assert.True(t, ok)
	assert.EqualValues(t, testValue2, v)
	assert.Equal(t, c.memoryHitsCount, 1)
	assert.Equal(t, c.localCacheHitsCount, 2)

	c.Add(testKey1, testValueExtra)

	c.Add(testKey3, testValue3)
	assert.Eventually(t, func() bool {
		return c.Contains(testKey1)
	}, 100*time.Millisecond, 1*time.Millisecond)
	v, ok = c.Get(testKey3)
	assert.EqualValues(t, testValue3, v)
	assert.Equal(t, c.memoryHitsCount, 2)
	assert.Equal(t, c.localCacheHitsCount, 2)

	c.Purge()
}
