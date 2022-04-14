package storage

import (
	"go.uber.org/zap"
	"golang.org/x/exp/mmap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/cache"
)

type LevelCache struct {
	memCache         *cache.LRU
	localFileCache   *cache.LRU
	localCacheEnable bool

	memoryHitsCount     int
	localCacheHitsCount int
}

func (l *LevelCache) Add(key cache.Key, value cache.Value) {
	l.memCache.Add(key, value)
}

func (l *LevelCache) Get(key cache.Key) (value cache.Value, ok bool) {
	if v, ok := l.memCache.Get(key); ok {
		l.memoryHitsCount++
		return transValue(v)
	}
	if l.localCacheEnable {
		if v, ok := l.localFileCache.Get(key); ok {
			l.localCacheHitsCount++
			l.memCache.Add(key, v)
			return transValue(v)
		}
	}
	return nil, false
}

func transValue(v cache.Value) ([]byte, bool) {
	if at, ok := v.(*mmap.ReaderAt); ok {
		p := make([]byte, at.Len())
		_, err := at.ReadAt(p, 0)
		if err != nil {
			log.Debug("translate value failed", zap.Error(err))
			return nil, false
		}
		return p, true
	} else if data, ok := v.([]byte); ok {
		return data, true
	}
	return nil, false
}

func (l *LevelCache) Remove(key cache.Key) {
	l.memCache.Remove(key)
}

func (l *LevelCache) Contains(key cache.Key) bool {
	if l.memCache.Contains(key) {
		return true
	}
	if l.localCacheEnable {
		if l.localFileCache.Contains(key) {
			return true
		}
	}
	return false
}

func (l *LevelCache) Purge() {
	l.memCache.Purge()
	if l.localCacheEnable {
		l.localFileCache.Purge()
	}
}

func (l *LevelCache) LocalCacheEnable() bool {
	return l.localCacheEnable
}

func (l *LevelCache) Close() {
	l.memCache.Close()
	if l.localCacheEnable {
		l.localFileCache.Close()
	}
}

func NewLevelCache(memLimit, localFileLimit uint64, cm ChunkManager, localCacheEnable bool) (*LevelCache, error) {
	localStoreHelper := &cache.StoreHelper{
		Store: func(key cache.Key, value cache.Value) *cache.Entry {
			err := cm.Write(key.(string), value.([]byte))
			if err != nil {
				log.Error("cache store helper store failed", zap.Error(err))
			}
			return &cache.Entry{Key: key, Value: nil}
		},
		Load: func(entry *cache.Entry) (cache.Value, bool) {
			result, err := cm.Mmap(entry.Key.(string))
			if err != nil {
				log.Error("cache store helper load failed", zap.Error(err))
				return nil, false
			}
			return result, true
		},
		OnEvicted: func(k cache.Key, v cache.Value) {
			err := cm.Remove(k.(string))
			if err != nil {
				log.Error("store helper remove file failed", zap.Error(err))
			}
		},
		MeasureSize: func(value cache.Value) int {
			if v, ok := value.([]byte); ok {
				return len(v)
			}
			return value.(*mmap.ReaderAt).Len()
		},
	}
	localFileCache, err := cache.NewLRU(localFileLimit, cache.SetStoreHelper(localStoreHelper))
	if err != nil {
		return nil, err
	}

	memStoreHelper := &cache.StoreHelper{
		Store: func(key cache.Key, value cache.Value) *cache.Entry {
			return &cache.Entry{Key: key, Value: value}
		},
		Load: func(entry *cache.Entry) (cache.Value, bool) {
			return entry.Value, true
		},
		MeasureSize: func(value cache.Value) int {
			if v, ok := value.([]byte); ok {
				return len(v)
			}
			return value.(*mmap.ReaderAt).Len()
		},
		OnEvicted: func(key cache.Key, value cache.Value) {
			var data []byte
			if r, ok := value.(*mmap.ReaderAt); ok {
				log.Debug("mmap")
				content := make([]byte, r.Len())
				_, err := r.ReadAt(content, 0)
				if err != nil {
					log.Debug("read from mmap failed")
				}
				r.Close()
			} else {
				log.Debug("value")
				data = value.([]byte)
			}
			localFileCache.Add(key, data)
		},
	}
	memCache, err := cache.NewLRU(memLimit, cache.SetStoreHelper(memStoreHelper))
	if err != nil {
		return nil, err
	}
	return &LevelCache{
		memCache:         memCache,
		localFileCache:   localFileCache,
		localCacheEnable: localCacheEnable,
	}, nil
}
