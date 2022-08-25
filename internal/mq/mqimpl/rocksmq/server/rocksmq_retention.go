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

package server

import (
	"fmt"
	"path"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"
)

// RocksmqRetentionTimeInMinutes is the time of retention
var RocksmqRetentionTimeInSecs int64
var DefaultRocksmqRetentionTimeInMins int64 = 7200

// RocksmqRetentionSizeInMB is the size of retention
var RocksmqRetentionSizeInMB int64
var DefaultRocksmqRetentionSizeInMB int64 = 8192

// RocksmqRetentionCompactionInterval is the Interval we trigger compaction,
var RocksmqRetentionCompactionInterval int64
var DefaultRocksmqRetentionCompactionInterval int64 = 86400

// Const value that used to convert unit
const (
	MB = 1024 * 1024
)

// TickerTimeInSeconds is the time of expired check, default 10 minutes
var TickerTimeInSeconds int64 = 600

type retentionInfo struct {
	// key is topic name, value is last retention time
	topicRetetionTime sync.Map
	mutex             sync.RWMutex

	kv *rocksdbkv.RocksdbKV
	db *gorocksdb.DB

	closeCh   chan struct{}
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

func initRetentionInfo(params paramtable.BaseTable, kv *rocksdbkv.RocksdbKV, db *gorocksdb.DB) (*retentionInfo, error) {
	rawRmqRetentionTimeInMinutes := params.ParseInt64WithDefault("rocksmq.retentionTimeInMinutes", DefaultRocksmqRetentionTimeInMins)
	atomic.StoreInt64(&RocksmqRetentionTimeInSecs, rawRmqRetentionTimeInMinutes*60)
	atomic.StoreInt64(&RocksmqRetentionSizeInMB, params.ParseInt64WithDefault("rocksmq.retentionSizeInMB", DefaultRocksmqRetentionSizeInMB))
	atomic.StoreInt64(&RocksmqRetentionCompactionInterval, params.ParseInt64WithDefault("rocksmq.compactionInterval", DefaultRocksmqRetentionCompactionInterval))
	ri := &retentionInfo{
		topicRetetionTime: sync.Map{},
		mutex:             sync.RWMutex{},
		kv:                kv,
		db:                db,
		closeCh:           make(chan struct{}),
		closeWg:           sync.WaitGroup{},
	}
	// Get topic from topic begin id
	topicKeys, _, err := ri.kv.LoadWithPrefix(TopicIDTitle)
	if err != nil {
		return nil, err
	}
	for _, key := range topicKeys {
		topic := key[len(TopicIDTitle):]
		ri.topicRetetionTime.Store(topic, time.Now().Unix())
		topicMu.Store(topic, new(sync.Mutex))
	}
	return ri, nil
}

// Before do retention, load retention info from rocksdb to retention info structure in goroutines.
// Because loadRetentionInfo may need some time, so do this asynchronously. Finally start retention goroutine.
func (ri *retentionInfo) startRetentionInfo() {
	// var wg sync.WaitGroup
	ri.closeWg.Add(1)
	go ri.retention()
}

// retention do time ticker and trigger retention check and operation for each topic
func (ri *retentionInfo) retention() error {
	log.Debug("Rocksmq retention goroutine start!")
	// Do retention check every 10 mins
	ticker := time.NewTicker(time.Duration(atomic.LoadInt64(&TickerTimeInSeconds) * int64(time.Second)))
	defer ticker.Stop()
	compactionTicker := time.NewTicker(time.Duration(atomic.LoadInt64(&RocksmqRetentionCompactionInterval) * int64(time.Second)))
	defer compactionTicker.Stop()
	defer ri.closeWg.Done()

	for {
		select {
		case <-ri.closeCh:
			log.Warn("Rocksmq retention finish!")
			return nil
		case <-compactionTicker.C:
			log.Info("trigger rocksdb compaction, should trigger rocksdb data clean")
			go ri.db.CompactRange(gorocksdb.Range{Start: nil, Limit: nil})
			go ri.kv.DB.CompactRange(gorocksdb.Range{Start: nil, Limit: nil})
		case t := <-ticker.C:
			timeNow := t.Unix()
			checkTime := atomic.LoadInt64(&RocksmqRetentionTimeInSecs) / 10
			ri.mutex.RLock()
			ri.topicRetetionTime.Range(func(k, v interface{}) bool {
				topic, _ := k.(string)
				lastRetentionTs, ok := v.(int64)
				if !ok {
					log.Warn("Can't parse lastRetention to int64", zap.String("topic", topic), zap.Any("value", v))
					return true
				}
				if lastRetentionTs+checkTime < timeNow {
					err := ri.expiredCleanUp(topic)
					if err != nil {
						log.Warn("Retention expired clean failed", zap.Any("error", err))
					}
					ri.topicRetetionTime.Store(topic, timeNow)
				}
				return true
			})
			ri.mutex.RUnlock()
		}
	}
}

// Stop close channel and stop retention
func (ri *retentionInfo) Stop() {
	ri.closeOnce.Do(func() {
		close(ri.closeCh)
		ri.closeWg.Wait()
	})
}

// expiredCleanUp check message retention by page:
// 1. check acked timestamp of each page id, if expired, the whole page is expired;
// 2. check acked size from the last unexpired page id;
// 3. delete acked info by range of page id;
// 4. delete message by range of page id;
func (ri *retentionInfo) expiredCleanUp(topic string) error {
	start := time.Now()
	var deletedAckedSize int64
	var pageCleaned UniqueID
	var pageEndID UniqueID
	var err error

	fixedAckedTsKey := constructKey(AckedTsTitle, topic)
	// calculate total acked size, simply add all page info
	totalAckedSize, err := ri.calculateTopicAckedSize(topic)
	if err != nil {
		return err
	}
	// Quick Path, No page to check
	if totalAckedSize == 0 {
		log.Debug("All messages are not expired, skip retention because no ack", zap.Any("topic", topic),
			zap.Any("time taken", time.Since(start).Milliseconds()))
		return nil
	}
	pageReadOpts := gorocksdb.NewDefaultReadOptions()
	defer pageReadOpts.Destroy()
	pageMsgPrefix := constructKey(PageMsgSizeTitle, topic) + "/"

	pageIter := rocksdbkv.NewRocksIteratorWithUpperBound(ri.kv.DB, typeutil.AddOne(pageMsgPrefix), pageReadOpts)
	defer pageIter.Close()
	pageIter.Seek([]byte(pageMsgPrefix))
	for ; pageIter.Valid(); pageIter.Next() {
		pKey := pageIter.Key()
		pageID, err := parsePageID(string(pKey.Data()))
		if pKey != nil {
			pKey.Free()
		}
		if err != nil {
			return err
		}
		ackedTsKey := fixedAckedTsKey + "/" + strconv.FormatInt(pageID, 10)
		ackedTsVal, err := ri.kv.Load(ackedTsKey)
		if err != nil {
			return err
		}
		// not acked page, TODO add TTL info there
		if ackedTsVal == "" {
			break
		}
		ackedTs, err := strconv.ParseInt(ackedTsVal, 10, 64)
		if err != nil {
			return err
		}
		if msgTimeExpiredCheck(ackedTs) {
			pageEndID = pageID
			pValue := pageIter.Value()
			size, err := strconv.ParseInt(string(pValue.Data()), 10, 64)
			if pValue != nil {
				pValue.Free()
			}
			if err != nil {
				return err
			}
			deletedAckedSize += size
			pageCleaned++
		} else {
			break
		}
	}
	if err := pageIter.Err(); err != nil {
		return err
	}

	log.Debug("Expired check by retention time", zap.Any("topic", topic),
		zap.Any("pageEndID", pageEndID), zap.Any("deletedAckedSize", deletedAckedSize),
		zap.Any("pageCleaned", pageCleaned), zap.Any("time taken", time.Since(start).Milliseconds()))

	for ; pageIter.Valid(); pageIter.Next() {
		pValue := pageIter.Value()
		size, err := strconv.ParseInt(string(pValue.Data()), 10, 64)
		if pValue != nil {
			pValue.Free()
		}
		pKey := pageIter.Key()
		pKeyStr := string(pKey.Data())
		if pKey != nil {
			pKey.Free()
		}
		if err != nil {
			return err
		}
		curDeleteSize := deletedAckedSize + size
		if msgSizeExpiredCheck(curDeleteSize, totalAckedSize) {
			pageEndID, err = parsePageID(pKeyStr)
			if err != nil {
				return err
			}
			deletedAckedSize += size
			pageCleaned++
		} else {
			break
		}
	}
	if err := pageIter.Err(); err != nil {
		return err
	}

	if pageEndID == 0 {
		log.Debug("All messages are not expired, skip retention", zap.Any("topic", topic), zap.Any("time taken", time.Since(start).Milliseconds()))
		return nil
	}
	expireTime := time.Since(start).Milliseconds()
	log.Debug("Expired check by message size: ", zap.Any("topic", topic),
		zap.Any("pageEndID", pageEndID), zap.Any("deletedAckedSize", deletedAckedSize),
		zap.Any("pageCleaned", pageCleaned), zap.Any("time taken", expireTime))
	return ri.cleanData(topic, pageEndID)
}

func (ri *retentionInfo) calculateTopicAckedSize(topic string) (int64, error) {
	fixedAckedTsKey := constructKey(AckedTsTitle, topic)

	pageReadOpts := gorocksdb.NewDefaultReadOptions()
	defer pageReadOpts.Destroy()
	pageMsgPrefix := constructKey(PageMsgSizeTitle, topic) + "/"
	// ensure the iterator won't iterate to other topics
	pageIter := rocksdbkv.NewRocksIteratorWithUpperBound(ri.kv.DB, typeutil.AddOne(pageMsgPrefix), pageReadOpts)
	defer pageIter.Close()
	pageIter.Seek([]byte(pageMsgPrefix))
	var ackedSize int64
	for ; pageIter.Valid(); pageIter.Next() {
		key := pageIter.Key()
		pageID, err := parsePageID(string(key.Data()))
		if key != nil {
			key.Free()
		}
		if err != nil {
			return -1, err
		}

		// check if page is acked
		ackedTsKey := fixedAckedTsKey + "/" + strconv.FormatInt(pageID, 10)
		ackedTsVal, err := ri.kv.Load(ackedTsKey)
		if err != nil {
			return -1, err
		}
		// not acked yet, break
		// TODO, Add TTL logic here, mark it as acked if not
		if ackedTsVal == "" {
			break
		}

		// Get page size
		val := pageIter.Value()
		size, err := strconv.ParseInt(string(val.Data()), 10, 64)
		if val != nil {
			val.Free()
		}
		if err != nil {
			return -1, err
		}
		ackedSize += size
	}
	if err := pageIter.Err(); err != nil {
		return -1, err
	}
	return ackedSize, nil
}

func (ri *retentionInfo) cleanData(topic string, pageEndID UniqueID) error {
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	pageMsgPrefix := constructKey(PageMsgSizeTitle, topic)
	fixedAckedTsKey := constructKey(AckedTsTitle, topic)
	pageStartIDKey := pageMsgPrefix + "/"
	pageEndIDKey := pageMsgPrefix + "/" + strconv.FormatInt(pageEndID+1, 10)
	writeBatch.DeleteRange([]byte(pageStartIDKey), []byte(pageEndIDKey))

	pageTsPrefix := constructKey(PageTsTitle, topic)
	pageTsStartIDKey := pageTsPrefix + "/"
	pageTsEndIDKey := pageTsPrefix + "/" + strconv.FormatInt(pageEndID+1, 10)
	writeBatch.DeleteRange([]byte(pageTsStartIDKey), []byte(pageTsEndIDKey))

	ackedStartIDKey := fixedAckedTsKey + "/"
	ackedEndIDKey := fixedAckedTsKey + "/" + strconv.FormatInt(pageEndID+1, 10)
	writeBatch.DeleteRange([]byte(ackedStartIDKey), []byte(ackedEndIDKey))

	ll, ok := topicMu.Load(topic)
	if !ok {
		return fmt.Errorf("topic name = %s not exist", topic)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return fmt.Errorf("get mutex failed, topic name = %s", topic)
	}
	lock.Lock()
	defer lock.Unlock()

	err := DeleteMessages(ri.db, topic, 0, pageEndID)
	if err != nil {
		return err
	}

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()
	err = ri.kv.DB.Write(writeOpts, writeBatch)
	if err != nil {
		return err
	}
	return nil
}

// DeleteMessages in rocksdb by range of [startID, endID)
func DeleteMessages(db *gorocksdb.DB, topic string, startID, endID UniqueID) error {
	// Delete msg by range of startID and endID
	startKey := path.Join(topic, strconv.FormatInt(startID, 10))
	endKey := path.Join(topic, strconv.FormatInt(endID+1, 10))
	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	writeBatch.DeleteRange([]byte(startKey), []byte(endKey))
	opts := gorocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()
	err := db.Write(opts, writeBatch)
	if err != nil {
		return err
	}
	log.Debug("Delete message for topic", zap.String("topic", topic), zap.Int64("startID", startID), zap.Int64("endID", endID))
	return nil
}

func msgTimeExpiredCheck(ackedTs int64) bool {
	if RocksmqRetentionTimeInSecs < 0 {
		return false
	}
	return ackedTs+atomic.LoadInt64(&RocksmqRetentionTimeInSecs) < time.Now().Unix()
}

func msgSizeExpiredCheck(deletedAckedSize, ackedSize int64) bool {
	if RocksmqRetentionSizeInMB < 0 {
		return false
	}
	return ackedSize-deletedAckedSize > atomic.LoadInt64(&RocksmqRetentionSizeInMB)*MB
}
