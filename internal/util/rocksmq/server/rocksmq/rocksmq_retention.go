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

package rocksmq

import (
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	rocksdbkv "github.com/milvus-io/milvus/internal/kv/rocksdb"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/tecbot/gorocksdb"
	"go.uber.org/zap"
)

// RocksmqRetentionTimeInMinutes is the time of retention
var RocksmqRetentionTimeInMinutes int64

// RocksmqRetentionSizeInMB is the size of retention
var RocksmqRetentionSizeInMB int64

// Const value that used to convert unit
const (
	MB     = 1024 * 1024
	MINUTE = 60
)

// TickerTimeInSeconds is the time of expired check, default 10 minutes
var TickerTimeInSeconds int64 = 10 * MINUTE

type retentionInfo struct {
	// key is topic name, value is last retention type
	topics sync.Map
	mutex  sync.RWMutex

	kv *rocksdbkv.RocksdbKV
	db *gorocksdb.DB

	closeCh   chan struct{}
	closeWg   sync.WaitGroup
	closeOnce sync.Once
}

// Interface LoadWithPrefix() in rocksdbkv needs to close db instance first and then reopen,
// which will cause crash when other goroutines operate the db instance. So here implement a
// prefixLoad without reopen db instance.
func prefixLoad(db *gorocksdb.DB, prefix string) ([]string, []string, error) {
	if db == nil {
		return nil, nil, errors.New("Rocksdb instance is nil when do prefixLoad")
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetPrefixSameAsStart(true)
	iter := db.NewIterator(readOpts)
	defer iter.Close()
	keys := make([]string, 0)
	values := make([]string, 0)
	iter.Seek([]byte(prefix))
	for ; iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		keys = append(keys, string(key.Data()))
		key.Free()
		values = append(values, string(value.Data()))
		value.Free()
	}
	return keys, values, nil
}

func initRetentionInfo(kv *rocksdbkv.RocksdbKV, db *gorocksdb.DB) (*retentionInfo, error) {
	ri := &retentionInfo{
		topics:  sync.Map{},
		mutex:   sync.RWMutex{},
		kv:      kv,
		db:      db,
		closeCh: make(chan struct{}),
		closeWg: sync.WaitGroup{},
	}
	// Get topic from topic begin id
	beginIDKeys, _, err := ri.kv.LoadWithPrefix(TopicBeginIDTitle)
	if err != nil {
		return nil, err
	}
	for _, key := range beginIDKeys {
		topic := key[len(TopicBeginIDTitle):]
		ri.topics.Store(topic, time.Now().Unix())
		topicMu.Store(topic, new(sync.Mutex))
	}
	return ri, nil
}

// Before do retention, load retention info from rocksdb to retention info structure in goroutines.
// Because loadRetentionInfo may need some time, so do this asynchronously. Finally start retention goroutine.
func (ri *retentionInfo) startRetentionInfo() {
	// var wg sync.WaitGroup
	ri.kv.ResetPrefixLength(FixedChannelNameLen)
	ri.closeWg.Add(1)
	go ri.retention()
}

// retention do time ticker and trigger retention check and operation for each topic
func (ri *retentionInfo) retention() error {
	log.Debug("Rocksmq retention goroutine start!")
	// Do retention check every 6s
	ticker := time.NewTicker(time.Duration(atomic.LoadInt64(&TickerTimeInSeconds) * int64(time.Second)))
	defer ri.closeWg.Done()

	for {
		select {
		case <-ri.closeCh:
			log.Debug("Rocksmq retention finish!")
			return nil
		case t := <-ticker.C:
			timeNow := t.Unix()
			checkTime := atomic.LoadInt64(&RocksmqRetentionTimeInMinutes) * MINUTE / 10
			ri.mutex.RLock()
			ri.topics.Range(func(k, v interface{}) bool {
				topic, _ := k.(string)
				lastRetentionTs, ok := v.(int64)
				if !ok {
					log.Warn("Can't parse lastRetention to int64", zap.String("topic", topic), zap.Any("value", v))
					return true
				}
				if lastRetentionTs+checkTime < timeNow {
					err := ri.newExpiredCleanUp(topic)
					if err != nil {
						log.Warn("Retention expired clean failed", zap.Any("error", err))
					}
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

func (ri *retentionInfo) newExpiredCleanUp(topic string) error {
	log.Debug("Timeticker triggers an expiredCleanUp task for topic: " + topic)
	var deletedAckedSize int64 = 0
	var startID UniqueID
	var endID UniqueID
	var pageStartID UniqueID = 0
	var err error

	fixedAckedTsKey, _ := constructKey(AckedTsTitle, topic)

	pageReadOpts := gorocksdb.NewDefaultReadOptions()
	defer pageReadOpts.Destroy()
	pageReadOpts.SetPrefixSameAsStart(true)
	pageIter := ri.kv.DB.NewIterator(pageReadOpts)
	defer pageIter.Close()
	pageMsgPrefix, _ := constructKey(PageMsgSizeTitle, topic)
	pageIter.Seek([]byte(pageMsgPrefix))
	if pageIter.Valid() {
		pageStartID, err = strconv.ParseInt(string(pageIter.Key().Data())[FixedChannelNameLen+1:], 10, 64)
		if err != nil {
			return err
		}

		for ; pageIter.Valid(); pageIter.Next() {
			pKey := pageIter.Key()
			pageID, err := strconv.ParseInt(string(pKey.Data())[FixedChannelNameLen+1:], 10, 64)
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
			ackedTs, err := strconv.ParseInt(ackedTsVal, 10, 64)
			if err != nil {
				return err
			}
			if msgTimeExpiredCheck(ackedTs) {
				endID = pageID
				pValue := pageIter.Value()
				size, err := strconv.ParseInt(string(pValue.Data()), 10, 64)
				if pValue != nil {
					pValue.Free()
				}
				if err != nil {
					return err
				}
				deletedAckedSize += size
			} else {
				break
			}
		}
	}

	pageEndID := endID

	ackedReadOpts := gorocksdb.NewDefaultReadOptions()
	defer ackedReadOpts.Destroy()
	ackedReadOpts.SetPrefixSameAsStart(true)
	ackedIter := ri.kv.DB.NewIterator(ackedReadOpts)
	defer ackedIter.Close()
	if err != nil {
		return err
	}
	ackedIter.Seek([]byte(fixedAckedTsKey))
	if !ackedIter.Valid() {
		return nil
	}

	startID, err = strconv.ParseInt(string(ackedIter.Key().Data())[FixedChannelNameLen+1:], 10, 64)
	if err != nil {
		return err
	}
	if endID > startID {
		newPos := fixedAckedTsKey + "/" + strconv.FormatInt(endID, 10)
		ackedIter.Seek([]byte(newPos))
	}

	for ; ackedIter.Valid(); ackedIter.Next() {
		aKey := ackedIter.Key()
		aValue := ackedIter.Value()
		ackedTs, err := strconv.ParseInt(string(aValue.Data()), 10, 64)
		if aValue != nil {
			aValue.Free()
		}
		if err != nil {
			if aKey != nil {
				aKey.Free()
			}
			return err
		}
		if msgTimeExpiredCheck(ackedTs) {
			endID, err = strconv.ParseInt(string(aKey.Data())[FixedChannelNameLen+1:], 10, 64)
			if aKey != nil {
				aKey.Free()
			}
			if err != nil {
				return err
			}
		} else {
			if aKey != nil {
				aKey.Free()
			}
			break
		}
	}

	if endID == 0 {
		log.Debug("All messages are not time expired")
	}
	log.Debug("Expired check by retention time", zap.Any("topic", topic), zap.Any("startID", startID), zap.Any("endID", endID), zap.Any("deletedAckedSize", deletedAckedSize))

	ackedSizeKey := AckedSizeTitle + topic
	totalAckedSizeVal, err := ri.kv.Load(ackedSizeKey)
	if err != nil {
		return err
	}
	totalAckedSize, err := strconv.ParseInt(totalAckedSizeVal, 10, 64)
	if err != nil {
		return err
	}

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
			endID, err = strconv.ParseInt(pKeyStr[FixedChannelNameLen+1:], 10, 64)
			if err != nil {
				return err
			}
			pageEndID = endID
			deletedAckedSize += size
		} else {
			break
		}
	}
	if endID == 0 {
		log.Debug("All messages are not expired")
		return nil
	}
	log.Debug("ExpiredCleanUp: ", zap.Any("topic", topic), zap.Any("startID", startID), zap.Any("endID", endID), zap.Any("deletedAckedSize", deletedAckedSize))

	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()

	pageStartIDKey := pageMsgPrefix + "/" + strconv.FormatInt(pageStartID, 10)
	pageEndIDKey := pageMsgPrefix + "/" + strconv.FormatInt(pageEndID+1, 10)
	if pageStartID == pageEndID {
		if pageStartID != 0 {
			writeBatch.Delete([]byte(pageStartIDKey))
		}
	} else if pageStartID < pageEndID {
		writeBatch.DeleteRange([]byte(pageStartIDKey), []byte(pageEndIDKey))
	}

	ackedStartIDKey := fixedAckedTsKey + "/" + strconv.Itoa(int(startID))
	ackedEndIDKey := fixedAckedTsKey + "/" + strconv.Itoa(int(endID+1))
	if startID > endID {
		return nil
	} else if startID == endID {
		writeBatch.Delete([]byte(ackedStartIDKey))
	} else {
		writeBatch.DeleteRange([]byte(ackedStartIDKey), []byte(ackedEndIDKey))
	}

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
	currentAckedSizeVal, err := ri.kv.Load(ackedSizeKey)
	if err != nil {
		return err
	}
	currentAckedSize, err := strconv.ParseInt(currentAckedSizeVal, 10, 64)
	if err != nil {
		return err
	}
	newAckedSize := currentAckedSize - deletedAckedSize
	writeBatch.Put([]byte(ackedSizeKey), []byte(strconv.FormatInt(newAckedSize, 10)))

	err = DeleteMessages(ri.db, topic, startID, endID)
	if err != nil {
		return err
	}

	writeOpts := gorocksdb.NewDefaultWriteOptions()
	defer writeOpts.Destroy()
	ri.kv.DB.Write(writeOpts, writeBatch)

	return nil
}

// DeleteMessages in rocksdb by range of [startID, endID)
func DeleteMessages(db *gorocksdb.DB, topic string, startID, endID UniqueID) error {
	// Delete msg by range of startID and endID
	startKey, err := combKey(topic, startID)
	if err != nil {
		log.Debug("RocksMQ: combKey(" + topic + "," + strconv.FormatInt(startID, 10) + ")")
		return err
	}
	endKey, err := combKey(topic, endID+1)
	if err != nil {
		log.Debug("RocksMQ: combKey(" + topic + "," + strconv.FormatInt(endID, 10) + ")")
		return err
	}

	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Destroy()
	if startID == endID {
		writeBatch.Delete([]byte(startKey))
	} else {
		writeBatch.DeleteRange([]byte(startKey), []byte(endKey))
	}
	opts := gorocksdb.NewDefaultWriteOptions()
	defer opts.Destroy()
	err = db.Write(opts, writeBatch)
	if err != nil {
		return err
	}

	log.Debug("Delete message for topic: "+topic, zap.Any("startID", startID), zap.Any("endID", endID))

	return nil
}

func msgTimeExpiredCheck(ackedTs int64) bool {
	return ackedTs+atomic.LoadInt64(&RocksmqRetentionTimeInMinutes)*MINUTE < time.Now().Unix()
}

func msgSizeExpiredCheck(deletedAckedSize, ackedSize int64) bool {
	return ackedSize-deletedAckedSize > atomic.LoadInt64(&RocksmqRetentionSizeInMB)*MB
}
