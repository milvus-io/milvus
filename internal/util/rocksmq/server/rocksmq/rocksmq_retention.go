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
	"context"
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

var RocksmqRetentionTimeInMinutes int64
var RocksmqRetentionSizeInMB int64
var TickerTimeInMinutes int64 = 1

const (
	MB     = 2 << 20
	MINUTE = 60
)

type topicPageInfo struct {
	pageEndID   []UniqueID
	pageMsgSize map[UniqueID]int64
}

type topicAckedInfo struct {
	topicBeginID UniqueID
	// TODO(yukun): may need to delete ackedTs
	ackedTs   map[UniqueID]UniqueID
	ackedSize int64
}

type retentionInfo struct {
	ctx    context.Context
	topics []string
	// pageInfo  map[string]*topicPageInfo
	pageInfo sync.Map
	// ackedInfo map[string]*topicAckedInfo
	ackedInfo sync.Map
	// Key is last_retention_time/${topic}
	// lastRetentionTime map[string]int64
	lastRetentionTime sync.Map

	kv *rocksdbkv.RocksdbKV
	db *gorocksdb.DB
}

// Interface LoadWithPrefix() in rocksdbkv needs to close db instance first and then reopen,
// which will cause crash when other goroutines operate the db instance. So here implement a
// prefixLoad without reopen db instance.
func prefixLoad(db *gorocksdb.DB, prefix string) ([]string, []string, error) {
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
	if err := iter.Err(); err != nil {
		return nil, nil, err
	}
	return keys, values, nil
}

func initRetentionInfo(kv *rocksdbkv.RocksdbKV, db *gorocksdb.DB) (*retentionInfo, error) {
	ri := &retentionInfo{
		ctx:               context.Background(),
		topics:            make([]string, 0),
		pageInfo:          sync.Map{},
		ackedInfo:         sync.Map{},
		lastRetentionTime: sync.Map{},
		kv:                kv,
		db:                db,
	}
	// Get topic from topic begin id
	beginIDKeys, _, err := ri.kv.LoadWithPrefix(TopicBeginIDTitle)
	if err != nil {
		return nil, err
	}
	for _, key := range beginIDKeys {
		topic := key[len(TopicBeginIDTitle):]
		ri.topics = append(ri.topics, topic)
		topicMu.Store(topic, new(sync.Mutex))
	}
	return ri, nil
}

func (ri *retentionInfo) startRetentionInfo() error {
	var wg sync.WaitGroup
	ri.kv.ResetPrefixLength(FixedChannelNameLen)
	for _, topic := range ri.topics {
		log.Debug("Start load retention info", zap.Any("topic", topic))
		// Load all page infos
		wg.Add(1)
		go ri.loadRetentionInfo(topic, &wg)
	}
	wg.Wait()
	log.Debug("Finish load retention info, start retention")
	go ri.retention()

	return nil
}

func (ri *retentionInfo) loadRetentionInfo(topic string, wg *sync.WaitGroup) {
	// TODO(yukun): If there needs to add lock
	// ll, ok := topicMu.Load(topic)
	// if !ok {
	// 	return fmt.Errorf("topic name = %s not exist", topic)
	// }
	// lock, ok := ll.(*sync.Mutex)
	// if !ok {
	// 	return fmt.Errorf("get mutex failed, topic name = %s", topic)
	// }
	// lock.Lock()
	// defer lock.Unlock()
	defer wg.Done()
	pageEndID := make([]UniqueID, 0)
	pageMsgSize := make(map[int64]UniqueID)

	fixedPageSizeKey, err := constructKey(PageMsgSizeTitle, topic)
	if err != nil {
		log.Debug("ConstructKey failed", zap.Any("error", err))
		return
	}
	pageMsgSizePrefix := fixedPageSizeKey + "/"
	pageMsgSizeKeys, pageMsgSizeVals, err := prefixLoad(ri.kv.DB, pageMsgSizePrefix)
	if err != nil {
		log.Debug("PrefixLoad failed", zap.Any("error", err))
		return
	}
	for i, key := range pageMsgSizeKeys {
		endID, err := strconv.ParseInt(key[FixedChannelNameLen+1:], 10, 64)
		if err != nil {
			log.Debug("ParseInt failed", zap.Any("error", err))
			return
		}
		pageEndID = append(pageEndID, endID)

		msgSize, err := strconv.ParseInt(pageMsgSizeVals[i], 10, 64)
		if err != nil {
			log.Debug("ParseInt failed", zap.Any("error", err))
			return
		}
		pageMsgSize[endID] = msgSize
	}
	topicPageInfo := &topicPageInfo{
		pageEndID:   pageEndID,
		pageMsgSize: pageMsgSize,
	}

	// Load all acked infos
	ackedTs := make(map[UniqueID]UniqueID)

	topicBeginIDKey := TopicBeginIDTitle + topic
	topicBeginIDVal, err := ri.kv.Load(topicBeginIDKey)
	if err != nil {
		return
	}
	topicBeginID, err := strconv.ParseInt(topicBeginIDVal, 10, 64)
	if err != nil {
		log.Debug("ParseInt failed", zap.Any("error", err))
		return
	}

	ackedTsPrefix, err := constructKey(AckedTsTitle, topic)
	if err != nil {
		log.Debug("ConstructKey failed", zap.Any("error", err))
		return
	}
	keys, vals, err := prefixLoad(ri.kv.DB, ackedTsPrefix)
	if err != nil {
		log.Debug("PrefixLoad failed", zap.Any("error", err))
		return
	}

	for i, key := range keys {
		offset := FixedChannelNameLen + 1
		ackedID, err := strconv.ParseInt((key)[offset:], 10, 64)
		if err != nil {
			log.Debug("RocksMQ: parse int " + key[offset:] + " failed")
			return
		}

		ts, err := strconv.ParseInt(vals[i], 10, 64)
		if err != nil {
			return
		}
		ackedTs[ackedID] = ts
	}

	ackedSizeKey := AckedSizeTitle + topic
	ackedSizeVal, err := ri.kv.Load(ackedSizeKey)
	if err != nil {
		log.Debug("Load failed", zap.Any("error", err))
		return
	}
	ackedSize, err := strconv.ParseInt(ackedSizeVal, 10, 64)
	if err != nil {
		log.Debug("PrefixLoad failed", zap.Any("error", err))
		return
	}

	ackedInfo := &topicAckedInfo{
		topicBeginID: topicBeginID,
		ackedTs:      ackedTs,
		ackedSize:    ackedSize,
	}

	//Load last retention timestamp
	lastRetentionTsKey := LastRetTsTitle + topic
	lastRetentionTsVal, err := ri.kv.Load(lastRetentionTsKey)
	if err != nil {
		log.Debug("Load failed", zap.Any("error", err))
		return
	}
	lastRetentionTs, err := strconv.ParseInt(lastRetentionTsVal, 10, 64)
	if err != nil {
		log.Debug("ParseInt failed", zap.Any("error", err))
		return
	}

	ri.ackedInfo.Store(topic, ackedInfo)
	ri.pageInfo.Store(topic, topicPageInfo)
	ri.lastRetentionTime.Store(topic, lastRetentionTs)
}

func (ri *retentionInfo) retention() error {
	log.Debug("Rocksmq retention goroutine start!")
	ticker := time.NewTicker(time.Duration(TickerTimeInMinutes * int64(time.Minute) / 10))

	for {
		select {
		case <-ri.ctx.Done():
			return nil
		case t := <-ticker.C:
			timeNow := t.Unix()
			checkTime := atomic.LoadInt64(&RocksmqRetentionTimeInMinutes) * 60 / 10
			log.Debug("In ticker: ", zap.Any("ticker", timeNow))
			ri.lastRetentionTime.Range(func(k, v interface{}) bool {
				if v.(int64)+checkTime < timeNow {
					err := ri.expiredCleanUp(k.(string))
					if err != nil {
						log.Warn("Retention expired clean failed", zap.Any("error", err))
					}
				}
				return true
			})
		}
	}
}

func (ri *retentionInfo) expiredCleanUp(topic string) error {
	// log.Debug("Timeticker triggers an expiredCleanUp task for topic: " + topic)
	var ackedInfo *topicAckedInfo
	if info, ok := ri.ackedInfo.Load(topic); ok {
		ackedInfo = info.(*topicAckedInfo)
	} else {
		log.Debug("Topic " + topic + " doesn't have acked infos")
		return nil
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

	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetPrefixSameAsStart(true)
	iter := ri.kv.DB.NewIterator(readOpts)
	defer iter.Close()
	ackedTsPrefix, err := constructKey(AckedTsTitle, topic)
	if err != nil {
		return err
	}
	iter.Seek([]byte(ackedTsPrefix))
	if !iter.Valid() {
		return nil
	}
	var startID UniqueID
	var endID UniqueID
	endID = 0
	startID, err = strconv.ParseInt(string(iter.Key().Data())[FixedChannelNameLen+1:], 10, 64)
	if err != nil {
		return err
	}

	var deletedAckedSize int64 = 0
	pageRetentionOffset := 0
	var pageInfo *topicPageInfo
	if info, ok := ri.pageInfo.Load(topic); ok {
		pageInfo = info.(*topicPageInfo)
	}
	if pageInfo != nil {
		for i, pageEndID := range pageInfo.pageEndID {
			// Clean by RocksmqRetentionTimeInMinutes
			if msgTimeExpiredCheck(ackedInfo.ackedTs[pageEndID]) {
				// All of the page expired, set the pageEndID to current endID
				endID = pageEndID
				fixedAckedTsKey, err := constructKey(AckedTsTitle, topic)
				if err != nil {
					return err
				}
				newKey := fixedAckedTsKey + "/" + strconv.Itoa(int(pageEndID))
				iter.Seek([]byte(newKey))
				pageRetentionOffset = i + 1

				deletedAckedSize += pageInfo.pageMsgSize[pageEndID]
				delete(pageInfo.pageMsgSize, pageEndID)
			}
		}
	}
	log.Debug("Expired check by page info", zap.Any("topic", topic), zap.Any("pageEndID", endID), zap.Any("deletedAckedSize", deletedAckedSize))

	pageEndID := endID
	// The end msg of the page is not expired, find the last expired msg in this page
	for ; iter.Valid(); iter.Next() {
		ackedTs, err := strconv.ParseInt(string(iter.Value().Data()), 10, 64)
		if err != nil {
			return err
		}
		if msgTimeExpiredCheck(ackedTs) {
			endID, err = strconv.ParseInt(string(iter.Key().Data())[FixedChannelNameLen+1:], 10, 64)
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	log.Debug("Expired check by retention time", zap.Any("topic", topic), zap.Any("startID", startID), zap.Any("endID", endID), zap.Any("deletedAckedSize", deletedAckedSize))
	// if endID == 0 {
	// 	log.Debug("All messages are not expired")
	// 	return nil
	// }

	// Delete page message size in rocksdb_kv
	if pageInfo != nil {
		// Judge expire by ackedSize
		if msgSizeExpiredCheck(deletedAckedSize, ackedInfo.ackedSize) {
			for _, pEndID := range pageInfo.pageEndID[pageRetentionOffset:] {
				curDeletedSize := deletedAckedSize + pageInfo.pageMsgSize[pEndID]
				if msgSizeExpiredCheck(curDeletedSize, ackedInfo.ackedSize) {
					endID = pEndID
					pageEndID = pEndID
					deletedAckedSize = curDeletedSize
					delete(pageInfo.pageMsgSize, pEndID)
				} else {
					break
				}
			}
			log.Debug("Expired check by retention size", zap.Any("topic", topic), zap.Any("new endID", endID), zap.Any("new deletedAckedSize", deletedAckedSize))
		}

		if pageEndID > 0 && len(pageInfo.pageEndID) > 0 {
			pageStartID := pageInfo.pageEndID[0]
			fixedPageSizeKey, err := constructKey(PageMsgSizeTitle, topic)
			if err != nil {
				return err
			}
			pageStartKey := fixedPageSizeKey + "/" + strconv.Itoa(int(pageStartID))
			pageEndKey := fixedPageSizeKey + "/" + strconv.Itoa(int(pageEndID))
			pageWriteBatch := gorocksdb.NewWriteBatch()
			defer pageWriteBatch.Clear()
			log.Debug("Delete page info", zap.Any("topic", topic), zap.Any("pageStartID", pageStartID), zap.Any("pageEndID", pageEndID))
			if pageStartID == pageEndID {
				pageWriteBatch.Delete([]byte(pageStartKey))
			} else if pageStartID < pageEndID {
				pageWriteBatch.DeleteRange([]byte(pageStartKey), []byte(pageEndKey))
			}
			ri.kv.DB.Write(gorocksdb.NewDefaultWriteOptions(), pageWriteBatch)

			pageInfo.pageEndID = pageInfo.pageEndID[pageRetentionOffset:]
		}
		ri.pageInfo.Store(topic, pageInfo)
	}
	if endID == 0 {
		log.Debug("All messages are not expired")
		return nil
	}
	log.Debug("ExpiredCleanUp: ", zap.Any("topic", topic), zap.Any("startID", startID), zap.Any("endID", endID), zap.Any("deletedAckedSize", deletedAckedSize))

	// Delete acked_ts in rocksdb_kv
	fixedAckedTsTitle, err := constructKey(AckedTsTitle, topic)
	if err != nil {
		return err
	}
	ackedStartIDKey := fixedAckedTsTitle + "/" + strconv.Itoa(int(startID))
	ackedEndIDKey := fixedAckedTsTitle + "/" + strconv.Itoa(int(endID))
	ackedTsWriteBatch := gorocksdb.NewWriteBatch()
	defer ackedTsWriteBatch.Clear()
	if startID > endID {
		return nil
	} else if startID == endID {
		ackedTsWriteBatch.Delete([]byte(ackedStartIDKey))
	} else {
		ackedTsWriteBatch.DeleteRange([]byte(ackedStartIDKey), []byte(ackedEndIDKey))
	}
	ri.kv.DB.Write(gorocksdb.NewDefaultWriteOptions(), ackedTsWriteBatch)

	// Update acked_size in rocksdb_kv

	// Update last retention ts
	lastRetentionTsKey := LastRetTsTitle + topic
	err = ri.kv.Save(lastRetentionTsKey, strconv.FormatInt(time.Now().Unix(), 10))
	if err != nil {
		return err
	}

	ackedInfo.ackedSize -= deletedAckedSize
	ackedSizeKey := AckedSizeTitle + topic
	err = ri.kv.Save(ackedSizeKey, strconv.FormatInt(ackedInfo.ackedSize, 10))
	if err != nil {
		return err
	}

	for k := range ackedInfo.ackedTs {
		if k < endID {
			delete(ackedInfo.ackedTs, k)
		}
	}
	ri.ackedInfo.Store(topic, ackedInfo)

	return DeleteMessages(ri.db, topic, startID, endID)
}

func DeleteMessages(db *gorocksdb.DB, topic string, startID, endID UniqueID) error {
	// Delete msg by range of startID and endID
	startKey, err := combKey(topic, startID)
	if err != nil {
		log.Debug("RocksMQ: combKey(" + topic + "," + strconv.FormatInt(startID, 10) + ")")
		return err
	}
	endKey, err := combKey(topic, endID)
	if err != nil {
		log.Debug("RocksMQ: combKey(" + topic + "," + strconv.FormatInt(endID, 10) + ")")
		return err
	}

	writeBatch := gorocksdb.NewWriteBatch()
	defer writeBatch.Clear()
	if startID == endID {
		writeBatch.Delete([]byte(startKey))
	} else {
		writeBatch.DeleteRange([]byte(startKey), []byte(endKey))
	}
	err = db.Write(gorocksdb.NewDefaultWriteOptions(), writeBatch)
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
