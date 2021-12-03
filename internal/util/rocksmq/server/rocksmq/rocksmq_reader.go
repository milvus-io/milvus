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
	"path"
	"strconv"
	"sync"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/tecbot/gorocksdb"
)

type rocksmqReader struct {
	store      *gorocksdb.DB
	topic      string
	readerName string

	readOpts *gorocksdb.ReadOptions
	iter     *gorocksdb.Iterator

	currentID          UniqueID
	messageIDInclusive bool
	readerMutex        chan struct{}
}

//Seek seek the rocksmq reader to the pointed position
func (rr *rocksmqReader) Seek(msgID UniqueID) { //nolint:govet
	rr.currentID = msgID
	select {
	case rr.readerMutex <- struct{}{}:
	default:
	}
}

func (rr *rocksmqReader) Next(ctx context.Context, messageIDInclusive bool) (*ConsumerMessage, error) {
	ll, ok := topicMu.Load(rr.topic)
	if !ok {
		return nil, fmt.Errorf("topic name = %s not exist", rr.topic)
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return nil, fmt.Errorf("get mutex failed, topic name = %s", rr.topic)
	}
	lock.Lock()
	defer lock.Unlock()
	fixChanName, err := fixChannelName(rr.topic)
	if err != nil {
		log.Debug("RocksMQ: fixChannelName " + rr.topic + " failed")
		return nil, err
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()

	var msg *ConsumerMessage
	readOpts.SetPrefixSameAsStart(true)
	iter := rr.store.NewIterator(readOpts)
	defer iter.Close()

	for {
		select {
		case <-ctx.Done():
			log.Debug("Stop get next reader message!")
			return nil, ctx.Err()
		case _, ok := <-rr.readerMutex:
			if !ok {
				log.Warn("reader Mutex closed")
				return nil, fmt.Errorf("reader Mutex closed")
			}
			dataKey := path.Join(fixChanName, strconv.FormatInt(rr.currentID, 10))
			if iter.Seek([]byte(dataKey)); !iter.Valid() {
				continue
			}
			if messageIDInclusive {
				val, err := rr.store.Get(readOpts, []byte(dataKey))
				if err != nil {
					return nil, err
				}
				if !val.Exists() {
					continue
				}
				msg = &ConsumerMessage{
					MsgID: rr.currentID,
				}
				origData := val.Data()
				dataLen := len(origData)
				if dataLen == 0 {
					msg.Payload = nil
				} else {
					msg.Payload = make([]byte, dataLen)
					copy(msg.Payload, origData)
				}
				val.Free()

				// Update nextID in readerOffset
				var nextID UniqueID
				iter.Next()
				if iter.Valid() {
					key := iter.Key()
					nextID, err = strconv.ParseInt(string(key.Data())[FixedChannelNameLen+1:], 10, 64)
					if key.Exists() {
						key.Free()
					}
					if err != nil {
						return nil, err
					}
					rr.readerMutex <- struct{}{}
				} else {
					nextID = rr.currentID + 1
				}
				rr.currentID = nextID
			} else {
				iter.Next()
				if iter.Valid() {
					key := iter.Key()
					tmpKey := string(key.Data())
					key.Free()
					id, err := strconv.ParseInt(tmpKey[FixedChannelNameLen+1:], 10, 64)
					if err != nil {
						return nil, err
					}
					val := iter.Value()
					msg = &ConsumerMessage{
						MsgID: id,
					}
					origData := val.Data()
					dataLen := len(origData)
					if dataLen == 0 {
						msg.Payload = nil
					} else {
						msg.Payload = make([]byte, dataLen)
						copy(msg.Payload, origData)
					}
					val.Free()
					rr.currentID = id
					rr.readerMutex <- struct{}{}
				}
			}
			return msg, nil
		}
	}
}

func (rr *rocksmqReader) HasNext(messageIDInclusive bool) bool {
	ll, ok := topicMu.Load(rr.topic)
	if !ok {
		return false
	}
	lock, ok := ll.(*sync.Mutex)
	if !ok {
		return false
	}
	lock.Lock()
	defer lock.Unlock()
	fixChanName, err := fixChannelName(rr.topic)
	if err != nil {
		log.Debug("RocksMQ: fixChannelName " + rr.topic + " failed")
		return false
	}
	readOpts := gorocksdb.NewDefaultReadOptions()
	defer readOpts.Destroy()
	readOpts.SetPrefixSameAsStart(true)
	iter := rr.store.NewIterator(readOpts)
	defer iter.Close()

	dataKey := path.Join(fixChanName, strconv.FormatInt(rr.currentID, 10))
	iter.Seek([]byte(dataKey))
	if !iter.Valid() {
		return false
	}
	if messageIDInclusive {
		return true
	}
	iter.Next()
	return iter.Valid()
}

func (rr *rocksmqReader) Close() {
	close(rr.readerMutex)
	rr.iter.Close()
	rr.readOpts.Destroy()
}
