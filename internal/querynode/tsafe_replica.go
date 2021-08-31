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

package querynode

import (
	"context"
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// TSafeReplicaInterface is the interface wrapper of tSafeReplica
type TSafeReplicaInterface interface {
	getTSafe(vChannel Channel) Timestamp
	setTSafe(vChannel Channel, id UniqueID, timestamp Timestamp)
	addTSafe(vChannel Channel)
	removeTSafe(vChannel Channel)
	registerTSafeWatcher(vChannel Channel, watcher *tSafeWatcher)
	removeRecord(vChannel Channel, id UniqueID)
}

type tSafeReplica struct {
	mu         sync.Mutex         // guards all
	tSafes     map[Channel]tSafer // map[vChannel]tSafer
	refCounter map[Channel]int    // map[vChannel]count
}

func (t *tSafeReplica) getTSafe(vChannel Channel) Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		log.Warn("get tSafe failed",
			zap.Any("channel", vChannel),
			zap.Error(err),
		)
		return 0
	}
	return safer.get()
}

func (t *tSafeReplica) setTSafe(vChannel Channel, id UniqueID, timestamp Timestamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		log.Warn("set tSafe failed", zap.Error(err))
		return
	}
	safer.set(id, timestamp)
}

func (t *tSafeReplica) getTSaferPrivate(vChannel Channel) (tSafer, error) {
	if _, ok := t.tSafes[vChannel]; !ok {
		err := errors.New("cannot found tSafer, vChannel = " + vChannel)
		//log.Warn(err.Error())
		return nil, err
	}
	return t.tSafes[vChannel], nil
}

func (t *tSafeReplica) addTSafe(vChannel Channel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ctx := context.Background()
	t.refCounter[vChannel]++
	if _, ok := t.tSafes[vChannel]; !ok {
		t.tSafes[vChannel] = newTSafe(ctx, vChannel)
		t.tSafes[vChannel].start()
		log.Debug("add tSafe done",
			zap.Any("channel", vChannel),
			zap.Any("count", t.refCounter[vChannel]),
		)
	} else {
		log.Debug("tSafe has been existed",
			zap.Any("channel", vChannel),
			zap.Any("count", t.refCounter[vChannel]),
		)
	}
}

func (t *tSafeReplica) removeTSafe(vChannel Channel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.refCounter[vChannel]--
	log.Debug("reduce tSafe reference count",
		zap.Any("vChannel", vChannel),
		zap.Any("count", t.refCounter[vChannel]),
	)
	if t.refCounter[vChannel] == 0 {
		safer, err := t.getTSaferPrivate(vChannel)
		if err != nil {
			log.Warn(err.Error())
			return
		}
		log.Debug("remove tSafe replica",
			zap.Any("vChannel", vChannel),
		)
		safer.close()
		delete(t.tSafes, vChannel)
		delete(t.refCounter, vChannel)
	}
}

func (t *tSafeReplica) registerTSafeWatcher(vChannel Channel, watcher *tSafeWatcher) {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		log.Warn("register tSafe watcher failed", zap.Error(err))
		return
	}
	safer.registerTSafeWatcher(watcher)
}

func (t *tSafeReplica) removeRecord(vChannel Channel, id UniqueID) {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		log.Warn("register tSafe watcher failed", zap.Error(err))
		return
	}
	safer.removeRecord(id)
}

func newTSafeReplica() TSafeReplicaInterface {
	var replica TSafeReplicaInterface = &tSafeReplica{
		tSafes:     make(map[Channel]tSafer),
		refCounter: make(map[Channel]int),
	}
	return replica
}
