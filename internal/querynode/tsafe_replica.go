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
	getTSafe(vChannel Channel) (Timestamp, error)
	setTSafe(vChannel Channel, id UniqueID, timestamp Timestamp) error
	addTSafe(vChannel Channel)
	removeTSafe(vChannel Channel) error
	registerTSafeWatcher(vChannel Channel, watcher *tSafeWatcher) error
	removeRecord(vChannel Channel, partitionID UniqueID) error
}

type tSafeRef struct {
	tSafer tSafer
	ref    int
}

// tSafeReplica implements `TSafeReplicaInterface` interface.
type tSafeReplica struct {
	mu     sync.Mutex            // guards tSafes
	tSafes map[Channel]*tSafeRef // map[vChannel]tSafeRef
}

func (t *tSafeReplica) getTSafe(vChannel Channel) (Timestamp, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		//log.Warn("get tSafe failed",
		//	zap.Any("channel", vChannel),
		//	zap.Error(err),
		//)
		return 0, err
	}
	return safer.get(), nil
}

func (t *tSafeReplica) setTSafe(vChannel Channel, id UniqueID, timestamp Timestamp) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		//log.Warn("set tSafe failed", zap.Error(err))
		return err
	}
	safer.set(id, timestamp)
	return nil
}

func (t *tSafeReplica) getTSaferPrivate(vChannel Channel) (tSafer, error) {
	if _, ok := t.tSafes[vChannel]; !ok {
		err := errors.New("cannot found tSafer, vChannel = " + vChannel)
		//log.Warn(err.Error())
		return nil, err
	}
	return t.tSafes[vChannel].tSafer, nil
}

func (t *tSafeReplica) addTSafe(vChannel Channel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ctx := context.Background()
	if _, ok := t.tSafes[vChannel]; !ok {
		t.tSafes[vChannel] = &tSafeRef{
			tSafer: newTSafe(ctx, vChannel),
			ref:    1,
		}
		t.tSafes[vChannel].tSafer.start()
		log.Debug("add tSafe done",
			zap.Any("channel", vChannel),
			zap.Any("count", t.tSafes[vChannel].ref),
		)
	} else {
		t.tSafes[vChannel].ref++
		log.Debug("tSafe has been existed",
			zap.Any("channel", vChannel),
			zap.Any("count", t.tSafes[vChannel].ref),
		)
	}
}

func (t *tSafeReplica) removeTSafe(vChannel Channel) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, ok := t.tSafes[vChannel]; !ok {
		return errors.New("tSafe not exist, vChannel = " + vChannel)
	}
	t.tSafes[vChannel].ref--
	log.Debug("reduce tSafe reference count",
		zap.Any("vChannel", vChannel),
		zap.Any("count", t.tSafes[vChannel].ref),
	)
	if t.tSafes[vChannel].ref == 0 {
		safer, err := t.getTSaferPrivate(vChannel)
		if err != nil {
			return err
		}
		log.Debug("remove tSafe replica",
			zap.Any("vChannel", vChannel),
		)
		safer.close()
		delete(t.tSafes, vChannel)
	}
	return nil
}

func (t *tSafeReplica) removeRecord(vChannel Channel, partitionID UniqueID) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		return err
	}
	safer.removeRecord(partitionID)
	return nil
}

func (t *tSafeReplica) registerTSafeWatcher(vChannel Channel, watcher *tSafeWatcher) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		return err
	}
	safer.registerTSafeWatcher(watcher)
	return nil
}

func newTSafeReplica() TSafeReplicaInterface {
	var replica TSafeReplicaInterface = &tSafeReplica{
		tSafes: make(map[string]*tSafeRef),
	}
	return replica
}
