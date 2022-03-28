// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package querynode

import (
	"errors"
	"sync"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
)

// TSafeReplicaInterface is the interface wrapper of tSafeReplica
type TSafeReplicaInterface interface {
	getTSafe(vChannel Channel) (Timestamp, error)
	setTSafe(vChannel Channel, timestamp Timestamp) error
	addTSafe(vChannel Channel)
	removeTSafe(vChannel Channel)
	registerTSafeWatcher(vChannel Channel, watcher *tSafeWatcher) error
}

// tSafeReplica implements `TSafeReplicaInterface` interface.
type tSafeReplica struct {
	mu     sync.Mutex         // guards tSafes
	tSafes map[Channel]*tSafe // map[DMLChannel|deltaChannel]*tSafe
}

func (t *tSafeReplica) getTSafe(vChannel Channel) (Timestamp, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.getTSafePrivate(vChannel)
	if err != nil {
		//log.Warn("get tSafe failed",
		//	zap.Any("channel", vChannel),
		//	zap.Error(err),
		//)
		return 0, err
	}
	return ts.get(), nil
}

func (t *tSafeReplica) setTSafe(vChannel Channel, timestamp Timestamp) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.getTSafePrivate(vChannel)
	if err != nil {
		return errors.New("set tSafe failed, err = " + err.Error())
	}
	ts.set(timestamp)
	return nil
}

func (t *tSafeReplica) getTSafePrivate(vChannel Channel) (*tSafe, error) {
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
	if _, ok := t.tSafes[vChannel]; !ok {
		t.tSafes[vChannel] = newTSafe(vChannel)
		log.Debug("add tSafe done",
			zap.Any("channel", vChannel),
		)
	} else {
		log.Debug("tSafe has been existed",
			zap.Any("channel", vChannel),
		)
	}
}

func (t *tSafeReplica) removeTSafe(vChannel Channel) {
	t.mu.Lock()
	defer t.mu.Unlock()

	log.Debug("remove tSafe replica",
		zap.Any("vChannel", vChannel),
	)
	delete(t.tSafes, vChannel)
}

func (t *tSafeReplica) registerTSafeWatcher(vChannel Channel, watcher *tSafeWatcher) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.getTSafePrivate(vChannel)
	if err != nil {
		return err
	}
	return ts.registerTSafeWatcher(watcher)
}

func newTSafeReplica() TSafeReplicaInterface {
	var replica TSafeReplicaInterface = &tSafeReplica{
		tSafes: make(map[string]*tSafe),
	}
	return replica
}
