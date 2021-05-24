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
	"errors"
	"sync"
)

type TSafeReplicaInterface interface {
	getTSafer(vChannel VChannel) (tSafer, error)
	getTSafe(vChannel VChannel) (Timestamp, error)
	setTSafe(vChannel VChannel, timestamp Timestamp) error
	addTSafe(vChannel VChannel)
	removeTSafe(vChannel VChannel)
}

type tSafeReplica struct {
	mu     sync.Mutex        // guards tSafes
	tSafes map[string]tSafer // map[vChannel]tSafer
}

func (t *tSafeReplica) getTSafer(vChannel VChannel) (tSafer, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		return nil, err
	}
	return safer, nil
}

func (t *tSafeReplica) getTSaferPrivate(vChannel VChannel) (tSafer, error) {
	safer, ok := t.tSafes[vChannel]
	if !ok {
		return nil, errors.New("tSafe closed, vChannel =" + vChannel)
	}
	return safer, nil
}

func (t *tSafeReplica) getTSafe(vChannel VChannel) (Timestamp, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		return 0, err
	}
	return safer.get(), nil
}

func (t *tSafeReplica) setTSafe(vChannel VChannel, timestamp Timestamp) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	safer, err := t.getTSaferPrivate(vChannel)
	if err != nil {
		return err
	}
	safer.set(timestamp)
	return nil
}

func (t *tSafeReplica) addTSafe(vChannel VChannel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tSafes[vChannel] = newTSafe()
}

func (t *tSafeReplica) removeTSafe(vChannel VChannel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts, err := t.getTSaferPrivate(vChannel)
	if err != nil && ts != nil {
		ts.close()
	}
	delete(t.tSafes, vChannel)
}

func newTSafeReplica() TSafeReplicaInterface {
	var replica TSafeReplicaInterface = &tSafeReplica{
		tSafes: make(map[string]tSafer),
	}
	return replica
}
