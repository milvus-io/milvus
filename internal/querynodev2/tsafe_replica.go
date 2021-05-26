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

import "sync"

type TSafeReplicaInterface interface {
	getTSafe(vChannel VChannel) Timestamp
	setTSafe(vChannel VChannel, timestamp Timestamp)
	addTSafe(vChannel VChannel)
	removeTSafe(vChannel VChannel)
}

type tSafeReplica struct {
	collectionID UniqueID
	mu           sync.Mutex        // guards tSafes
	tSafes       map[string]tSafer // map[vChannel]tSafer
}

func (t *tSafeReplica) getTSafe(vChannel VChannel) Timestamp {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.getTSaferPrivate(vChannel).get()
}

func (t *tSafeReplica) setTSafe(vChannel VChannel, timestamp Timestamp) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.getTSaferPrivate(vChannel).set(timestamp)
}

func (t *tSafeReplica) getTSaferPrivate(vChannel VChannel) tSafer {
	return t.tSafes[vChannel]
}

func (t *tSafeReplica) addTSafe(vChannel VChannel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.tSafes[vChannel] = newTSafe()
}

func (t *tSafeReplica) removeTSafe(vChannel VChannel) {
	t.mu.Lock()
	defer t.mu.Unlock()
	ts := t.getTSaferPrivate(vChannel)
	ts.close()
	delete(t.tSafes, vChannel)
}

func newTSafeReplica(collectionID UniqueID) TSafeReplicaInterface {
	var replica TSafeReplicaInterface = &tSafeReplica{
		collectionID: collectionID,
		tSafes:       make(map[string]tSafer),
	}
	return replica
}
