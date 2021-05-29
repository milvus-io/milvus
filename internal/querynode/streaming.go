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
	"fmt"
	"sync"
)

type streaming struct {
	replica      ReplicaInterface
	tSafeReplica TSafeReplicaInterface

	dsServicesMu     sync.Mutex // guards dataSyncServices
	dataSyncServices map[UniqueID]*dataSyncService
}

func newStreaming() *streaming {
	replica := newCollectionReplica()
	tReplica := newTSafeReplica()
	ds := make(map[UniqueID]*dataSyncService)
	return &streaming{
		replica:          replica,
		tSafeReplica:     tReplica,
		dataSyncServices: ds,
	}
}

func (s *streaming) start() {
	// TODO: start stats
}

func (s *streaming) close() {
	// TODO: stop stats
	for _, ds := range s.dataSyncServices {
		ds.close()
	}
	s.dataSyncServices = make(map[UniqueID]*dataSyncService)

	// free collectionReplica
	s.replica.freeAll()
}

func (s *streaming) getDataSyncService(collectionID UniqueID) (*dataSyncService, error) {
	s.dsServicesMu.Lock()
	defer s.dsServicesMu.Unlock()
	ds, ok := s.dataSyncServices[collectionID]
	if !ok {
		return nil, errors.New("cannot found dataSyncService, collectionID =" + fmt.Sprintln(collectionID))
	}
	return ds, nil
}

func (s *streaming) addDataSyncService(collectionID UniqueID, ds *dataSyncService) error {
	s.dsServicesMu.Lock()
	defer s.dsServicesMu.Unlock()
	if _, ok := s.dataSyncServices[collectionID]; ok {
		return errors.New("dataSyncService has been existed, collectionID =" + fmt.Sprintln(collectionID))
	}
	s.dataSyncServices[collectionID] = ds
	return nil
}

func (s *streaming) removeDataSyncService(collectionID UniqueID) {
	s.dsServicesMu.Lock()
	defer s.dsServicesMu.Unlock()
	delete(s.dataSyncServices, collectionID)
}
