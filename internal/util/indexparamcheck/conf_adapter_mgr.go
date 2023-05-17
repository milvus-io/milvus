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

package indexparamcheck

import (
	"errors"
	"sync"
)

type IndexCheckerMgr interface {
	GetChecker(indexType string) (IndexChecker, error)
}

// indexCheckerMgrImpl implements IndexChecker.
type indexCheckerMgrImpl struct {
	checkers map[IndexType]IndexChecker
	once     sync.Once
}

func (mgr *indexCheckerMgrImpl) GetChecker(indexType string) (IndexChecker, error) {
	mgr.once.Do(mgr.registerIndexChecker)

	checker, ok := mgr.checkers[indexType]
	if ok {
		return checker, nil
	}
	return nil, errors.New("Can not find index checker: " + indexType)
}

func (mgr *indexCheckerMgrImpl) registerIndexChecker() {
	mgr.checkers[IndexFaissIDMap] = newFlatChecker()
	mgr.checkers[IndexFaissIvfFlat] = newIVFBaseChecker()
	mgr.checkers[IndexFaissIvfPQ] = newIVFPQChecker()
	mgr.checkers[IndexFaissIvfSQ8] = newIVFSQChecker()
	mgr.checkers[IndexFaissIvfSQ8H] = newIVFSQChecker()
	mgr.checkers[IndexFaissBinIDMap] = newBinFlatChecker()
	mgr.checkers[IndexFaissBinIvfFlat] = newBinIVFFlatChecker()
	mgr.checkers[IndexNSG] = newNsgChecker()
	mgr.checkers[IndexHNSW] = newHnswChecker()
	mgr.checkers[IndexANNOY] = newAnnoyChecker()
	mgr.checkers[IndexRHNSWFlat] = newRHnswFlatChecker()
	mgr.checkers[IndexRHNSWPQ] = newRHnswPQChecker()
	mgr.checkers[IndexRHNSWSQ] = newRHnswSQChecker()
	mgr.checkers[IndexNGTPANNG] = newNgtPANNGChecker()
	mgr.checkers[IndexNGTONNG] = newNgtONNGChecker()
	mgr.checkers[IndexDISKANN] = newDiskannChecker()
}

func newIndexCheckerMgr() *indexCheckerMgrImpl {
	return &indexCheckerMgrImpl{
		checkers: make(map[IndexType]IndexChecker),
	}
}

var indexCheckerMgr IndexCheckerMgr

var getIndexCheckerMgrOnce sync.Once

// GetIndexCheckerMgrInstance gets the instance of IndexCheckerMgr.
func GetIndexCheckerMgrInstance() IndexCheckerMgr {
	getIndexCheckerMgrOnce.Do(func() {
		indexCheckerMgr = newIndexCheckerMgr()
	})
	return indexCheckerMgr
}
