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

package checkers

import (
	"context"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/querycoordv2/task"
	"github.com/milvus-io/milvus/internal/querycoordv2/utils"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type Checker interface {
	ID() utils.CheckerType
	Description() string
	Check(ctx context.Context) []task.Task
	IsActive() bool
	Activate()
	Deactivate()
	DeactivateCollection(collectionID int64)
	ActivateCollection(collectionID int64)
	IsCollectionActive(collectionID int64) bool
	GetInactiveCollections() []int64
}

type checkerActivation struct {
	active              atomic.Bool
	inactiveCollections *typeutil.ConcurrentSet[int64]
}

func (c *checkerActivation) IsActive() bool {
	return c.active.Load()
}

func (c *checkerActivation) Activate() {
	c.active.Store(true)
}

func (c *checkerActivation) Deactivate() {
	c.active.Store(false)
}

func (c *checkerActivation) DeactivateCollection(collectionID int64) {
	c.inactiveCollections.Insert(collectionID)
}

func (c *checkerActivation) ActivateCollection(collectionID int64) {
	c.inactiveCollections.Remove(collectionID)
}

func (c *checkerActivation) IsCollectionActive(collectionID int64) bool {
	return !c.inactiveCollections.Contain(collectionID)
}

func (c *checkerActivation) GetInactiveCollections() []int64 {
	return c.inactiveCollections.Collect()
}

func newCheckerActivation() *checkerActivation {
	c := &checkerActivation{
		inactiveCollections: typeutil.NewConcurrentSet[int64](),
	}
	c.Activate()
	return c
}
