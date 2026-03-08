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
)

type Checker interface {
	ID() utils.CheckerType
	Description() string
	Check(ctx context.Context) []task.Task
	IsActive() bool
	Activate()
	Deactivate()
}

type checkerActivation struct {
	active atomic.Bool
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

func newCheckerActivation() *checkerActivation {
	c := &checkerActivation{}
	c.Activate()
	return c
}
