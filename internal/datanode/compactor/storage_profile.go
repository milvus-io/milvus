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

package compactor

import (
	"sync"

	"github.com/milvus-io/milvus/internal/storageprofile"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

type profiledCompactor struct {
	Compactor
	scope  *storageprofile.Scope
	finish sync.Once
}

func WithStorageProfile(task Compactor, scope *storageprofile.Scope) Compactor {
	if task == nil || scope == nil {
		return task
	}
	return &profiledCompactor{Compactor: task, scope: scope}
}

func (t *profiledCompactor) Compact() (*datapb.CompactionPlanResult, error) {
	result, err := t.Compactor.Compact()
	t.finishProfile()
	return result, err
}

func (t *profiledCompactor) Stop() {
	t.Compactor.Stop()
	t.finishProfile()
}

func (t *profiledCompactor) finishProfile() {
	t.finish.Do(t.scope.Finish)
}

var _ Compactor = (*profiledCompactor)(nil)
