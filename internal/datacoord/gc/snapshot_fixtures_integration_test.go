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

package gc

import (
	"testing"

	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
)

// newMemoryMetaWithSnapshot builds a meta over an in-memory catalog together with the index, analyze and snapshot metas.
func newMemoryMetaWithSnapshot(t *testing.T) (*meta.Meta, *snapshot.SnapshotTestMetas, error) {
	dm := &snapshot.SnapshotTestMetas{}
	m, err := meta.NewMemoryMetaWithLoader(t, snapshot.WithSnapshotTestMetas(dm))
	return m, dm, err
}
