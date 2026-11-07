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

//go:build test

package gc

import (
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/datacoord/snapshot"
)

// gcTestDomainOption returns opt with the domain metas in dm.
func GcTestDomainOption(dm *snapshot.SnapshotTestMetas, opt GcOption) GcOption {
	opt.IndexMeta = dm.IndexMeta
	opt.AnalyzeMeta = dm.AnalyzeMeta
	opt.SnapshotMeta = dm.SnapshotMeta
	return opt
}

// newTestGarbageCollector passes the domain metadata stored by a test fixture.
func NewTestGarbageCollector(m *meta.Meta, handler meta.Handler, opt GcOption) *GarbageCollector {
	if m != nil {
		if opt.IndexMeta == nil && m.IndexCatalog != nil {
			opt.IndexMeta = m.IndexCatalog.(*index.IndexMeta)
		}
		if opt.SnapshotMeta == nil && m.CompactionProtection != nil {
			opt.SnapshotMeta = m.CompactionProtection.(*snapshot.SnapshotMeta)
		}
	}
	return NewGarbageCollector(m, handler, opt)
}
