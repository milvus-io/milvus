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

package datacoord

import (
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"testing"
)

// gcTestDomainOption returns opt with the domain metas in dm.
func gcTestDomainOption(dm *snapshotTestMetas, opt GcOption) GcOption {
	opt.indexMeta = dm.indexMeta
	opt.analyzeMeta = dm.analyzeMeta
	opt.snapshotMeta = dm.snapshotMeta
	return opt
}

func newGCManifestReadIO(t *testing.T) *packed.ManifestIOContext {
	t.Helper()
	io := packed.NewManifestIOContext(1)
	t.Cleanup(io.Close)
	return io
}
