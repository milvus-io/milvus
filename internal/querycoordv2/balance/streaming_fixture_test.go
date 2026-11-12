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

package balance

import (
	"testing"

	"github.com/bytedance/mockey"

	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/pkg/v3/extension"
)

// useRegularQueryNodes models a Form replica whose WAL lives on a separate SN.
// These tests exercise regular-QN placement and the fallback when the WAL owner
// is outside the replica, without starting a StreamingCoord.
func useRegularQueryNodes(t *testing.T) {
	t.Helper()
	extension.SetForm()
	t.Cleanup(extension.ResetForTest)
	wal := mockey.Mock((*snmanager.StreamingNodeManager).GetWALLocated).Return(int64(10000)).Build()
	t.Cleanup(func() { wal.UnPatch() })
}
