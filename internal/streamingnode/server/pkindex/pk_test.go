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

package pkindex

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
)

func TestPKsOfKeepsOrderAndType(t *testing.T) {
	pks := PKsOf(primarykey.Keys{Int64Values: []int64{3, 1}})
	assert.Equal(t, []dedup.PK{dedup.Int64PK(3), dedup.Int64PK(1)}, pks)
	pks = PKsOf(primarykey.Keys{StringValues: []string{"b", "a"}})
	assert.Equal(t, []dedup.PK{dedup.VarCharPK("b"), dedup.VarCharPK("a")}, pks)
	pks = PKsOf(primarykey.Keys{Int64Values: []int64{1}, StringValues: []string{"a"}})
	assert.Equal(t, []dedup.PK{dedup.Int64PK(1), dedup.VarCharPK("a")}, pks)
	assert.Empty(t, PKsOf(primarykey.Keys{}))
}
