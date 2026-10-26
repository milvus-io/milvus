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
	"github.com/milvus-io/milvus/internal/pkindex/dedup"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
)

// PKsOf converts the primary keys of a WAL message to the keys of the
// decider, in the order of keys. The WAL interceptor and the recovery
// share it, so a replay writes the same keys as the append path.
func PKsOf(keys primarykey.Keys) []dedup.PK {
	pks := make([]dedup.PK, 0, keys.Len())
	for _, v := range keys.Int64Values {
		pks = append(pks, dedup.Int64PK(v))
	}
	for _, v := range keys.StringValues {
		pks = append(pks, dedup.VarCharPK(v))
	}
	return pks
}
