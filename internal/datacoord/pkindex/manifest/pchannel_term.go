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

package manifest

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// checkPChannelTerm fences a flush against the assigned writer. The pointer
// transaction uses the predicate from the same read that matched the term.
func checkPChannelTerm(ctx context.Context, meta MetaStore, pchannel string, term int64) (predicates.Predicate, error) {
	cur, guard, err := streamingcoord.LoadAssignedWriterTerm(ctx, pchannel, meta.Load)
	if err == nil && cur > 0 && term > cur {
		cur, guard, err = streamingcoord.LoadAssignedWriterTerm(ctx, pchannel, meta.Load)
	}
	switch {
	case errors.Is(err, merr.ErrDataIntegrity):
		return nil, errors.Mark(err, pkindex.ErrCorrupted)
	case err != nil:
		return nil, markUnavailable(err, "load writer term for %s", pchannel)
	case cur == 0:
		return nil, merr.Wrapf(ErrTermNotAssigned, "pchannel %s has no assigned writer, term %d", pchannel, term)
	case term < cur:
		return nil, merr.Wrapf(ErrFenced, "pchannel %s: current term %d, term %d", pchannel, cur, term)
	case term > cur:
		return nil, merr.Wrapf(ErrTermNotAssigned, "pchannel %s: current term %d, term %d", pchannel, cur, term)
	}
	return guard, nil
}
