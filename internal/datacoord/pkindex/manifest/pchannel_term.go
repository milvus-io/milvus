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
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/metastore/kv/streamingcoord"
	"github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/kv/predicates"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
)

// checkPChannelTerm checks that term is the current term of pchannel in the
// streamingcoord pchannel meta, and returns the predicate that the pchannel
// meta still holds the value that term was checked against. It returns
// ErrFenced when term is below the current term, and ErrTermNotAssigned when
// pchannel has no meta or term is still above the current term after a second
// read. It reads the meta store on every call.
func checkPChannelTerm(ctx context.Context, meta MetaStore, pchannel string, term int64) (predicates.Predicate, error) {
	key := streamingcoord.BuildPChannelInfoPath(pchannel)
	value, cur, err := loadPChannelTerm(ctx, meta, key)
	if err == nil && cur > 0 && term > cur {
		value, cur, err = loadPChannelTerm(ctx, meta, key)
	}
	switch {
	case err != nil:
		return nil, err
	case cur == 0:
		return nil, errors.Wrapf(ErrTermNotAssigned, "pchannel %s: no pchannel meta, term %d", pchannel, term)
	case term < cur:
		return nil, errors.Wrapf(ErrFenced, "pchannel %s: current term %d, term %d", pchannel, cur, term)
	case term > cur:
		return nil, errors.Wrapf(ErrTermNotAssigned, "pchannel %s: current term %d, term %d", pchannel, cur, term)
	}
	return predicates.ValueEqual(key, value), nil
}

// loadPChannelTerm reads the pchannel meta under key and returns its stored
// value and its term. It returns term 0 when key does not exist.
func loadPChannelTerm(ctx context.Context, meta MetaStore, key string) (string, int64, error) {
	value, found, err := meta.Load(ctx, key)
	if err != nil {
		return "", 0, markUnavailable(err, "load pchannel meta %s", key)
	}
	if !found {
		return "", 0, nil
	}
	var pb streamingpb.PChannelMeta
	if err := proto.Unmarshal([]byte(value), &pb); err != nil {
		return "", 0, errors.Mark(errors.Wrapf(err, "decode pchannel meta %s", key), pkindex.ErrCorrupted)
	}
	if term := pb.GetChannel().GetTerm(); term > 0 {
		return value, term, nil
	}
	return "", 0, errors.Mark(errors.Newf("pchannel meta %s has term %d", key, pb.GetChannel().GetTerm()), pkindex.ErrCorrupted)
}
