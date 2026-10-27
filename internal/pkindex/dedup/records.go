// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package dedup

import (
	"context"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// This file reads the records of the index, PK → codec.PKEntry, on top of the
// byte interface of the engine. The decider only reads: the observe of the
// WAL recovery writes the engine.

// errValueCountMismatch reports an engine whose MultiGet result does not align with the keys.
var errValueCountMismatch = errors.New("primary key index engine returned a wrong number of values")

// lookup returns the entries aligned with pks, nil for absent keys.
func lookup(ctx context.Context, eng engine.RW, pks []PK) ([]*codec.PKEntry, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	keys := make([][]byte, len(pks))
	for i, pk := range pks {
		keys[i] = pk.Encode()
	}
	values, err := eng.MultiGet(ctx, keys)
	if err != nil {
		return nil, err
	}
	if len(values) != len(keys) {
		return nil, errors.Wrapf(errValueCountMismatch, "%d values for %d keys", len(values), len(keys))
	}
	entries := make([]*codec.PKEntry, len(pks))
	for i, value := range values {
		if value == nil {
			continue
		}
		entry, err := codec.DecodePKEntry(value)
		if err != nil {
			return nil, err
		}
		entries[i] = &entry
	}
	return entries, nil
}
