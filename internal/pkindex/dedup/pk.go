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

package dedup

import (
	"encoding/binary"
)

func (p PK) IsVarChar() bool { return p.varchar }

func (p PK) Int64() int64 { return p.i }

func (p PK) VarChar() string { return p.s }

// Encode returns the order-preserving key of the primary key.
// An int64 becomes 8 big-endian bytes with the sign bit flipped.
// A varchar is its raw bytes.
func (p PK) Encode() []byte {
	if p.varchar {
		return []byte(p.s)
	}
	key := make([]byte, 8)
	binary.BigEndian.PutUint64(key, uint64(p.i)^(1<<63))
	return key
}

// lockKey returns the key of the primary key in the in-flight set: the value of
// an int64 key, or the 64-bit FNV-1a hash of a varchar key. Two varchar keys
// with the same hash share one key in the set. That only adds a wait for the
// other key and a companion delete of a key that may not exist, both safe.
func (p PK) lockKey() uint64 {
	if !p.varchar {
		return uint64(p.i)
	}
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)
	h := uint64(offset64)
	for i := 0; i < len(p.s); i++ {
		h ^= uint64(p.s[i])
		h *= prime64
	}
	return h
}
