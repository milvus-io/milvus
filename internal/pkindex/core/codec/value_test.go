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

package codec

import (
	"encoding/hex"
	"math"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus/internal/pkindex/pkerr"
)

func TestPKEntryRoundtrip(t *testing.T) {
	for _, id := range []int64{0, 1, -1, math.MaxInt64, math.MinInt64, 445566} {
		enc := EncodePKEntry(PKEntry{SegmentID: id})
		dec, err := DecodePKEntry(enc)
		assert.NoError(t, err)
		assert.Equal(t, PKEntry{SegmentID: id}, dec)
	}
}

func TestTombstone(t *testing.T) {
	assert.True(t, IsTombstone(EncodeTombstone()))
	assert.False(t, IsTombstone(EncodePKEntry(PKEntry{SegmentID: 1})))
	assert.False(t, IsTombstone(nil))
}

func TestDecodePKEntryInvalid(t *testing.T) {
	// short buffer
	for _, b := range [][]byte{nil, {}, {byte(valueKindPKEntry)}, make([]byte, 8)} {
		_, err := DecodePKEntry(b)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
	}
	// unknown kind
	bad := EncodePKEntry(PKEntry{SegmentID: 1})
	bad[0] = 0x7f
	_, err := DecodePKEntry(bad)
	assert.Error(t, err)
	assert.True(t, errors.Is(err, pkerr.ErrCorrupted))
}

// The kind byte and the fixed part are the on-disk format; pinning them keeps
// a later kind from silently shifting SegmentID.
func TestValueGoldenBytes(t *testing.T) {
	assert.Equal(t, "00", hex.EncodeToString(EncodeTombstone()))
	assert.Equal(t, "010102030405060708",
		hex.EncodeToString(EncodePKEntry(PKEntry{SegmentID: 0x0102030405060708})))
}
