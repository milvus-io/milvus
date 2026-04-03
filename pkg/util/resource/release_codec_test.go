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

package resource

import (
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
)

func TestReleaseCodecTriggerAfterMarshal(t *testing.T) {
	MsgPins.ResetForTest()

	msg := &internalpb.SearchResults{}
	var released atomic.Int32

	MsgPins.Pin(msg, func() { released.Add(1) })
	require.True(t, MsgPins.HasPinned(msg))

	codec := releaseCodec{}
	out, err := codec.Marshal(msg)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.EqualValues(t, 1, released.Load())
	require.False(t, MsgPins.HasPinned(msg))
}

func TestReleaseCodecDoublePinIgnoresSecond(t *testing.T) {
	MsgPins.ResetForTest()

	msg := &internalpb.SearchResults{}
	var firstReleased, secondReleased atomic.Int32

	MsgPins.Pin(msg, func() { firstReleased.Add(1) })
	MsgPins.Pin(msg, func() { secondReleased.Add(1) }) // double-pin: second is ignored
	require.True(t, MsgPins.HasPinned(msg))

	codec := releaseCodec{}
	_, err := codec.Marshal(msg)
	require.NoError(t, err)
	require.EqualValues(t, 1, firstReleased.Load()) // first cleanup is called
	require.Zero(t, secondReleased.Load())          // second was ignored
	require.False(t, MsgPins.HasPinned(msg))
}

func TestReleaseCodecNonPinnableSkipsRelease(t *testing.T) {
	MsgPins.ResetForTest()
	defer MsgPins.ResetForTest()

	// SearchRequest does not implement MsgPinnable — codec should not touch MsgPins.
	msg := &internalpb.SearchRequest{}
	var released atomic.Int32
	MsgPins.Pin(msg, func() { released.Add(1) })

	codec := releaseCodec{}
	_, err := codec.Marshal(msg)
	require.NoError(t, err)
	require.Zero(t, released.Load())        // cleanup was NOT called by codec
	require.True(t, MsgPins.HasPinned(msg)) // still in the map
}

func TestReleaseCodecUnmarshal(t *testing.T) {
	original := &internalpb.SearchResults{
		NumQueries: 5,
		TopK:       10,
		MetricType: "L2",
	}

	codec := releaseCodec{}
	out, err := codec.Marshal(original)
	require.NoError(t, err)
	require.NotNil(t, out)

	got := &internalpb.SearchResults{}
	err = codec.Unmarshal(out, got)
	require.NoError(t, err)
	require.Equal(t, int64(5), got.NumQueries)
	require.Equal(t, int64(10), got.TopK)
	require.Equal(t, "L2", got.MetricType)
}

func TestReleaseCodecMarshalLargeMessage(t *testing.T) {
	// Create a message large enough to exceed the buffer pooling threshold,
	// exercising the pool.Get / MarshalAppend / NewBuffer branch.
	MsgPins.ResetForTest()
	defer MsgPins.ResetForTest()

	largeBlob := make([]byte, 1<<20) // 1 MiB
	for i := range largeBlob {
		largeBlob[i] = byte(i % 256)
	}
	msg := &internalpb.SearchResults{
		SlicedBlob: largeBlob,
		NumQueries: 42,
	}
	var released atomic.Int32
	MsgPins.Pin(msg, func() { released.Add(1) })

	codec := releaseCodec{}
	out, err := codec.Marshal(msg)
	require.NoError(t, err)
	require.NotNil(t, out)
	// Pinnable message should have been released
	require.EqualValues(t, 1, released.Load())

	// Round-trip: unmarshal and verify
	got := &internalpb.SearchResults{}
	err = codec.Unmarshal(out, got)
	require.NoError(t, err)
	require.Equal(t, int64(42), got.NumQueries)
	require.Equal(t, largeBlob, got.SlicedBlob)
}

func TestReleaseCodecMarshalNonProto(t *testing.T) {
	codec := releaseCodec{}
	_, err := codec.Marshal("not a proto message")
	require.Error(t, err)
}

func TestReleaseCodecUnmarshalNonProto(t *testing.T) {
	// First get a valid BufferSlice from a real message
	codec := releaseCodec{}
	out, err := codec.Marshal(&internalpb.SearchResults{})
	require.NoError(t, err)

	err = codec.Unmarshal(out, "not a proto message")
	require.Error(t, err)
}

func TestSearchResultsImplementsMsgPinnable(t *testing.T) {
	// Verify the MsgPinnable marker interface is correctly implemented.
	var msg interface{} = &internalpb.SearchResults{}
	_, ok := msg.(MsgPinnable)
	require.True(t, ok, "SearchResults should implement MsgPinnable")

	// SearchRequest should NOT implement it.
	var req interface{} = &internalpb.SearchRequest{}
	_, ok = req.(MsgPinnable)
	require.False(t, ok, "SearchRequest should not implement MsgPinnable")
}

func TestReleaseCodecMarshalNotPinnedPinnable(t *testing.T) {
	// A MsgPinnable message that is NOT pinned — Marshal should succeed without error.
	MsgPins.ResetForTest()
	defer MsgPins.ResetForTest()

	msg := &internalpb.SearchResults{NumQueries: 7}
	codec := releaseCodec{}
	out, err := codec.Marshal(msg)
	require.NoError(t, err)
	require.NotNil(t, out)
	require.False(t, MsgPins.HasPinned(msg))
}
