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

package storage

import (
	"bytes"
	"fmt"
	"io"
	"testing"
	"testing/iotest"

	"github.com/stretchr/testify/require"
)

func TestBM25StatsDecodeChunkBoundaries(t *testing.T) {
	original := NewBM25Stats()
	for i := uint32(0); i < 1025; i++ {
		original.Append(map[uint32]float32{i: 2})
	}
	data, err := original.Serialize()
	require.NoError(t, err)
	readers := map[string]func() io.Reader{
		"buffer":        func() io.Reader { return bytes.NewReader(data) },
		"one_byte":      func() io.Reader { return iotest.OneByteReader(bytes.NewReader(data)) },
		"half_buffer":   func() io.Reader { return iotest.HalfReader(bytes.NewReader(data)) },
		"data_with_eof": func() io.Reader { return iotest.DataErrReader(bytes.NewReader(data)) },
	}
	for name, reader := range readers {
		t.Run(name, func(t *testing.T) {
			restored := NewBM25Stats()
			require.NoError(t, restored.Deserialize(data))
			require.Equal(t, original, restored)
			require.NoError(t, restored.DeserializeFromReader(reader()))
			require.NoError(t, restored.Deserialize(data))
			expected := original.Clone()
			expected.Merge(original)
			expected.Merge(original)
			require.Equal(t, expected, restored)
		})
	}
}

func TestBM25StatsDecodeTruncatedHeader(t *testing.T) {
	for size := 0; size < 20; size++ {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			want := io.ErrUnexpectedEOF
			if size == 0 || size == 4 || size == 12 {
				want = io.EOF
			}
			data := make([]byte, size)
			stats := NewBM25Stats()
			stats.Append(map[uint32]float32{42: 2})
			before := stats.Clone()
			require.ErrorIs(t, stats.Deserialize(data), want)
			require.Equal(t, before, stats)
			require.ErrorIs(t, stats.DeserializeFromReader(bytes.NewReader(data)), want)
			require.Equal(t, before, stats)
		})
	}
}

func TestBM25StatsDecodeTruncatedRecord(t *testing.T) {
	original := NewBM25Stats()
	original.Append(map[uint32]float32{42: 2})
	data, err := original.Serialize()
	require.NoError(t, err)
	for trailing := 1; trailing < 8; trailing++ {
		t.Run(fmt.Sprint(trailing), func(t *testing.T) {
			want := io.ErrUnexpectedEOF
			if trailing == 4 {
				want = io.EOF
			}
			payload := append(bytes.Clone(data), make([]byte, trailing)...)
			restored := NewBM25Stats()
			require.ErrorIs(t, restored.DeserializeFromReader(iotest.OneByteReader(bytes.NewReader(payload))), want)
			require.Equal(t, original, restored)
		})
	}
}

func TestBM25StatsDecodeReaderError(t *testing.T) {
	original := NewBM25Stats()
	original.Append(map[uint32]float32{42: 2})
	data, err := original.Serialize()
	require.NoError(t, err)
	reader := io.MultiReader(bytes.NewReader(data), iotest.ErrReader(io.ErrClosedPipe))
	restored := NewBM25Stats()
	require.ErrorIs(t, restored.DeserializeFromReader(reader), io.ErrClosedPipe)
	require.Equal(t, original, restored)
}

func BenchmarkBM25StatsDecode(b *testing.B) {
	original := NewBM25Stats()
	for i := uint32(0); i < 4096; i++ {
		original.Append(map[uint32]float32{i: 2})
	}
	data, err := original.Serialize()
	require.NoError(b, err)
	b.Run("bytes", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for i := 0; i < b.N; i++ {
			require.NoError(b, NewBM25Stats().Deserialize(data))
		}
	})
	b.Run("reader", func(b *testing.B) {
		b.ReportAllocs()
		b.SetBytes(int64(len(data)))
		for i := 0; i < b.N; i++ {
			require.NoError(b, NewBM25Stats().DeserializeFromReader(bytes.NewReader(data)))
		}
	})
}
