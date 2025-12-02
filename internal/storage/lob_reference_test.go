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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLOBReference(t *testing.T) {
	ref := NewLOBReference(12345, 678)
	assert.Equal(t, MagicLOBReference, ref.Magic)
	assert.Equal(t, uint64(12345), ref.LobFileID)
	assert.Equal(t, uint32(678), ref.RowOffset)
}

func TestEncodeLOBReference(t *testing.T) {
	tests := []struct {
		name     string
		ref      *LOBReference
		wantSize int
	}{
		{
			name:     "normal reference",
			ref:      NewLOBReference(453718835200262143, 100),
			wantSize: 16,
		},
		{
			name:     "zero values",
			ref:      NewLOBReference(0, 0),
			wantSize: 16,
		},
		{
			name:     "max values",
			ref:      NewLOBReference(^uint64(0), ^uint32(0)),
			wantSize: 16,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded := EncodeLOBReference(tt.ref)
			assert.Len(t, encoded, tt.wantSize)
		})
	}
}

func TestDecodeLOBReference(t *testing.T) {
	tests := []struct {
		name    string
		ref     *LOBReference
		wantErr bool
	}{
		{
			name:    "normal reference",
			ref:     NewLOBReference(453718835200262143, 100),
			wantErr: false,
		},
		{
			name:    "large TSO value (year 2100)",
			ref:     NewLOBReference(1075431289651462143, 999999),
			wantErr: false,
		},
		{
			name:    "zero values",
			ref:     NewLOBReference(0, 0),
			wantErr: false,
		},
		{
			name:    "max uint64",
			ref:     NewLOBReference(^uint64(0), ^uint32(0)),
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Encode
			encoded := EncodeLOBReference(tt.ref)

			// Decode
			decoded, err := DecodeLOBReference(encoded)

			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.ref.Magic, decoded.Magic)
			assert.Equal(t, tt.ref.LobFileID, decoded.LobFileID)
			assert.Equal(t, tt.ref.RowOffset, decoded.RowOffset)
		})
	}
}

func TestDecodeLOBReference_InvalidSize(t *testing.T) {
	tests := []struct {
		name    string
		data    []byte
		wantErr bool
	}{
		{
			name:    "empty buffer",
			data:    []byte{},
			wantErr: true,
		},
		{
			name:    "too short",
			data:    []byte{1, 2, 3, 4, 5, 6, 7, 8},
			wantErr: true,
		},
		{
			name:    "too long",
			data:    make([]byte, 20),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := DecodeLOBReference(tt.data)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestLOBReference_DebugString(t *testing.T) {
	ref := NewLOBReference(12345, 678)
	expected := "lob:0x00FF00FF:12345:678"
	assert.Equal(t, expected, ref.DebugString())
}

func TestIsLOBReference(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{
			name:     "valid LOB reference",
			data:     EncodeLOBReference(NewLOBReference(12345, 678)),
			expected: true,
		},
		{
			name:     "wrong size - too short",
			data:     []byte{0xFF, 0x00, 0xFF, 0x00},
			expected: false,
		},
		{
			name:     "wrong size - too long",
			data:     make([]byte, 20),
			expected: false,
		},
		{
			name:     "wrong magic",
			data:     make([]byte, 16),
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, IsLOBReference(tt.data))
		})
	}
}

func TestParseDebugString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    *LOBReference
		wantErr bool
	}{
		{
			name:  "valid string",
			input: "lob:0x00FF00FF:12345:678",
			want:  NewLOBReference(12345, 678),
		},
		{
			name:  "large numbers",
			input: "lob:0x00FF00FF:453718835200262143:999999",
			want:  NewLOBReference(453718835200262143, 999999),
		},
		{
			name:    "invalid format - missing prefix",
			input:   "12345:678",
			wantErr: true,
		},
		{
			name:    "invalid format - missing magic",
			input:   "lob:12345:678",
			wantErr: true,
		},
		{
			name:    "invalid format - non-numeric",
			input:   "lob:0x00FF00FF:abc:def",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ParseDebugString(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want.LobFileID, got.LobFileID)
			assert.Equal(t, tt.want.RowOffset, got.RowOffset)
		})
	}
}

func TestLOBReference_RoundTrip(t *testing.T) {
	// Test encode -> decode -> encode produces identical results
	original := NewLOBReference(453718835200262143, 100)

	// First round
	encoded1 := EncodeLOBReference(original)
	decoded, err := DecodeLOBReference(encoded1)
	require.NoError(t, err)

	// Second round
	encoded2 := EncodeLOBReference(decoded)

	assert.Equal(t, encoded1, encoded2)
	assert.Equal(t, original.LobFileID, decoded.LobFileID)
	assert.Equal(t, original.RowOffset, decoded.RowOffset)
}

func BenchmarkEncodeLOBReference(b *testing.B) {
	ref := NewLOBReference(453718835200262143, 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = EncodeLOBReference(ref)
	}
}

func BenchmarkDecodeLOBReference(b *testing.B) {
	ref := NewLOBReference(453718835200262143, 100)
	encoded := EncodeLOBReference(ref)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = DecodeLOBReference(encoded)
	}
}
