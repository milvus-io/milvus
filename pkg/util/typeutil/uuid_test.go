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

package typeutil

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNormalizeUUID(t *testing.T) {
	canonical := "550e8400-e29b-41d4-a716-446655440000"

	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{"canonical pass-through", canonical, canonical, false},
		{"uppercase to lowercase", "550E8400-E29B-41D4-A716-446655440000", canonical, false},
		{"mixed case", "550e8400-e29b-41D4-a716-446655440000", canonical, false},
		{"32 hex digits without dashes", "550e8400e29b41d4a716446655440000", canonical, false},
		{"32 hex digits uppercase", "550E8400E29B41D4A716446655440000", canonical, false},
		{"braces", "{550e8400-e29b-41d4-a716-446655440000}", canonical, false},
		{"urn prefix", "urn:uuid:550e8400-e29b-41d4-a716-446655440000", canonical, false},
		{"empty string", "", "", true},
		{"too short", "550e8400", "", true},
		{"invalid hex", "550e8400-e29b-41d4-a716-44665544000z", "", true},
		{"not a uuid", "not-a-uuid", "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NormalizeUUID(tt.input)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, "", got)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestParseUUIDAndToString(t *testing.T) {
	canonical := "550e8400-e29b-41d4-a716-446655440000"
	u, err := ParseUUID(canonical)
	assert.NoError(t, err)
	assert.Equal(t, 16, len(u))
	assert.Equal(t, canonical, UUIDToString(u))

	// Invalid UUID string
	_, err = ParseUUID("invalid-uuid")
	assert.Error(t, err)

	// BytesToUUID valid and invalid
	validBytes := u[:]
	uFromBytes, err := BytesToUUID(validBytes)
	assert.NoError(t, err)
	assert.Equal(t, u, uFromBytes)

	_, err = BytesToUUID([]byte{1, 2, 3})
	assert.Error(t, err)
}
