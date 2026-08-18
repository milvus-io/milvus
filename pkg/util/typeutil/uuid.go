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
	"github.com/google/uuid"

	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NormalizeUUID parses s and returns its canonical lowercase form
// (uuid.UUID.String()). It accepts and canonicalizes the same input forms
// as uuid.Parse (32 hex digits, braces, URN prefix), matching insert-side
// behavior.
func NormalizeUUID(s string) (string, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return "", merr.WrapErrParameterInvalid("valid UUID string", s, err.Error())
	}
	return u.String(), nil
}

// ParseUUID parses a UUID string and returns a 16-byte array in RFC 4122 big-endian format.
func ParseUUID(s string) ([16]byte, error) {
	u, err := uuid.Parse(s)
	if err != nil {
		return [16]byte{}, merr.WrapErrParameterInvalid("valid UUID string", s, err.Error())
	}
	return [16]byte(u), nil
}

// UUIDToString converts a 16-byte UUID array to canonical lowercase string format.
func UUIDToString(u [16]byte) string {
	return uuid.UUID(u).String()
}

// BytesToUUID converts a 16-byte slice to a 16-byte array.
func BytesToUUID(b []byte) ([16]byte, error) {
	if len(b) != 16 {
		return [16]byte{}, merr.WrapErrParameterInvalidMsg("invalid uuid bytes length %d, expected 16", len(b))
	}
	var u [16]byte
	copy(u[:], b)
	return u, nil
}
