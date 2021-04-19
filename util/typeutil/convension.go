// Copyright 2016 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package typeutil

import (
	"encoding/binary"

	"github.com/czs007/suvlim/errors"
)

// BytesToUint64 converts a byte slice to uint64.
func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, errors.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}
