// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package typeutil

import (
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
)

// Float32ToBytes converts a float to byte slice.
func Float32ToBytes(float float32) []byte {
	bits := math.Float32bits(float)
	bytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(bytes, bits)

	return bytes
}

// BytesToFloat32 converts a byte slice to float32.
func BytesToFloat32(bytes []byte) float32 {
	bits := binary.LittleEndian.Uint32(bytes)

	return math.Float32frombits(bits)
}

// BytesToUint64 converts a byte slice to uint64.
func BytesToInt64(b []byte) (int64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return int64(binary.BigEndian.Uint64(b)), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, uint64(v))
	return b
}

// BytesToUint64 converts a byte slice to uint64.
func BytesToUint64(b []byte) (uint64, error) {
	if len(b) != 8 {
		return 0, fmt.Errorf("invalid data, must 8 bytes, but %d", len(b))
	}

	return binary.BigEndian.Uint64(b), nil
}

// Uint64ToBytes converts uint64 to a byte slice.
func Uint64ToBytes(v uint64) []byte {
	b := make([]byte, 8)
	binary.BigEndian.PutUint64(b, v)
	return b
}

func SliceRemoveDuplicate(a interface{}) (ret []interface{}) {
	if reflect.TypeOf(a).Kind() != reflect.Slice {
		fmt.Printf("input is not slice but %T\n", a)
		return ret
	}

	va := reflect.ValueOf(a)
	for i := 0; i < va.Len(); i++ {
		if i > 0 && reflect.DeepEqual(va.Index(i-1).Interface(), va.Index(i).Interface()) {
			continue
		}
		ret = append(ret, va.Index(i).Interface())
	}

	return ret
}
