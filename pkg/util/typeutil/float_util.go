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
	"encoding/binary"
	"fmt"
	"math"

	"github.com/cockroachdb/errors"
)

func bfloat16IsNaN(f uint16) bool {
	// the nan value of bfloat16 is x111 1111 1xxx xxxx
	return (f&0x7F80 == 0x7F80) && (f&0x007f != 0)
}

func bfloat16IsInf(f uint16, sign int) bool {
	// +inf: 0111 1111 1000 0000
	// -inf: 1111 1111 1000 0000
	return ((f == 0x7F80) && sign >= 0) ||
		(f == 0xFF80 && sign <= 0)
}

func float16IsNaN(f uint16) bool {
	// the nan value of bfloat16 is x111 1100 0000 0000
	return (f&0x7c00 == 0x7c00) && (f&0x03ff != 0)
}

func float16IsInf(f uint16, sign int) bool {
	// +inf: 0111 1100 0000 0000
	// -inf: 1111 1100 0000 0000
	return ((f == 0x7c00) && sign >= 0) ||
		(f == 0xfc00 && sign <= 0)
}

func VerifyFloat(value float64) error {
	// not allow not-a-number and infinity
	if math.IsNaN(value) || math.IsInf(value, -1) || math.IsInf(value, 1) {
		return fmt.Errorf("value '%f' is not a number or infinity", value)
	}

	return nil
}

func VerifyFloats32(values []float32) error {
	for _, f := range values {
		err := VerifyFloat(float64(f))
		if err != nil {
			return err
		}
	}

	return nil
}

func VerifyFloats64(values []float64) error {
	for _, f := range values {
		err := VerifyFloat(f)
		if err != nil {
			return err
		}
	}

	return nil
}

func VerifyFloats16(value []byte) error {
	if len(value)%2 != 0 {
		return errors.New("The length of float16 is not aligned to 2.")
	}
	dataSize := len(value) / 2
	for i := 0; i < dataSize; i++ {
		v := binary.LittleEndian.Uint16(value[i*2:])
		if float16IsNaN(v) || float16IsInf(v, -1) || float16IsInf(v, 1) {
			return errors.New("float16 vector contain nan or infinity value.")
		}
	}
	return nil
}

func VerifyBFloats16(value []byte) error {
	if len(value)%2 != 0 {
		return errors.New("The length of bfloat16 in not aligned to 2")
	}
	dataSize := len(value) / 2
	for i := 0; i < dataSize; i++ {
		v := binary.LittleEndian.Uint16(value[i*2:])
		if bfloat16IsNaN(v) || bfloat16IsInf(v, -1) || bfloat16IsInf(v, 1) {
			return errors.New("bfloat16 vector contain nan or infinity value.")
		}
	}
	return nil
}
