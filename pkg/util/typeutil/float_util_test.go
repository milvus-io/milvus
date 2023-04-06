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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_VerifyFloat(t *testing.T) {
	var value = math.NaN()
	err := VerifyFloat(value)
	assert.Error(t, err)

	value = math.Inf(1)
	err = VerifyFloat(value)
	assert.Error(t, err)

	value = math.Inf(-1)
	err = VerifyFloat(value)
	assert.Error(t, err)
}

func Test_VerifyFloats32(t *testing.T) {
	data := []float32{2.5, 32.2, 53.254}
	err := VerifyFloats32(data)
	assert.NoError(t, err)

	data = []float32{2.5, 32.2, 53.254, float32(math.NaN())}
	err = VerifyFloats32(data)
	assert.Error(t, err)

	data = []float32{2.5, 32.2, 53.254, float32(math.Inf(1))}
	err = VerifyFloats32(data)
	assert.Error(t, err)
}

func Test_VerifyFloats64(t *testing.T) {
	data := []float64{2.5, 32.2, 53.254}
	err := VerifyFloats64(data)
	assert.NoError(t, err)

	data = []float64{2.5, 32.2, 53.254, math.NaN()}
	err = VerifyFloats64(data)
	assert.Error(t, err)

	data = []float64{2.5, 32.2, 53.254, math.Inf(-1)}
	err = VerifyFloats64(data)
	assert.Error(t, err)
}
