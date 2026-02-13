/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package mol

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenerateMACCSFingerprint(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		fp, err := GenerateMACCSFingerprint("CCO")
		assert.NoError(t, err)
		assert.Equal(t, MACCSNumBits/8+1, len(fp))
	})

	t.Run("benzene", func(t *testing.T) {
		fp, err := GenerateMACCSFingerprint("c1ccccc1")
		assert.NoError(t, err)
		assert.Equal(t, MACCSNumBits/8+1, len(fp))
		hasNonZero := false
		for _, b := range fp {
			if b != 0 {
				hasNonZero = true
				break
			}
		}
		assert.True(t, hasNonZero)
	})

	t.Run("empty SMILES returns zero vector", func(t *testing.T) {
		fp, err := GenerateMACCSFingerprint("")
		assert.NoError(t, err)
		assert.Equal(t, MACCSNumBits/8+1, len(fp))
		for _, b := range fp {
			assert.Equal(t, byte(0), b)
		}
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := GenerateMACCSFingerprint("not_valid!!!")
		assert.Error(t, err)
	})
}

func TestMACCSFingerprintToFloatVector(t *testing.T) {
	t.Run("all zeros", func(t *testing.T) {
		fp := make([]byte, MACCSNumBits/8+1)
		result := MACCSFingerprintToFloatVector(fp)
		assert.Equal(t, MACCSNumBits, len(result))
		for _, v := range result {
			assert.Equal(t, float32(0.0), v)
		}
	})

	t.Run("all ones", func(t *testing.T) {
		fp := []byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
			0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF}
		result := MACCSFingerprintToFloatVector(fp)
		assert.Equal(t, MACCSNumBits, len(result))
		for _, v := range result {
			assert.Equal(t, float32(1.0), v)
		}
	})
}

func TestGenerateMACCSFingerprintAsFloatVector(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		result, err := GenerateMACCSFingerprintAsFloatVector("CCO")
		assert.NoError(t, err)
		assert.Equal(t, MACCSNumBits, len(result))
		for _, v := range result {
			assert.True(t, v == 0.0 || v == 1.0)
		}
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := GenerateMACCSFingerprintAsFloatVector("not_valid!!!")
		assert.Error(t, err)
	})
}

func TestMACCSNumBitsConstant(t *testing.T) {
	assert.Equal(t, 167, MACCSNumBits)
}
