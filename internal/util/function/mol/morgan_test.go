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

func TestGenerateMorganFingerprint(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		fp, err := GenerateMorganFingerprint("CCO", 2, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
	})

	t.Run("benzene", func(t *testing.T) {
		fp, err := GenerateMorganFingerprint("c1ccccc1", 2, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
		// benzene should have some bits set
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
		fp, err := GenerateMorganFingerprint("", 2, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
		for _, b := range fp {
			assert.Equal(t, byte(0), b)
		}
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := GenerateMorganFingerprint("not_valid!!!", 2, 2048)
		assert.Error(t, err)
	})

	t.Run("different radius produces different fingerprints", func(t *testing.T) {
		fp1, err := GenerateMorganFingerprint("c1ccccc1", 1, 2048)
		assert.NoError(t, err)
		fp2, err := GenerateMorganFingerprint("c1ccccc1", 3, 2048)
		assert.NoError(t, err)
		assert.Equal(t, len(fp1), len(fp2))
	})

	t.Run("custom fingerprint size", func(t *testing.T) {
		fp, err := GenerateMorganFingerprint("CCO", 2, 1024)
		assert.NoError(t, err)
		assert.Equal(t, 1024/8, len(fp))
	})
}

func TestMorganFingerprintToFloatVector(t *testing.T) {
	t.Run("all zeros", func(t *testing.T) {
		fp := make([]byte, 4) // 32 bits
		result := MorganFingerprintToFloatVector(fp, 32)
		assert.Equal(t, 32, len(result))
		for _, v := range result {
			assert.Equal(t, float32(0.0), v)
		}
	})

	t.Run("all ones", func(t *testing.T) {
		fp := []byte{0xFF, 0xFF}
		result := MorganFingerprintToFloatVector(fp, 16)
		assert.Equal(t, 16, len(result))
		for _, v := range result {
			assert.Equal(t, float32(1.0), v)
		}
	})

	t.Run("specific pattern", func(t *testing.T) {
		fp := []byte{0x01} // bit 0 set
		result := MorganFingerprintToFloatVector(fp, 8)
		assert.Equal(t, float32(1.0), result[0])
		assert.Equal(t, float32(0.0), result[1])
	})
}

func TestGenerateMorganFingerprintAsFloatVector(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		result, err := GenerateMorganFingerprintAsFloatVector("CCO", 2, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048, len(result))
		for _, v := range result {
			assert.True(t, v == 0.0 || v == 1.0)
		}
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := GenerateMorganFingerprintAsFloatVector("not_valid!!!", 2, 2048)
		assert.Error(t, err)
	})
}
