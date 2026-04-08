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

func TestGenerateRDKitFingerprint(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		fp, err := GenerateRDKitFingerprint("CCO", 1, 7, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
	})

	t.Run("benzene", func(t *testing.T) {
		fp, err := GenerateRDKitFingerprint("c1ccccc1", 1, 7, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
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
		fp, err := GenerateRDKitFingerprint("", 1, 7, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
		for _, b := range fp {
			assert.Equal(t, byte(0), b)
		}
	})

	t.Run("empty SMILES with non-byte-aligned size", func(t *testing.T) {
		fp, err := GenerateRDKitFingerprint("", 1, 7, 167)
		assert.NoError(t, err)
		assert.Equal(t, 167/8+1, len(fp))
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := GenerateRDKitFingerprint("not_valid!!!", 1, 7, 2048)
		assert.Error(t, err)
	})

	t.Run("custom fingerprint size", func(t *testing.T) {
		fp, err := GenerateRDKitFingerprint("CCO", 1, 7, 1024)
		assert.NoError(t, err)
		assert.Equal(t, 1024/8, len(fp))
	})
}

func TestRDKitFingerprintToFloatVector(t *testing.T) {
	t.Run("all zeros", func(t *testing.T) {
		fp := make([]byte, 4) // 32 bits
		result := RDKitFingerprintToFloatVector(fp, 32)
		assert.Equal(t, 32, len(result))
		for _, v := range result {
			assert.Equal(t, float32(0.0), v)
		}
	})

	t.Run("all ones", func(t *testing.T) {
		fp := []byte{0xFF, 0xFF}
		result := RDKitFingerprintToFloatVector(fp, 16)
		assert.Equal(t, 16, len(result))
		for _, v := range result {
			assert.Equal(t, float32(1.0), v)
		}
	})

	t.Run("specific pattern", func(t *testing.T) {
		fp := []byte{0x01} // bit 0 set
		result := RDKitFingerprintToFloatVector(fp, 8)
		assert.Equal(t, float32(1.0), result[0])
		assert.Equal(t, float32(0.0), result[1])
	})
}

func TestGenerateRDKitFingerprintAsFloatVector(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		result, err := GenerateRDKitFingerprintAsFloatVector("CCO", 1, 7, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048, len(result))
		for _, v := range result {
			assert.True(t, v == 0.0 || v == 1.0)
		}
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := GenerateRDKitFingerprintAsFloatVector("not_valid!!!", 1, 7, 2048)
		assert.Error(t, err)
	})
}

func TestConvertSMILESToPickle(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		pickle, err := ConvertSMILESToPickle("CCO")
		assert.NoError(t, err)
		assert.True(t, len(pickle) > 0)
	})

	t.Run("benzene", func(t *testing.T) {
		pickle, err := ConvertSMILESToPickle("c1ccccc1")
		assert.NoError(t, err)
		assert.True(t, len(pickle) > 0)
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := ConvertSMILESToPickle("not_valid!!!")
		assert.Error(t, err)
	})
}

func TestConvertPickleToSMILES(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		pickle, err := ConvertSMILESToPickle("CCO")
		assert.NoError(t, err)

		smiles, err := ConvertPickleToSMILES(pickle)
		assert.NoError(t, err)
		assert.Equal(t, "CCO", smiles)
	})

	t.Run("benzene roundtrip", func(t *testing.T) {
		pickle, err := ConvertSMILESToPickle("c1ccccc1")
		assert.NoError(t, err)

		smiles, err := ConvertPickleToSMILES(pickle)
		assert.NoError(t, err)
		assert.NotEmpty(t, smiles)
	})

	t.Run("invalid pickle", func(t *testing.T) {
		_, err := ConvertPickleToSMILES([]byte{0x00, 0x01, 0x02})
		assert.Error(t, err)
	})
}
