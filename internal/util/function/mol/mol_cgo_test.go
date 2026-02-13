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

func TestConvertSMILESToPickle(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		testCases := []string{
			"CCO",                // ethanol
			"c1ccccc1",          // benzene
			"CC(=O)O",           // acetic acid
			"CC(=O)Oc1ccccc1C(=O)O", // aspirin
		}
		for _, smiles := range testCases {
			pickle, err := ConvertSMILESToPickle(smiles)
			assert.NoError(t, err)
			assert.NotEmpty(t, pickle)
		}
	})

	t.Run("empty SMILES", func(t *testing.T) {
		_, err := cgoConvertSMILESToPickle("")
		assert.Error(t, err)
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := cgoConvertSMILESToPickle("not_a_valid_smiles_XYZ123!!!")
		assert.Error(t, err)
	})
}

func TestConvertPickleToSMILES(t *testing.T) {
	t.Run("roundtrip", func(t *testing.T) {
		testCases := []string{
			"CCO",
			"c1ccccc1",
			"CC(=O)O",
		}
		for _, smiles := range testCases {
			pickle, err := cgoConvertSMILESToPickle(smiles)
			assert.NoError(t, err)

			result, err := ConvertPickleToSMILES(pickle)
			assert.NoError(t, err)
			assert.NotEmpty(t, result)
		}
	})

	t.Run("empty pickle", func(t *testing.T) {
		_, err := cgoConvertPickleToSMILES(nil)
		assert.Error(t, err)

		_, err = cgoConvertPickleToSMILES([]byte{})
		assert.Error(t, err)
	})

	t.Run("invalid pickle", func(t *testing.T) {
		_, err := cgoConvertPickleToSMILES([]byte{0x00, 0x01, 0x02})
		assert.Error(t, err)
	})
}

func TestGenerateMorganFingerprint(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		fp, err := cgoGenerateMorganFingerprint("CCO", 2, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
	})

	t.Run("empty SMILES", func(t *testing.T) {
		_, err := cgoGenerateMorganFingerprint("", 2, 2048)
		assert.Error(t, err)
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := cgoGenerateMorganFingerprint("not_valid!!!", 2, 2048)
		assert.Error(t, err)
	})

	t.Run("invalid parameters", func(t *testing.T) {
		_, err := cgoGenerateMorganFingerprint("CCO", -1, 2048)
		assert.Error(t, err)

		_, err = cgoGenerateMorganFingerprint("CCO", 2, 0)
		assert.Error(t, err)
	})
}

func TestGenerateMACCSFingerprint(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		fp, err := cgoGenerateMACCSFingerprint("CCO")
		assert.NoError(t, err)
		assert.NotEmpty(t, fp)
		// MACCS fingerprint is 167 bits = 21 bytes
		assert.Equal(t, (167+7)/8, len(fp))
	})

	t.Run("empty SMILES", func(t *testing.T) {
		_, err := cgoGenerateMACCSFingerprint("")
		assert.Error(t, err)
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := cgoGenerateMACCSFingerprint("not_valid!!!")
		assert.Error(t, err)
	})
}

func TestGenerateRDKitFingerprint(t *testing.T) {
	t.Run("valid SMILES", func(t *testing.T) {
		fp, err := cgoGenerateRDKitFingerprint("CCO", 1, 7, 2048)
		assert.NoError(t, err)
		assert.Equal(t, 2048/8, len(fp))
	})

	t.Run("empty SMILES", func(t *testing.T) {
		_, err := cgoGenerateRDKitFingerprint("", 1, 7, 2048)
		assert.Error(t, err)
	})

	t.Run("invalid SMILES", func(t *testing.T) {
		_, err := cgoGenerateRDKitFingerprint("not_valid!!!", 1, 7, 2048)
		assert.Error(t, err)
	})

	t.Run("invalid parameters", func(t *testing.T) {
		_, err := cgoGenerateRDKitFingerprint("CCO", 0, 7, 2048)
		assert.Error(t, err)

		_, err = cgoGenerateRDKitFingerprint("CCO", 1, 0, 2048)
		assert.Error(t, err)

		_, err = cgoGenerateRDKitFingerprint("CCO", 1, 7, 0)
		assert.Error(t, err)
	})
}
