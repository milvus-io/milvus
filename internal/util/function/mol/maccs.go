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

const (
	// MACCSNumBits is the fixed size of MACCS fingerprint
	MACCSNumBits = 167
)

// GenerateMACCSFingerprint generates a MACCS fingerprint for a SMILES string
// MACCS fingerprint is fixed at 167 bits
// Returns binary fingerprint as []byte
func GenerateMACCSFingerprint(smiles string) ([]byte, error) {
	if len(smiles) == 0 {
		return make([]byte, MACCSNumBits/8+1), nil
	}

	return cgoGenerateMACCSFingerprint(smiles)
}

// MACCSFingerprintToFloatVector converts binary MACCS fingerprint to float32 vector
// Each bit becomes 0.0 or 1.0
func MACCSFingerprintToFloatVector(fingerprint []byte) []float32 {
	result := make([]float32, MACCSNumBits)
	for i := 0; i < MACCSNumBits && i/8 < len(fingerprint); i++ {
		if fingerprint[i/8]&(1<<(i%8)) != 0 {
			result[i] = 1.0
		}
	}
	return result
}

// GenerateMACCSFingerprintAsFloatVector generates MACCS fingerprint and converts to float32 vector
func GenerateMACCSFingerprintAsFloatVector(smiles string) ([]float32, error) {
	fp, err := GenerateMACCSFingerprint(smiles)
	if err != nil {
		return nil, err
	}
	return MACCSFingerprintToFloatVector(fp), nil
}
