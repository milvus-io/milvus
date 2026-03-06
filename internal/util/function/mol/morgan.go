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

// GenerateMorganFingerprint generates a Morgan fingerprint for a SMILES string
// radius: fingerprint radius (commonly 2 for ECFP4)
// fingerprintSize: number of bits (commonly 2048)
// Returns binary fingerprint as []byte
func GenerateMorganFingerprint(smiles string, radius int, fingerprintSize int) ([]byte, error) {
	if len(smiles) == 0 {
		return make([]byte, fingerprintSize/8), nil
	}

	return cgoGenerateMorganFingerprint(smiles, radius, fingerprintSize)
}

// MorganFingerprintToFloatVector converts binary fingerprint to float32 vector
// Each bit becomes 0.0 or 1.0
func MorganFingerprintToFloatVector(fingerprint []byte, numBits int) []float32 {
	result := make([]float32, numBits)
	for i := 0; i < numBits && i/8 < len(fingerprint); i++ {
		if fingerprint[i/8]&(1<<(i%8)) != 0 {
			result[i] = 1.0
		}
	}
	return result
}

// GenerateMorganFingerprintAsFloatVector generates Morgan fingerprint and converts to float32 vector
func GenerateMorganFingerprintAsFloatVector(smiles string, radius int, fingerprintSize int) ([]float32, error) {
	fp, err := GenerateMorganFingerprint(smiles, radius, fingerprintSize)
	if err != nil {
		return nil, err
	}
	return MorganFingerprintToFloatVector(fp, fingerprintSize), nil
}
