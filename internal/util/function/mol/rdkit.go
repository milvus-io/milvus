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

// GenerateRDKitFingerprint generates an RDKit fingerprint for a SMILES string
// minPath: minimum path length (commonly 1)
// maxPath: maximum path length (commonly 7)
// fingerprintSize: number of bits (commonly 2048)
// Returns binary fingerprint as []byte
func GenerateRDKitFingerprint(smiles string, minPath int, maxPath int, fingerprintSize int) ([]byte, error) {
	if len(smiles) == 0 {
		byteSize := fingerprintSize / 8
		if fingerprintSize%8 != 0 {
			byteSize++
		}
		return make([]byte, byteSize), nil
	}

	return cgoGenerateRDKitFingerprint(smiles, minPath, maxPath, fingerprintSize)
}

// RDKitFingerprintToFloatVector converts binary RDKit fingerprint to float32 vector
// Each bit becomes 0.0 or 1.0
func RDKitFingerprintToFloatVector(fingerprint []byte, numBits int) []float32 {
	result := make([]float32, numBits)
	for i := 0; i < numBits && i/8 < len(fingerprint); i++ {
		if fingerprint[i/8]&(1<<(i%8)) != 0 {
			result[i] = 1.0
		}
	}
	return result
}

// GenerateRDKitFingerprintAsFloatVector generates RDKit fingerprint and converts to float32 vector
func GenerateRDKitFingerprintAsFloatVector(smiles string, minPath int, maxPath int, fingerprintSize int) ([]float32, error) {
	fp, err := GenerateRDKitFingerprint(smiles, minPath, maxPath, fingerprintSize)
	if err != nil {
		return nil, err
	}
	return RDKitFingerprintToFloatVector(fp, fingerprintSize), nil
}

// ConvertSMILESToPickle converts SMILES string to RDKit pickle format for storage
func ConvertSMILESToPickle(smiles string) ([]byte, error) {
	return cgoConvertSMILESToPickle(smiles)
}

// ConvertPickleToSMILES converts RDKit pickle format back to SMILES string
func ConvertPickleToSMILES(pickle []byte) (string, error) {
	return cgoConvertPickleToSMILES(pickle)
}
