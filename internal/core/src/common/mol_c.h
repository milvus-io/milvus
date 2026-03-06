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

#ifndef MILVUS_MOL_C_H
#define MILVUS_MOL_C_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

// Error codes
#define MOL_SUCCESS 0
#define MOL_ERROR_INVALID_SMILES 1
#define MOL_ERROR_PICKLE_FAILED 2
#define MOL_ERROR_FINGERPRINT_FAILED 3
#define MOL_ERROR_MEMORY 4

// Result structure for operations that return binary data
typedef struct {
    uint8_t* data;      // Binary data (caller must free with FreeMolDataResult)
    size_t size;        // Size of data in bytes
    int32_t error_code; // Error code (0 = success)
    char* error_msg;    // Error message (caller must free with FreeMolDataResult if not NULL)
} MolDataResult;

// Free the MolDataResult structure
void FreeMolDataResult(MolDataResult* result);

// Convert SMILES string to Pickle binary format
// Returns MolDataResult with pickle data on success, or error on failure
MolDataResult ConvertSMILESToPickle(const char* smiles);

// Convert Pickle binary data to SMILES string
// Returns MolDataResult with SMILES string (as bytes) on success, or error on failure
MolDataResult ConvertPickleToSMILES(const uint8_t* pickle_data, size_t pickle_size);

// Generate Morgan fingerprint from SMILES
// radius: fingerprint radius (e.g., 2)
// fingerprint_size: number of bits (e.g., 2048)
// Returns MolDataResult with binary fingerprint on success, or error on failure
MolDataResult GenerateMorganFingerprint(const char* smiles, int radius, int fingerprint_size);

// Generate MACCS fingerprint from SMILES
// Returns MolDataResult with binary fingerprint (167 bits) on success, or error on failure
MolDataResult GenerateMACCSFingerprint(const char* smiles);

// Generate RDKit fingerprint from SMILES
// min_path: minimum path length (e.g., 1)
// max_path: maximum path length (e.g., 7)
// fingerprint_size: number of bits (e.g., 2048)
// Returns MolDataResult with binary fingerprint on success, or error on failure
MolDataResult GenerateRDKitFingerprint(const char* smiles, int min_path, int max_path, int fingerprint_size);

#ifdef __cplusplus
}
#endif

#endif // MILVUS_MOL_C_H
