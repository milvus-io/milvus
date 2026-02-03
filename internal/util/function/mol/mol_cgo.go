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

/*
#cgo CFLAGS: -I${SRCDIR}/../../../core/src/common
#cgo LDFLAGS: -L${SRCDIR}/../../../core/output/lib -lmilvus_core -Wl,-rpath,${SRCDIR}/../../../core/output/lib

#include "mol_c.h"
#include <stdlib.h>
*/
import "C"
import (
	"fmt"
	"unsafe"
)

// cgoGenerateMorganFingerprint calls the C++ RDKit implementation
func cgoGenerateMorganFingerprint(smiles string, radius int, fingerprintSize int) ([]byte, error) {
	cSmiles := C.CString(smiles)
	defer C.free(unsafe.Pointer(cSmiles))

	result := C.GenerateMorganFingerprint(cSmiles, C.int(radius), C.int(fingerprintSize))
	defer C.FreeMolDataResult(&result)

	if result.error_code != C.MOL_SUCCESS {
		if result.error_msg != nil {
			return nil, fmt.Errorf("Morgan fingerprint generation failed: %s", C.GoString(result.error_msg))
		}
		return nil, fmt.Errorf("Morgan fingerprint generation failed with error code: %d", result.error_code)
	}

	if result.data == nil || result.size == 0 {
		return nil, fmt.Errorf("Morgan fingerprint generation returned empty result")
	}

	return C.GoBytes(unsafe.Pointer(result.data), C.int(result.size)), nil
}

// cgoGenerateMACCSFingerprint calls the C++ RDKit implementation
func cgoGenerateMACCSFingerprint(smiles string) ([]byte, error) {
	cSmiles := C.CString(smiles)
	defer C.free(unsafe.Pointer(cSmiles))

	result := C.GenerateMACCSFingerprint(cSmiles)
	defer C.FreeMolDataResult(&result)

	if result.error_code != C.MOL_SUCCESS {
		if result.error_msg != nil {
			return nil, fmt.Errorf("MACCS fingerprint generation failed: %s", C.GoString(result.error_msg))
		}
		return nil, fmt.Errorf("MACCS fingerprint generation failed with error code: %d", result.error_code)
	}

	if result.data == nil || result.size == 0 {
		return nil, fmt.Errorf("MACCS fingerprint generation returned empty result")
	}

	return C.GoBytes(unsafe.Pointer(result.data), C.int(result.size)), nil
}

// cgoGenerateRDKitFingerprint calls the C++ RDKit implementation
func cgoGenerateRDKitFingerprint(smiles string, minPath int, maxPath int, fingerprintSize int) ([]byte, error) {
	cSmiles := C.CString(smiles)
	defer C.free(unsafe.Pointer(cSmiles))

	result := C.GenerateRDKitFingerprint(cSmiles, C.int(minPath), C.int(maxPath), C.int(fingerprintSize))
	defer C.FreeMolDataResult(&result)

	if result.error_code != C.MOL_SUCCESS {
		if result.error_msg != nil {
			return nil, fmt.Errorf("RDKit fingerprint generation failed: %s", C.GoString(result.error_msg))
		}
		return nil, fmt.Errorf("RDKit fingerprint generation failed with error code: %d", result.error_code)
	}

	if result.data == nil || result.size == 0 {
		return nil, fmt.Errorf("RDKit fingerprint generation returned empty result")
	}

	return C.GoBytes(unsafe.Pointer(result.data), C.int(result.size)), nil
}

// cgoConvertSMILESToPickle converts SMILES to RDKit pickle format
func cgoConvertSMILESToPickle(smiles string) ([]byte, error) {
	cSmiles := C.CString(smiles)
	defer C.free(unsafe.Pointer(cSmiles))

	result := C.ConvertSMILESToPickle(cSmiles)
	defer C.FreeMolDataResult(&result)

	if result.error_code != C.MOL_SUCCESS {
		if result.error_msg != nil {
			return nil, fmt.Errorf("SMILES to pickle conversion failed: %s", C.GoString(result.error_msg))
		}
		return nil, fmt.Errorf("SMILES to pickle conversion failed with error code: %d", result.error_code)
	}

	if result.data == nil || result.size == 0 {
		return nil, fmt.Errorf("SMILES to pickle conversion returned empty result")
	}

	return C.GoBytes(unsafe.Pointer(result.data), C.int(result.size)), nil
}

// cgoConvertPickleToSMILES converts RDKit pickle format to SMILES
func cgoConvertPickleToSMILES(pickle []byte) (string, error) {
	if len(pickle) == 0 {
		return "", fmt.Errorf("empty pickle data")
	}

	result := C.ConvertPickleToSMILES((*C.uint8_t)(unsafe.Pointer(&pickle[0])), C.size_t(len(pickle)))
	defer C.FreeMolDataResult(&result)

	if result.error_code != C.MOL_SUCCESS {
		if result.error_msg != nil {
			return "", fmt.Errorf("pickle to SMILES conversion failed: %s", C.GoString(result.error_msg))
		}
		return "", fmt.Errorf("pickle to SMILES conversion failed with error code: %d", result.error_code)
	}

	if result.data == nil || result.size == 0 {
		return "", fmt.Errorf("pickle to SMILES conversion returned empty result")
	}

	// result.data contains null-terminated string
	return C.GoString((*C.char)(unsafe.Pointer(result.data))), nil
}
