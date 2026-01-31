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

package function

import (
	"fmt"
	"strconv"
	"sync"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/mol"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// Helper function to get dimension from type parameters
func getDimFromTypeParams(params []*commonpb.KeyValuePair) (int, error) {
	for _, param := range params {
		if param.GetKey() == "dim" {
			dim, err := strconv.Atoi(param.GetValue())
			if err != nil {
				return 0, fmt.Errorf("invalid dim parameter: %s", param.GetValue())
			}
			return dim, nil
		}
	}
	return 0, fmt.Errorf("dim parameter not found")
}

const (
	paramFingerprintType   = "fingerprint_type"
	paramFingerprintSize   = "fingerprint_size"
	paramRadius            = "radius"
	paramMinPath           = "min_path"
	paramMaxPath           = "max_path"
	defaultFingerprintSize = 2048
	defaultRadius          = 2
	defaultMinPath         = 1
	defaultMaxPath         = 7
	maccsFingerprintSize   = 167 // MACCS fingerprint is fixed at 167 bits
)

const (
	fingerprintTypeMorgan = "morgan"
	fingerprintTypeMACCS  = "maccs"
	fingerprintTypeRDKit  = "rdkit"
)

// MolFingerprintFunctionRunner implements FunctionRunner for MOL fingerprint generation
// Input: MOL type (SMILES strings)
// Output: BINARY_VECTOR type (fingerprint vectors)
type MolFingerprintFunctionRunner struct {
	mu     sync.RWMutex
	closed bool

	schema      *schemapb.FunctionSchema
	outputField *schemapb.FieldSchema
	inputField  *schemapb.FieldSchema

	fingerprintType string
	fingerprintSize int
	radius          int
	minPath         int
	maxPath         int
}

// NewMolFingerprintFunctionRunner creates a new MolFingerprintFunctionRunner
func NewMolFingerprintFunctionRunner(coll *schemapb.CollectionSchema, schema *schemapb.FunctionSchema) (FunctionRunner, error) {
	if len(schema.GetOutputFieldIds()) != 1 {
		return nil, fmt.Errorf("mol fingerprint function should only have one output field, but now %d", len(schema.GetOutputFieldIds()))
	}

	if len(schema.GetInputFieldIds()) != 1 {
		return nil, fmt.Errorf("mol fingerprint function should only have one input field, but now %d", len(schema.GetInputFieldIds()))
	}

	var inputField, outputField *schemapb.FieldSchema
	for _, field := range coll.GetFields() {
		if field.GetFieldID() == schema.GetOutputFieldIds()[0] {
			outputField = field
		}
		if field.GetFieldID() == schema.GetInputFieldIds()[0] {
			inputField = field
		}
	}

	if inputField == nil {
		return nil, errors.New("input field not found")
	}
	if outputField == nil {
		return nil, errors.New("output field not found")
	}

	// Validate input field type
	if inputField.GetDataType() != schemapb.DataType_Mol {
		return nil, fmt.Errorf("mol fingerprint function input field must be MOL type, got %s", inputField.GetDataType().String())
	}

	// Validate output field type
	if outputField.GetDataType() != schemapb.DataType_BinaryVector {
		return nil, fmt.Errorf("mol fingerprint function output field must be BINARY_VECTOR type, got %s", outputField.GetDataType().String())
	}

	// Parse parameters
	fingerprintType := fingerprintTypeMorgan // default
	fingerprintSize := defaultFingerprintSize
	radius := defaultRadius
	minPath := defaultMinPath
	maxPath := defaultMaxPath

	for _, param := range schema.GetParams() {
		switch param.GetKey() {
		case paramFingerprintType:
			fingerprintType = param.GetValue()
			switch fingerprintType {
			case fingerprintTypeMorgan, fingerprintTypeMACCS, fingerprintTypeRDKit:
				// Valid types
			default:
				return nil, fmt.Errorf("unsupported fingerprint type: %s, supported types: morgan, maccs, rdkit", fingerprintType)
			}
		case paramFingerprintSize:
			size, err := strconv.Atoi(param.GetValue())
			if err != nil {
				return nil, fmt.Errorf("invalid fingerprint_size parameter: %s", param.GetValue())
			}
			if size <= 0 {
				return nil, fmt.Errorf("fingerprint_size must be positive, got %d", size)
			}
			fingerprintSize = size
		case paramRadius:
			r, err := strconv.Atoi(param.GetValue())
			if err != nil {
				return nil, fmt.Errorf("invalid radius parameter: %s", param.GetValue())
			}
			if r < 0 {
				return nil, fmt.Errorf("radius must be non-negative, got %d", r)
			}
			radius = r
		case paramMinPath:
			mp, err := strconv.Atoi(param.GetValue())
			if err != nil {
				return nil, fmt.Errorf("invalid min_path parameter: %s", param.GetValue())
			}
			if mp < 1 {
				return nil, fmt.Errorf("min_path must be at least 1, got %d", mp)
			}
			minPath = mp
		case paramMaxPath:
			mp, err := strconv.Atoi(param.GetValue())
			if err != nil {
				return nil, fmt.Errorf("invalid max_path parameter: %s", param.GetValue())
			}
			if mp < minPath {
				return nil, fmt.Errorf("max_path (%d) must be >= min_path (%d)", mp, minPath)
			}
			maxPath = mp
		}
	}

	// MACCS fingerprint has fixed size
	if fingerprintType == fingerprintTypeMACCS {
		fingerprintSize = maccsFingerprintSize
	}

	// Validate output field dimension matches fingerprint size
	dim, err := getDimFromTypeParams(outputField.GetTypeParams())
	if err != nil {
		return nil, fmt.Errorf("failed to get dimension from output field: %w", err)
	}
	if dim != fingerprintSize {
		return nil, fmt.Errorf("output field dimension (%d) does not match fingerprint_size (%d)", dim, fingerprintSize)
	}

	return &MolFingerprintFunctionRunner{
		schema:          schema,
		inputField:      inputField,
		outputField:     outputField,
		fingerprintType: fingerprintType,
		fingerprintSize: fingerprintSize,
		radius:          radius,
		minPath:         minPath,
		maxPath:         maxPath,
	}, nil
}

// BatchRun processes a batch of SMILES strings and generates fingerprint vectors
func (v *MolFingerprintFunctionRunner) BatchRun(inputs ...any) ([]any, error) {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if v.closed {
		return nil, errors.New("mol fingerprint function received request after closed")
	}

	if len(inputs) != 1 {
		return nil, errors.New("mol fingerprint function received more than one input column")
	}

	// Extract SMILES strings from input
	// Input can be []string (SMILES strings) or [][]byte (pickle data)
	var smilesData []string
	switch input := inputs[0].(type) {
	case []string:
		smilesData = input
	case [][]byte:
		// Convert [][]byte to []string
		smilesData = make([]string, len(input))
		for i, bytes := range input {
			smilesData[i] = string(bytes)
		}
	default:
		return nil, fmt.Errorf("mol fingerprint function batch input must be []string or [][]byte, got %T", inputs[0])
	}

	rowNum := len(smilesData)
	if rowNum == 0 {
		return []any{&storage.BinaryVectorFieldData{
			Data: []byte{},
			Dim:  v.fingerprintSize,
		}}, nil
	}

	// Generate fingerprints based on fingerprint type
	fingerprints := make([][]byte, rowNum)
	for i, smiles := range smilesData {
		if len(smiles) == 0 {
			// Empty SMILES -> zero fingerprint
			byteSize := v.fingerprintSize / 8
			if v.fingerprintSize%8 != 0 {
				byteSize++
			}
			fingerprints[i] = make([]byte, byteSize)
			continue
		}

		var fp []byte
		var err error

		switch v.fingerprintType {
		case fingerprintTypeMorgan:
			fp, err = mol.GenerateMorganFingerprint(smiles, v.radius, v.fingerprintSize)
		case fingerprintTypeMACCS:
			fp, err = mol.GenerateMACCSFingerprint(smiles)
		case fingerprintTypeRDKit:
			fp, err = mol.GenerateRDKitFingerprint(smiles, v.minPath, v.maxPath, v.fingerprintSize)
		default:
			return nil, fmt.Errorf("unsupported fingerprint type: %s", v.fingerprintType)
		}

		if err != nil {
			log.Warn("failed to generate fingerprint for SMILES",
				zap.String("smiles", smiles),
				zap.String("fingerprint_type", v.fingerprintType),
				zap.Int("index", i),
				zap.Error(err))
			return nil, merr.WrapErrParameterInvalidMsg("failed to generate %s fingerprint for SMILES %s: %v", v.fingerprintType, smiles, err)
		}
		fingerprints[i] = fp
	}

	// Convert fingerprints to BINARY_VECTOR format
	// BINARY_VECTOR stores bits packed into bytes
	byteSize := v.fingerprintSize / 8
	if v.fingerprintSize%8 != 0 {
		byteSize++
	}
	binaryVector := make([]byte, 0, rowNum*byteSize)
	for _, fp := range fingerprints {
		binaryVector = append(binaryVector, fp...)
	}

	// Return BinaryVectorFieldData
	outputData := &storage.BinaryVectorFieldData{
		Data: binaryVector,
		Dim:  v.fingerprintSize,
	}

	return []any{outputData}, nil
}

// GetSchema returns the function schema
func (v *MolFingerprintFunctionRunner) GetSchema() *schemapb.FunctionSchema {
	return v.schema
}

// GetOutputFields returns the output field schemas
func (v *MolFingerprintFunctionRunner) GetOutputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{v.outputField}
}

// GetInputFields returns the input field schemas
func (v *MolFingerprintFunctionRunner) GetInputFields() []*schemapb.FieldSchema {
	return []*schemapb.FieldSchema{v.inputField}
}

// Close closes the function runner
func (v *MolFingerprintFunctionRunner) Close() {
	v.mu.Lock()
	defer v.mu.Unlock()
	v.closed = true
}
