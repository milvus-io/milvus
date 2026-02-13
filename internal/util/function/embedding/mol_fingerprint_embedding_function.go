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

package embedding

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/function/mol"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

// MolFingerprintEmbeddingFunction wraps the MolFingerprintFunctionRunner for search operations
type MolFingerprintEmbeddingFunction struct {
	schema       *schemapb.FunctionSchema
	outputFields []*schemapb.FieldSchema
	runner       function.FunctionRunner

	collectionName   string
	functionTypeName string
	functionName     string
}

func NewMolFingerprintEmbeddingFunction(coll *schemapb.CollectionSchema, functionSchema *schemapb.FunctionSchema) (*MolFingerprintEmbeddingFunction, error) {
	runner, err := function.NewMolFingerprintFunctionRunner(coll, functionSchema)
	if err != nil {
		return nil, err
	}

	f := &MolFingerprintEmbeddingFunction{
		schema:           functionSchema,
		outputFields:     runner.GetOutputFields(),
		runner:           runner,
		collectionName:   coll.Name,
		functionTypeName: functionSchema.GetType().String(),
		functionName:     functionSchema.Name,
	}

	return f, nil
}

func (f *MolFingerprintEmbeddingFunction) GetSchema() *schemapb.FunctionSchema {
	return f.schema
}

func (f *MolFingerprintEmbeddingFunction) GetOutputFields() []*schemapb.FieldSchema {
	return f.outputFields
}

func (f *MolFingerprintEmbeddingFunction) GetCollectionName() string {
	return f.collectionName
}

func (f *MolFingerprintEmbeddingFunction) GetFunctionTypeName() string {
	return f.functionTypeName
}

func (f *MolFingerprintEmbeddingFunction) GetFunctionProvider() string {
	return "local_rdkit"
}

func (f *MolFingerprintEmbeddingFunction) GetFunctionName() string {
	return f.functionName
}

func (f *MolFingerprintEmbeddingFunction) Check(ctx context.Context) error {
	// Local RDKit, no external service to check
	return nil
}

func (f *MolFingerprintEmbeddingFunction) MaxBatch() int {
	return 1024 // Reasonable batch size for local processing
}

func (f *MolFingerprintEmbeddingFunction) ProcessInsert(ctx context.Context, inputs []*schemapb.FieldData) ([]*schemapb.FieldData, error) {
	if len(inputs) != 1 {
		return nil, fmt.Errorf("MolFingerprint function only receives one input, but got [%d]", len(inputs))
	}

	// Get SMILES strings from input
	// Client sends MolSmilesData (SMILES strings), which may be converted to MolData (pickle) by validation
	var smilesList []string
	if molSmilesData := inputs[0].GetScalars().GetMolSmilesData(); molSmilesData != nil {
		// Direct SMILES data from client
		smilesList = molSmilesData.GetData()
	} else if stringData := inputs[0].GetScalars().GetStringData(); stringData != nil {
		// String data (also SMILES)
		smilesList = stringData.GetData()
	} else if molData := inputs[0].GetScalars().GetMolData(); molData != nil {
		// MOL data in pickle format, need to convert back to SMILES for fingerprint generation
		molBytes := molData.GetData()
		smilesList = make([]string, len(molBytes))
		for i, bytes := range molBytes {
			smilesList[i] = string(bytes)
		}
	} else {
		return nil, fmt.Errorf("MolFingerprint function input must be MOL or string type, got: %T", inputs[0].GetScalars().GetData())
	}

	// Generate fingerprints
	output, err := f.runner.BatchRun(smilesList)
	if err != nil {
		return nil, fmt.Errorf("failed to generate fingerprints: %w", err)
	}

	binaryVectorData, ok := output[0].(*storage.BinaryVectorFieldData)
	if !ok {
		return nil, fmt.Errorf("MolFingerprint runner returned unexpected type: %T", output[0])
	}

	// Convert to FieldData
	outputField := f.outputFields[0]
	fieldData := &schemapb.FieldData{
		Type:      schemapb.DataType_BinaryVector,
		FieldName: outputField.GetName(),
		FieldId:   outputField.GetFieldID(),
		Field: &schemapb.FieldData_Vectors{
			Vectors: &schemapb.VectorField{
				Dim: int64(binaryVectorData.Dim),
				Data: &schemapb.VectorField_BinaryVector{
					BinaryVector: binaryVectorData.Data,
				},
			},
		},
	}

	return []*schemapb.FieldData{fieldData}, nil
}

func (f *MolFingerprintEmbeddingFunction) ProcessSearch(ctx context.Context, placeholderGroup *commonpb.PlaceholderGroup) (*commonpb.PlaceholderGroup, error) {
	// Get SMILES strings from placeholder
	smilesList := funcutil.GetVarCharFromPlaceholder(placeholderGroup.Placeholders[0])

	numRows := len(smilesList)
	if numRows > f.MaxBatch() {
		return nil, fmt.Errorf("MolFingerprint supports up to [%d] pieces of data at a time, got [%d]", f.MaxBatch(), numRows)
	}

	// Convert SMILES to pickle and back to canonical SMILES to match storage path
	// Storage: SMILES -> Pickle -> canonical SMILES -> fingerprint
	// Search must follow the same path for consistency
	canonicalSmilesList := make([]string, len(smilesList))
	for i, smiles := range smilesList {
		if len(smiles) == 0 {
			canonicalSmilesList[i] = ""
			continue
		}
		// Convert to pickle and back to get canonical SMILES
		pickle, err := mol.ConvertSMILESToPickle(smiles)
		if err != nil {
			return nil, fmt.Errorf("failed to convert SMILES to pickle at index %d: %w", i, err)
		}
		canonical, err := mol.ConvertPickleToSMILES(pickle)
		if err != nil {
			return nil, fmt.Errorf("failed to convert pickle to canonical SMILES at index %d: %w", i, err)
		}
		canonicalSmilesList[i] = canonical
	}

	// Generate fingerprints using the runner with canonical SMILES
	output, err := f.runner.BatchRun(canonicalSmilesList)
	if err != nil {
		return nil, fmt.Errorf("failed to generate fingerprints for search: %w", err)
	}

	binaryVectorData, ok := output[0].(*storage.BinaryVectorFieldData)
	if !ok {
		return nil, fmt.Errorf("MolFingerprint runner returned unexpected type: %T", output[0])
	}

	// Convert binary vectors to PlaceholderGroup
	return BinaryVectorsToPlaceholderGroup(binaryVectorData.Data, binaryVectorData.Dim), nil
}

func (f *MolFingerprintEmbeddingFunction) ProcessBulkInsert(ctx context.Context, inputs []storage.FieldData) (map[storage.FieldID]storage.FieldData, error) {
	// BulkInsert is handled by the pipeline embedding node, not here
	return nil, fmt.Errorf("MolFingerprintEmbeddingFunction.ProcessBulkInsert should not be called directly")
}

// BinaryVectorsToPlaceholderGroup converts binary vector data to a PlaceholderGroup
func BinaryVectorsToPlaceholderGroup(data []byte, dim int) *commonpb.PlaceholderGroup {
	bytesPerVector := dim / 8
	if dim%8 != 0 {
		bytesPerVector++
	}

	numVectors := len(data) / bytesPerVector
	values := make([][]byte, numVectors)

	for i := 0; i < numVectors; i++ {
		start := i * bytesPerVector
		end := start + bytesPerVector
		values[i] = data[start:end]
	}

	return &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{{
			Tag:    "$0",
			Type:   commonpb.PlaceholderType_BinaryVector,
			Values: values,
		}},
	}
}
