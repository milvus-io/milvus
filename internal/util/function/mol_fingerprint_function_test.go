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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function/mol"
)

func TestMolFingerprintFunctionRunnerSuite(t *testing.T) {
	suite.Run(t, new(MolFingerprintFunctionRunnerSuite))
}

type MolFingerprintFunctionRunnerSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *MolFingerprintFunctionRunnerSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "mol_field", DataType: schemapb.DataType_Mol},
			{
				FieldID: 102, Name: "fp_field", DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "2048"},
				},
			},
		},
	}
}

func (s *MolFingerprintFunctionRunnerSuite) newFuncSchema(params []*commonpb.KeyValuePair) *schemapb.FunctionSchema {
	return &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
		Params:         params,
	}
}

func (s *MolFingerprintFunctionRunnerSuite) TestValidateMolFingerprintFunction() {
	// valid
	err := ValidateMolFingerprintFunction(s.schema, &schemapb.FunctionSchema{
		InputFieldNames:  []string{"mol_field"},
		OutputFieldNames: []string{"fp_field"},
	})
	s.NoError(err)

	// wrong input field count
	err = ValidateMolFingerprintFunction(s.schema, &schemapb.FunctionSchema{
		InputFieldNames:  []string{},
		OutputFieldNames: []string{"fp_field"},
	})
	s.Error(err)

	// wrong output field count
	err = ValidateMolFingerprintFunction(s.schema, &schemapb.FunctionSchema{
		InputFieldNames:  []string{"mol_field"},
		OutputFieldNames: []string{},
	})
	s.Error(err)

	// input field not found
	err = ValidateMolFingerprintFunction(s.schema, &schemapb.FunctionSchema{
		InputFieldNames:  []string{"nonexistent"},
		OutputFieldNames: []string{"fp_field"},
	})
	s.Error(err)

	// output field not found
	err = ValidateMolFingerprintFunction(s.schema, &schemapb.FunctionSchema{
		InputFieldNames:  []string{"mol_field"},
		OutputFieldNames: []string{"nonexistent"},
	})
	s.Error(err)

	// input field wrong type
	wrongSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "text_field", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "fp_field", DataType: schemapb.DataType_BinaryVector},
		},
	}
	err = ValidateMolFingerprintFunction(wrongSchema, &schemapb.FunctionSchema{
		InputFieldNames:  []string{"text_field"},
		OutputFieldNames: []string{"fp_field"},
	})
	s.Error(err)

	// output field wrong type
	wrongSchema2 := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "mol_field", DataType: schemapb.DataType_Mol},
			{FieldID: 102, Name: "vec_field", DataType: schemapb.DataType_FloatVector},
		},
	}
	err = ValidateMolFingerprintFunction(wrongSchema2, &schemapb.FunctionSchema{
		InputFieldNames:  []string{"mol_field"},
		OutputFieldNames: []string{"vec_field"},
	})
	s.Error(err)
}

func (s *MolFingerprintFunctionRunnerSuite) TestNewRunner() {
	// default params (morgan, 2048)
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema(nil))
	s.NoError(err)
	s.NotNil(runner)

	// explicit morgan
	runner, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_type", Value: "morgan"},
	}))
	s.NoError(err)
	s.NotNil(runner)

	// rdkit type
	runner, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_type", Value: "rdkit"},
	}))
	s.NoError(err)
	s.NotNil(runner)

	// unsupported fingerprint type
	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_type", Value: "unknown"},
	}))
	s.Error(err)

	// invalid fingerprint_size
	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_size", Value: "abc"},
	}))
	s.Error(err)

	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_size", Value: "0"},
	}))
	s.Error(err)

	// invalid radius
	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "radius", Value: "abc"},
	}))
	s.Error(err)

	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "radius", Value: "-1"},
	}))
	s.Error(err)

	// invalid min_path
	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "min_path", Value: "0"},
	}))
	s.Error(err)

	// invalid max_path (< min_path)
	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "min_path", Value: "3"},
		{Key: "max_path", Value: "1"},
	}))
	s.Error(err)

	// dimension mismatch
	_, err = NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_size", Value: "1024"},
	}))
	s.Error(err)

	// missing output field
	_, err = NewMolFingerprintFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{999},
	})
	s.Error(err)

	// missing input field
	_, err = NewMolFingerprintFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{999},
		OutputFieldIds: []int64{102},
	})
	s.Error(err)

	// wrong field counts
	_, err = NewMolFingerprintFunctionRunner(s.schema, &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{},
	})
	s.Error(err)
}

func (s *MolFingerprintFunctionRunnerSuite) TestNewRunnerMACCS() {
	maccsSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "mol_field", DataType: schemapb.DataType_Mol},
			{
				FieldID: 102, Name: "fp_field", DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "167"},
				},
			},
		},
	}
	runner, err := NewMolFingerprintFunctionRunner(maccsSchema, &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: "fingerprint_type", Value: "maccs"},
		},
	})
	s.NoError(err)
	s.NotNil(runner)
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunMorgan() {
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema(nil))
	s.NoError(err)

	output, err := runner.BatchRun([]string{"CCO", "c1ccccc1"})
	s.NoError(err)
	s.Equal(1, len(output))

	result, ok := output[0].(*storage.BinaryVectorFieldData)
	s.True(ok)
	s.Equal(2048, result.Dim)
	s.Equal(2*2048/8, len(result.Data))
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunRDKit() {
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema([]*commonpb.KeyValuePair{
		{Key: "fingerprint_type", Value: "rdkit"},
	}))
	s.NoError(err)

	output, err := runner.BatchRun([]string{"CCO"})
	s.NoError(err)

	result, ok := output[0].(*storage.BinaryVectorFieldData)
	s.True(ok)
	s.Equal(2048, result.Dim)
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunMACCS() {
	maccsSchema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 101, Name: "mol_field", DataType: schemapb.DataType_Mol},
			{
				FieldID: 102, Name: "fp_field", DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "167"},
				},
			},
		},
	}
	runner, err := NewMolFingerprintFunctionRunner(maccsSchema, &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: "fingerprint_type", Value: "maccs"},
		},
	})
	s.NoError(err)

	output, err := runner.BatchRun([]string{"CCO"})
	s.NoError(err)

	result, ok := output[0].(*storage.BinaryVectorFieldData)
	s.True(ok)
	s.Equal(167, result.Dim)
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunPickleInput() {
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema(nil))
	s.NoError(err)

	pickle, err := mol.ConvertSMILESToPickle("CCO")
	s.NoError(err)

	output, err := runner.BatchRun([][]byte{pickle})
	s.NoError(err)

	result, ok := output[0].(*storage.BinaryVectorFieldData)
	s.True(ok)
	s.Equal(2048, result.Dim)
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunEmptySmiles() {
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema(nil))
	s.NoError(err)

	output, err := runner.BatchRun([]string{""})
	s.NoError(err)

	result, ok := output[0].(*storage.BinaryVectorFieldData)
	s.True(ok)
	s.Equal(2048/8, len(result.Data))
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunEmptyBatch() {
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema(nil))
	s.NoError(err)

	output, err := runner.BatchRun([]string{})
	s.NoError(err)

	result, ok := output[0].(*storage.BinaryVectorFieldData)
	s.True(ok)
	s.Equal(0, len(result.Data))
}

func (s *MolFingerprintFunctionRunnerSuite) TestBatchRunErrors() {
	runner, err := NewMolFingerprintFunctionRunner(s.schema, s.newFuncSchema(nil))
	s.NoError(err)

	// more than one input
	_, err = runner.BatchRun([]string{}, []string{})
	s.Error(err)

	// wrong input type
	_, err = runner.BatchRun([]int64{})
	s.Error(err)

	// invalid SMILES
	_, err = runner.BatchRun([]string{"not_valid_smiles!!!"})
	s.Error(err)

	// close and run
	runner.Close()
	_, err = runner.BatchRun([]string{"CCO"})
	s.Error(err)
}

func (s *MolFingerprintFunctionRunnerSuite) TestGetters() {
	funcSchema := s.newFuncSchema(nil)
	runner, err := NewMolFingerprintFunctionRunner(s.schema, funcSchema)
	s.NoError(err)

	s.Equal(funcSchema, runner.GetSchema())
	s.Equal(1, len(runner.GetOutputFields()))
	s.Equal("fp_field", runner.GetOutputFields()[0].GetName())
	s.Equal(1, len(runner.GetInputFields()))
	s.Equal("mol_field", runner.GetInputFields()[0].GetName())
}
