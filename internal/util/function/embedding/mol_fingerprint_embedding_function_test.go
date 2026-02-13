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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/mol"
)

func TestMolFingerprintEmbeddingFunction(t *testing.T) {
	suite.Run(t, new(MolFingerprintEmbeddingFunctionSuite))
}

type MolFingerprintEmbeddingFunctionSuite struct {
	suite.Suite
	schema     *schemapb.CollectionSchema
	funcSchema *schemapb.FunctionSchema
}

func (s *MolFingerprintEmbeddingFunctionSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test_mol",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "mol_field", DataType: schemapb.DataType_Mol},
			{
				FieldID: 102, Name: "fp_field", DataType: schemapb.DataType_BinaryVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "2048"},
				},
			},
		},
	}
	s.funcSchema = &schemapb.FunctionSchema{
		Name:           "mol_fp",
		Type:           schemapb.FunctionType_MolFingerprint,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
		InputFieldNames:  []string{"mol_field"},
		OutputFieldNames: []string{"fp_field"},
	}
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestNewFunction() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)
	s.NotNil(f)
	s.Equal("test_mol", f.GetCollectionName())
	s.Equal("mol_fp", f.GetFunctionName())
	s.Equal("local_rdkit", f.GetFunctionProvider())
	s.Equal(1024, f.MaxBatch())
	s.NoError(f.Check(context.Background()))
	s.Equal(1, len(f.GetOutputFields()))
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessInsertMolSmilesData() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	inputs := []*schemapb.FieldData{
		{
			Type:    schemapb.DataType_Mol,
			FieldId: 101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_MolSmilesData{
						MolSmilesData: &schemapb.MolSmilesArray{
							Data: []string{"CCO", "c1ccccc1"},
						},
					},
				},
			},
		},
	}

	result, err := f.ProcessInsert(context.Background(), inputs)
	s.NoError(err)
	s.Equal(1, len(result))
	s.Equal(schemapb.DataType_BinaryVector, result[0].GetType())
	s.Equal(int64(2048), result[0].GetVectors().GetDim())
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessInsertStringData() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	inputs := []*schemapb.FieldData{
		{
			Type:    schemapb.DataType_VarChar,
			FieldId: 101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{"CCO"},
						},
					},
				},
			},
		},
	}

	result, err := f.ProcessInsert(context.Background(), inputs)
	s.NoError(err)
	s.Equal(1, len(result))
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessInsertErrors() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	// wrong input count
	_, err = f.ProcessInsert(context.Background(), []*schemapb.FieldData{})
	s.Error(err)

	_, err = f.ProcessInsert(context.Background(), []*schemapb.FieldData{{}, {}})
	s.Error(err)

	// unsupported input type
	_, err = f.ProcessInsert(context.Background(), []*schemapb.FieldData{
		{
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{Data: []int32{1}},
					},
				},
			},
		},
	})
	s.Error(err)
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessSearch() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	placeholderGroup := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_VarChar,
				Values: [][]byte{[]byte("CCO")},
			},
		},
	}

	result, err := f.ProcessSearch(context.Background(), placeholderGroup)
	s.NoError(err)
	s.Equal(1, len(result.GetPlaceholders()))
	s.Equal(commonpb.PlaceholderType_BinaryVector, result.GetPlaceholders()[0].GetType())
	s.Equal(1, len(result.GetPlaceholders()[0].GetValues()))
	s.Equal(2048/8, len(result.GetPlaceholders()[0].GetValues()[0]))
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessSearchEmptySmiles() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	placeholderGroup := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_VarChar,
				Values: [][]byte{[]byte("")},
			},
		},
	}

	result, err := f.ProcessSearch(context.Background(), placeholderGroup)
	s.NoError(err)
	s.Equal(1, len(result.GetPlaceholders()[0].GetValues()))
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessSearchInvalidSmiles() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	placeholderGroup := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:    "$0",
				Type:   commonpb.PlaceholderType_VarChar,
				Values: [][]byte{[]byte("not_valid!!!")},
			},
		},
	}

	_, err = f.ProcessSearch(context.Background(), placeholderGroup)
	s.Error(err)
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessBulkInsert() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	_, err = f.ProcessBulkInsert(context.Background(), nil)
	s.Error(err)
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestBinaryVectorsToPlaceholderGroup() {
	// 2 vectors, dim=16 (2 bytes each)
	data := []byte{0xFF, 0x00, 0x00, 0xFF}
	result := BinaryVectorsToPlaceholderGroup(data, 16)

	s.Equal(1, len(result.GetPlaceholders()))
	s.Equal(commonpb.PlaceholderType_BinaryVector, result.GetPlaceholders()[0].GetType())
	s.Equal(2, len(result.GetPlaceholders()[0].GetValues()))
	s.Equal([]byte{0xFF, 0x00}, result.GetPlaceholders()[0].GetValues()[0])
	s.Equal([]byte{0x00, 0xFF}, result.GetPlaceholders()[0].GetValues()[1])
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestBinaryVectorsToPlaceholderGroupOddDim() {
	// dim=9 -> 2 bytes per vector, 1 vector
	data := []byte{0xFF, 0x01}
	result := BinaryVectorsToPlaceholderGroup(data, 9)

	s.Equal(1, len(result.GetPlaceholders()[0].GetValues()))
	s.Equal(2, len(result.GetPlaceholders()[0].GetValues()[0]))
}

func (s *MolFingerprintEmbeddingFunctionSuite) TestProcessInsertMolData() {
	f, err := NewMolFingerprintEmbeddingFunction(s.schema, s.funcSchema)
	s.NoError(err)

	pickle, err := mol.ConvertSMILESToPickle("CCO")
	s.NoError(err)

	inputs := []*schemapb.FieldData{
		{
			Type:    schemapb.DataType_Mol,
			FieldId: 101,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_MolData{
						MolData: &schemapb.MolArray{
							Data: [][]byte{pickle},
						},
					},
				},
			},
		},
	}

	result, err := f.ProcessInsert(context.Background(), inputs)
	s.NoError(err)
	s.Equal(1, len(result))
}
