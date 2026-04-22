// Copyright 2025 Zilliz
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package embedding

import (
	"context"
	"fmt"

	"github.com/cockroachdb/errors"
	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// RunOptions packs runtime context required by some function runners.
type RunOptions struct {
	// ClusterID and DBName feed TextEmbedding model RPC headers.
	ClusterID string
	DBName    string
	// AllowNonBM25Outputs controls whether GetNeedProcessFunctions accepts
	// pre-existing non-BM25 outputs in the data (import sets via collection
	// property; external table refresh leaves false).
	AllowNonBM25Outputs bool
}

// RunAll executes TextEmbedding, BM25, and MinHash functions declared on
// schema over data in-place. Function outputs are written into data.Data
// keyed by the output field id.
//
// Order matters: TextEmbedding must run before BM25/MinHash. RunTextEmbedding
// calls GetNeedProcessFunctions which rejects any input containing BM25 output
// field ids (it cannot tell a freshly-computed column from a user-supplied
// one). Populating those columns first would cause the subsequent
// GetNeedProcessFunctions check to error.
//
// Single canonical entry point shared by import and external-table refresh.
func RunAll(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	data *storage.InsertData,
	opts RunOptions,
) error {
	if err := RunTextEmbedding(ctx, schema, data, opts); err != nil {
		return err
	}
	if err := RunBM25(schema, data); err != nil {
		return err
	}
	return RunMinHash(schema, data)
}

// RunBM25 executes every BM25 function in schema over data, dispatching the
// runner output into the appropriate FieldData type for each output field.
func RunBM25(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() != schemapb.FunctionType_BM25 {
			continue
		}
		if err := runOne(schema, fn, data, assignBM25Output); err != nil {
			return err
		}
	}
	return nil
}

// RunMinHash executes every MinHash function in schema over data.
func RunMinHash(schema *schemapb.CollectionSchema, data *storage.InsertData) error {
	for _, fn := range schema.GetFunctions() {
		if fn.GetType() != schemapb.FunctionType_MinHash {
			continue
		}
		if err := runOne(schema, fn, data, assignMinHashOutput); err != nil {
			return err
		}
	}
	return nil
}

// RunTextEmbedding executes any non-BM25/non-MinHash functions (currently
// TextEmbedding) via the batched function executor.
func RunTextEmbedding(
	ctx context.Context,
	schema *schemapb.CollectionSchema,
	data *storage.InsertData,
	opts RunOptions,
) error {
	if !HasNonBM25AndMinHashFunctions(schema.GetFunctions(), []int64{}) {
		return nil
	}
	fieldIDs := lo.Keys(lo.PickBy(data.Data, func(_ int64, fd storage.FieldData) bool {
		return fd.RowNum() > 0
	}))
	needProcess, err := typeutil.GetNeedProcessFunctions(
		fieldIDs, schema.GetFunctions(), opts.AllowNonBM25Outputs, false)
	if err != nil {
		return err
	}
	if len(needProcess) == 0 {
		return nil
	}
	exec, err := NewFunctionExecutor(schema, needProcess,
		&models.ModelExtraInfo{ClusterID: opts.ClusterID, DBName: opts.DBName})
	if err != nil {
		return err
	}
	if err := exec.ProcessBulkInsert(ctx, data); err != nil {
		return fmt.Errorf("text embedding: %w", err)
	}
	return nil
}

// runOne is the shared body for BM25 and MinHash: build a runner, collect
// inputs, BatchRun, then hand outputs to the type-specific assignFn.
func runOne(
	schema *schemapb.CollectionSchema,
	fn *schemapb.FunctionSchema,
	data *storage.InsertData,
	assignFn func(*schemapb.CollectionSchema, *schemapb.FunctionSchema, function.FunctionRunner, []any, *storage.InsertData) error,
) error {
	runner, err := function.NewFunctionRunner(schema, fn)
	if err != nil {
		return fmt.Errorf("%s runner: %w", fn.GetType(), err)
	}
	if runner == nil {
		return nil
	}
	defer runner.Close()

	inputs := make([]any, 0, len(runner.GetInputFields()))
	for _, f := range runner.GetInputFields() {
		inputs = append(inputs, data.Data[f.GetFieldID()].GetDataRows())
	}
	outputs, err := runner.BatchRun(inputs...)
	if err != nil {
		return fmt.Errorf("%s execution: %w", fn.GetType(), err)
	}
	return assignFn(schema, fn, runner, outputs, data)
}

func assignBM25Output(
	schema *schemapb.CollectionSchema,
	fn *schemapb.FunctionSchema,
	_ function.FunctionRunner,
	outputs []any,
	data *storage.InsertData,
) error {
	if len(outputs) != len(fn.GetOutputFieldIds()) {
		return fmt.Errorf("BM25 runner output count mismatch: got %d, expected %d",
			len(outputs), len(fn.GetOutputFieldIds()))
	}
	for i, outID := range fn.GetOutputFieldIds() {
		outField := typeutil.GetField(schema, outID)
		switch outField.GetDataType() {
		case schemapb.DataType_FloatVector:
			fd, ok := outputs[i].(*storage.FloatVectorFieldData)
			if !ok {
				return fmt.Errorf("BM25 output %d: want *FloatVectorFieldData, got %T", outID, outputs[i])
			}
			data.Data[outID] = fd
		case schemapb.DataType_BFloat16Vector:
			fd, ok := outputs[i].(*storage.BFloat16VectorFieldData)
			if !ok {
				return fmt.Errorf("BM25 output %d: want *BFloat16VectorFieldData, got %T", outID, outputs[i])
			}
			data.Data[outID] = fd
		case schemapb.DataType_Float16Vector:
			fd, ok := outputs[i].(*storage.Float16VectorFieldData)
			if !ok {
				return fmt.Errorf("BM25 output %d: want *Float16VectorFieldData, got %T", outID, outputs[i])
			}
			data.Data[outID] = fd
		case schemapb.DataType_BinaryVector:
			fd, ok := outputs[i].(*storage.BinaryVectorFieldData)
			if !ok {
				return fmt.Errorf("BM25 output %d: want *BinaryVectorFieldData, got %T", outID, outputs[i])
			}
			data.Data[outID] = fd
		case schemapb.DataType_SparseFloatVector:
			sparse, ok := outputs[i].(*schemapb.SparseFloatArray)
			if !ok {
				return fmt.Errorf("BM25 output %d: want *SparseFloatArray, got %T", outID, outputs[i])
			}
			data.Data[outID] = &storage.SparseFloatVectorFieldData{
				SparseFloatArray: schemapb.SparseFloatArray{
					Dim: sparse.GetDim(), Contents: sparse.GetContents(),
				},
			}
		default:
			return fmt.Errorf("unsupported BM25 output type %s for field %d",
				outField.GetDataType(), outID)
		}
	}
	return nil
}

func assignMinHashOutput(
	_ *schemapb.CollectionSchema,
	_ *schemapb.FunctionSchema,
	runner function.FunctionRunner,
	outputs []any,
	data *storage.InsertData,
) error {
	if len(outputs) == 0 {
		return errors.New("MinHash runner returned empty output")
	}
	fd, ok := outputs[0].(*schemapb.FieldData)
	if !ok {
		return fmt.Errorf("MinHash output 0: want *FieldData, got %T", outputs[0])
	}
	vec := fd.GetVectors()
	if vec == nil {
		return errors.New("MinHash output is not a vector field")
	}
	binVec := vec.GetBinaryVector()
	if binVec == nil {
		return errors.New("MinHash output is not a binary vector")
	}
	outFields := runner.GetOutputFields()
	if len(outFields) == 0 {
		return errors.New("MinHash runner has no output fields")
	}
	data.Data[outFields[0].GetFieldID()] = &storage.BinaryVectorFieldData{
		Data: binVec, Dim: int(vec.GetDim()),
	}
	return nil
}
