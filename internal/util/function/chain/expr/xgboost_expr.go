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

package expr

import (
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const XGBoostFuncName = "xgboost"

// XGBoostExpr implements FunctionExpr for XGBoost model inference.
// It takes numeric feature columns as input, loads an XGBoost model,
// and outputs prediction scores.
//
// Expected inputs:
//   - inputs[0..N]: Float32/Float64/Int32/Int64 columns (one per feature)
//
// Outputs:
//   - outputs[0]: Float32 prediction column
//
// Parameters:
//   - model_uri: URI to XGBoost model file (file path or cloud URL)
//   - features: list of feature names that match model input features
//   - output_type: "float32" (default) or "int32" for output predictions
type XGBoostExpr struct {
	BaseExpr
	modelURI   string
	features   []string
	outputType arrow.DataType
	// modelMu protects model access (load once)
	modelMu sync.Mutex
}

// NewXGBoostExpr creates a new XGBoostExpr with the given model URI and features.
func NewXGBoostExpr(modelURI string, features []string, outputType arrow.DataType) (*XGBoostExpr, error) {
	if modelURI == "" {
		return nil, merr.WrapErrParameterMissingMsg("xgboost: model_uri must not be empty")
	}
	if len(features) == 0 {
		return nil, merr.WrapErrParameterMissingMsg("xgboost: features must not be empty")
	}
	if outputType == nil {
		outputType = arrow.PrimitiveTypes.Float32
	}

	// Validate output type is numeric
	if !isNumericType(outputType) {
		return nil, merr.WrapErrParameterInvalidMsg("xgboost: output_type must be numeric, got %s", outputType)
	}

	return &XGBoostExpr{
		BaseExpr:   *NewBaseExpr(XGBoostFuncName, []string{types.StageL2Rerank, types.StageL1Rerank}),
		modelURI:   modelURI,
		features:   features,
		outputType: outputType,
	}, nil
}

// NewXGBoostExprFromParams creates XGBoostExpr from function configuration.
func NewXGBoostExprFromParams(ctx types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
	reader := types.NewParamReader(XGBoostFuncName, cfg.Params)

	modelURI, err := reader.String("model_uri", true)
	if err != nil {
		return nil, err
	}

	features, err := reader.StringSlice("features", true)
	if err != nil {
		return nil, err
	}

	outputTypeStr, err := reader.String("output_type", false)
	if err != nil {
		return nil, err
	}

	// Parse output type (default: Float32)
	var outputType arrow.DataType = arrow.PrimitiveTypes.Float32
	if outputTypeStr != "" {
		switch outputTypeStr {
		case "float32":
			outputType = arrow.PrimitiveTypes.Float32
		case "float64":
			outputType = arrow.PrimitiveTypes.Float64
		case "int32":
			outputType = arrow.PrimitiveTypes.Int32
		case "int64":
			outputType = arrow.PrimitiveTypes.Int64
		default:
			return nil, merr.WrapErrParameterInvalidMsg("xgboost: invalid output_type %s (must be float32, float64, int32, or int64)", outputTypeStr)
		}
	}

	return NewXGBoostExpr(modelURI, features, outputType)
}

// OutputDataTypes returns the data types of output columns.
func (x *XGBoostExpr) OutputDataTypes() []arrow.DataType {
	return []arrow.DataType{x.outputType}
}

// Execute performs XGBoost inference on the input feature columns.
func (x *XGBoostExpr) Execute(ctx *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
	if len(inputs) != len(x.features) {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: expected %d input columns (features), got %d", len(x.features), len(inputs))
	}

	// Validate all inputs are numeric
	for i, input := range inputs {
		if !isNumericChunkedType(input.DataType()) {
			return nil, merr.WrapErrParameterInvalidMsg("xgboost: feature column %d (%s) must be numeric, got %s", i, x.features[i], input.DataType())
		}
	}

	numChunks := len(inputs[0].Chunks())

	// Validate all inputs have same chunk structure
	for i, input := range inputs {
		if len(input.Chunks()) != numChunks {
			return nil, merr.WrapErrServiceInternalMsg("xgboost: input column %d has %d chunks but expected %d", i, len(input.Chunks()), numChunks)
		}
	}

	// Process each chunk
	outputChunks := make([]arrow.Array, numChunks)
	for chunkIdx := 0; chunkIdx < numChunks; chunkIdx++ {
		chunks := make([]arrow.Array, len(inputs))
		for i, input := range inputs {
			chunks[i] = input.Chunk(chunkIdx)
		}

		outputChunk, err := x.processChunk(ctx, chunks)
		if err != nil {
			for i := 0; i < chunkIdx; i++ {
				outputChunks[i].Release()
			}
			return nil, err
		}
		outputChunks[chunkIdx] = outputChunk
	}

	result := arrow.NewChunked(x.outputType, outputChunks)
	for _, chunk := range outputChunks {
		chunk.Release()
	}

	return []*arrow.Chunked{result}, nil
}

// processChunk performs inference on a single chunk of feature data.
func (x *XGBoostExpr) processChunk(ctx *types.FuncContext, featureChunks []arrow.Array) (arrow.Array, error) {
	if len(featureChunks) == 0 {
		return nil, merr.WrapErrServiceInternal("xgboost: no feature chunks provided")
	}

	numRows := featureChunks[0].Len()
	if numRows == 0 {
		// Return empty array of output type
		switch x.outputType.ID() {
		case arrow.FLOAT32:
			builder := array.NewFloat32Builder(ctx.Pool())
			defer builder.Release()
			return builder.NewArray(), nil
		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(ctx.Pool())
			defer builder.Release()
			return builder.NewArray(), nil
		case arrow.INT32:
			builder := array.NewInt32Builder(ctx.Pool())
			defer builder.Release()
			return builder.NewArray(), nil
		case arrow.INT64:
			builder := array.NewInt64Builder(ctx.Pool())
			defer builder.Release()
			return builder.NewArray(), nil
		default:
			return nil, merr.WrapErrServiceInternalMsg("xgboost: unsupported output type %s", x.outputType)
		}
	}

	// Extract numeric feature values (treating nulls as 0.0)
	features := make([][]float64, len(featureChunks))
	for i, chunk := range featureChunks {
		colFeatures := make([]float64, numRows)
		for j := 0; j < numRows; j++ {
			if chunk.IsNull(j) {
				colFeatures[j] = 0.0
			} else {
				val, err := GetNumericValue(chunk, j)
				if err != nil {
					return nil, err
				}
				colFeatures[j] = val
			}
		}
		features[i] = colFeatures
	}

	// Transpose: convert column-major to row-major (rows of feature vectors)
	featureVectors := make([][]float64, numRows)
	for i := 0; i < numRows; i++ {
		featureVectors[i] = make([]float64, len(featureChunks))
		for j := 0; j < len(featureChunks); j++ {
			featureVectors[i][j] = features[j][i]
		}
	}

	// Perform predictions (placeholder for actual XGBoost inference)
	// In real implementation, this would call the XGBoost model loaded from modelURI
	predictions, err := x.predictBatch(ctx.Context(), featureVectors)
	if err != nil {
		return nil, err
	}

	if len(predictions) != numRows {
		return nil, merr.WrapErrServiceInternalMsg("xgboost: expected %d predictions, got %d", numRows, len(predictions))
	}

	// Build output array of appropriate type
	switch x.outputType.ID() {
	case arrow.FLOAT32:
		builder := array.NewFloat32Builder(ctx.Pool())
		defer builder.Release()
		for _, pred := range predictions {
			builder.Append(float32(pred))
		}
		return builder.NewArray(), nil

	case arrow.FLOAT64:
		builder := array.NewFloat64Builder(ctx.Pool())
		defer builder.Release()
		for _, pred := range predictions {
			builder.Append(pred)
		}
		return builder.NewArray(), nil

	case arrow.INT32:
		builder := array.NewInt32Builder(ctx.Pool())
		defer builder.Release()
		for _, pred := range predictions {
			builder.Append(int32(pred))
		}
		return builder.NewArray(), nil

	case arrow.INT64:
		builder := array.NewInt64Builder(ctx.Pool())
		defer builder.Release()
		for _, pred := range predictions {
			builder.Append(int64(pred))
		}
		return builder.NewArray(), nil

	default:
		return nil, merr.WrapErrServiceInternalMsg("xgboost: unsupported output type %s", x.outputType)
	}
}

// predictBatch performs batch inference on feature vectors.
// This is a placeholder that returns dummy predictions.
// In a real implementation, this would call the XGBoost model loaded from modelURI.
func (x *XGBoostExpr) predictBatch(_ interface{}, featureVectors [][]float64) ([]float64, error) {
	// TODO: Implement actual XGBoost model loading and inference
	// For now, return dummy predictions (sum of features)
	predictions := make([]float64, len(featureVectors))
	for i, features := range featureVectors {
		sum := 0.0
		for _, f := range features {
			sum += f
		}
		predictions[i] = sum
	}
	return predictions, nil
}

// Helper functions

// isNumericType checks if an Arrow data type is numeric.
func isNumericType(dt arrow.DataType) bool {
	if dt == nil {
		return false
	}
	id := dt.ID()
	return id == arrow.INT8 || id == arrow.INT16 || id == arrow.INT32 || id == arrow.INT64 ||
		id == arrow.UINT8 || id == arrow.UINT16 || id == arrow.UINT32 || id == arrow.UINT64 ||
		id == arrow.FLOAT32 || id == arrow.FLOAT64
}

// isNumericChunkedType checks if a chunked Arrow data type is numeric.
func isNumericChunkedType(dt arrow.DataType) bool {
	return isNumericType(dt)
}

func init() {
	types.MustRegisterFunction(XGBoostFuncName, NewXGBoostExprFromParams)
}
