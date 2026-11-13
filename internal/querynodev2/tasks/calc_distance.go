// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/distance"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Block-count knobs for the bounded parallel scoring. The fused kernels decode
// a row inline (or into a reused dim-sized rowBuf on the SIMD path), so a
// block's transient working set is just its output distances array — memory is
// no longer the sizing constraint. Blocks are instead sized to a small multiple
// of the CPU count so the dynamic worker pool has a couple of blocks per worker
// to rebalance against.
const (
	// minDenseBlockRows floors the dense block size; below it the per-block
	// overhead (distances alloc, SIMD rowBuf, worker handoff) dominates.
	minDenseBlockRows = 256
	// minSparseBlockRows floors the sparse block size. Sparse rows have
	// variable per-row cost (nnz), so more/smaller blocks leave more room for
	// the pool to rebalance stragglers.
	minSparseBlockRows = 256
)

// blockRowLimit returns the maximum rows per block so the reduced result is
// split into about 2×GOMAXPROCS blocks (dense) or 4×GOMAXPROCS blocks (sparse).
// More cores → more, smaller blocks; the 256-row floors keep per-block overhead
// negligible on low-core machines and small result sets. totalRows is the
// reduced result's row count (≤ limit + offset per QN, 16384 without large
// topK).
func blockRowLimit(totalRows int, sparse bool) int {
	targetBlocks := 2 * runtime.GOMAXPROCS(0)
	minRows := minDenseBlockRows
	if sparse {
		targetBlocks *= 2
		minRows = minSparseBlockRows
	}
	rows := (totalRows + targetBlocks - 1) / targetBlocks
	if rows < minRows {
		rows = minRows
	}
	return rows
}

// applyCalcDistanceToResult computes and appends the per-row distance column to
// the reduced retrieve result when the plan requests calc_distance. It is a
// no-op otherwise. The reference vector, metric and target field are read from
// the plan (written by the proxy); the user's output field ids decide whether
// the target vector column is kept or dropped after scoring.
func applyCalcDistanceToResult(result *segcorepb.RetrieveResults, planQuery *planpb.QueryPlanNode, schema *schemapb.CollectionSchema, userOutputFieldIDs []int64) error {
	cd := planQuery.GetCalcDistance()
	if cd == nil || !cd.GetReturnDistance() || typeutil.GetSizeOfIDs(result.GetIds()) == 0 {
		return nil
	}
	if len(cd.GetRefVectors()) != 1 {
		return merr.WrapErrServiceInternalMsg("calc_distance: expected exactly one reference vector, got %d", len(cd.GetRefVectors()))
	}
	vectorField := typeutil.GetFieldByID(schema, cd.GetVectorFieldId())
	if vectorField == nil {
		return merr.WrapErrServiceInternalMsg("calc_distance: vector field %d not found in schema", cd.GetVectorFieldId())
	}
	// fieldDim is only needed for fixed-dimension dense types; sparse vectors
	// carry their own dimension per row.
	var fieldDim int64
	var err error
	if cd.GetRefVectorType() != schemapb.DataType_SparseFloatVector {
		fieldDim, err = typeutil.GetDim(vectorField)
		if err != nil {
			return err
		}
	}
	return calcDistanceColumn(result,
		cd.GetRefVectors()[0],
		cd.GetRefVectorType(),
		cd.GetMetricType(),
		cd.GetVectorFieldId(),
		cd.GetDistanceOutputName(),
		userOutputFieldIDs,
		fieldDim,
	)
}

// calcDistanceColumn computes per-row distance between the reference vector and
// the target vector field of a reduced retrieve result, appends it as a
// synthetic float32 column, and drops the target vector column when the user
// did not request it.
//
// refVector is the canonical byte encoding for refVectorType (see the proxy's
// resolveCalcDistance): float32 LE bytes for FloatVector, half-precision bytes
// for Float16/BFloat16, dim bytes for Int8Vector, packed bits for BinaryVector,
// and a compact sparse row for SparseFloatVector. fieldDim must be the
// schema-fixed dimension for dense types and 0 for sparse.
func calcDistanceColumn(result *segcorepb.RetrieveResults, refVector []byte, refVectorType schemapb.DataType, metricType string, vectorFieldID int64, distanceOutputName string, userOutputFieldIDs []int64, fieldDim int64) error {
	// locate the target vector field data
	vectorFieldIdx := -1
	var vectorData *schemapb.VectorField
	var validData []bool
	for i, fd := range result.GetFieldsData() {
		if fd.GetFieldId() == vectorFieldID {
			vectorFieldIdx = i
			vectorData = fd.GetVectors()
			validData = typeutil.GetFieldDataValidData(fd)
			break
		}
	}
	if vectorFieldIdx == -1 {
		return merr.WrapErrServiceInternalMsg("calc_distance: vector field %d not found in retrieve result", vectorFieldID)
	}

	distances, err := calcDistances(refVector, refVectorType, metricType, vectorData, fieldDim, vectorFieldID)
	if err != nil {
		return err
	}
	// Nullable vector fields arrive with compact data (valid rows only) and a
	// ValidData bitmap. Score only the valid rows and expand to one entry per
	// row so the distance column stays row-aligned with the ids. Nullness is
	// carried on the synthetic column's ValidData (null rows get a zero
	// placeholder value), never as NaN — NaN cannot be represented in JSON,
	// which would break the RESTful output.
	if len(validData) > 0 {
		validCount := 0
		for _, v := range validData {
			if v {
				validCount++
			}
		}
		// guard against a future non-compact payload or a ValidData/payload
		// mismatch, which would otherwise silently misalign or panic
		if len(distances) != validCount {
			return merr.WrapErrServiceInternalMsg("calc_distance: nullable vector field distance count %d does not match valid row count %d", len(distances), validCount)
		}
		full := make([]float32, len(validData))
		vi := 0
		for i, v := range validData {
			if v {
				full[i] = distances[vi]
				vi++
			}
		}
		distances = full
	}

	// append synthetic distance column at the end
	distanceColumn := &schemapb.FieldData{
		Type:      schemapb.DataType_Float,
		FieldName: distanceOutputName,
		FieldId:   common.CalcDistanceFieldID,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_FloatData{
					FloatData: &schemapb.FloatArray{Data: distances},
				},
			},
		},
	}
	// mirror the target field's nullness onto the synthetic column so SDKs and
	// the RESTful layer treat null rows as null rather than as NaN values
	if len(validData) > 0 {
		typeutil.SetFieldDataValidData(distanceColumn, validData)
	}
	result.FieldsData = append(result.FieldsData, distanceColumn)

	// drop the vector column unless the user requested it
	userRequested := false
	for _, id := range userOutputFieldIDs {
		if id == vectorFieldID {
			userRequested = true
			break
		}
	}
	if !userRequested {
		result.FieldsData = append(result.FieldsData[:vectorFieldIdx], result.FieldsData[vectorFieldIdx+1:]...)
	}
	return nil
}

// calcDistances dispatches the per-row distance computation on the target
// vector field's data type. The reference vector is always a single vector;
// the result is one distance per row of vectorData. Rows are split into
// parallel blocks (estimateDistanceNumBlocks) so a large result set does not
// serialize on one core; the reference vector stays the same across blocks.
// vectorFieldID names the field in length-mismatch errors.
func calcDistances(refVector []byte, refVectorType schemapb.DataType, metricType string, vectorData *schemapb.VectorField, fieldDim int64, vectorFieldID int64) ([]float32, error) {
	// calcBlock computes distances for rows [start, end) of the field data.
	var calcBlock func(start, end int) ([]float32, error)
	var totalRows int

	switch refVectorType {
	case schemapb.DataType_FloatVector:
		rowData := vectorData.GetFloatVector().GetData()
		if int64(len(rowData))%fieldDim != 0 {
			return nil, merr.WrapErrServiceInternalMsg("calc_distance: float vector field %d data length %d is not a multiple of dim %d", vectorFieldID, len(rowData), fieldDim)
		}
		totalRows = len(rowData) / int(fieldDim)
		refVec := typeutil.BytesToFloat32Vector(refVector)
		calcBlock = func(start, end int) ([]float32, error) {
			return distance.CalcFloatDistance(fieldDim, refVec, rowData[start*int(fieldDim):end*int(fieldDim)], metricType)
		}
	case schemapb.DataType_Int8Vector:
		data := vectorData.GetInt8Vector()
		if int64(len(data))%fieldDim != 0 {
			return nil, merr.WrapErrServiceInternalMsg("calc_distance: int8 vector field %d data length %d is not a multiple of dim %d", vectorFieldID, len(data), fieldDim)
		}
		totalRows = len(data) / int(fieldDim)
		refVec := int8BytesToFloat32Vector(refVector)
		calcBlock = func(start, end int) ([]float32, error) {
			return distance.CalcInt8FloatDistance(fieldDim, refVec, data[start*int(fieldDim):end*int(fieldDim)], metricType)
		}
	case schemapb.DataType_Float16Vector:
		data := vectorData.GetFloat16Vector()
		if int64(len(data))%(fieldDim*2) != 0 {
			return nil, merr.WrapErrServiceInternalMsg("calc_distance: float16 vector field %d data length %d is not a multiple of %d", vectorFieldID, len(data), fieldDim*2)
		}
		totalRows = len(data) / int(fieldDim*2)
		refVec := typeutil.Float16BytesToFloat32Vector(refVector)
		calcBlock = func(start, end int) ([]float32, error) {
			return distance.CalcFloat16FloatDistance(fieldDim, refVec, data[start*int(fieldDim*2):end*int(fieldDim*2)], metricType)
		}
	case schemapb.DataType_BFloat16Vector:
		data := vectorData.GetBfloat16Vector()
		if int64(len(data))%(fieldDim*2) != 0 {
			return nil, merr.WrapErrServiceInternalMsg("calc_distance: bfloat16 vector field %d data length %d is not a multiple of %d", vectorFieldID, len(data), fieldDim*2)
		}
		totalRows = len(data) / int(fieldDim*2)
		refVec := typeutil.BFloat16BytesToFloat32Vector(refVector)
		calcBlock = func(start, end int) ([]float32, error) {
			return distance.CalcBFloat16FloatDistance(fieldDim, refVec, data[start*int(fieldDim*2):end*int(fieldDim*2)], metricType)
		}
	case schemapb.DataType_BinaryVector:
		bytesPerRow := int(fieldDim / 8)
		data := vectorData.GetBinaryVector()
		if len(data)%bytesPerRow != 0 {
			return nil, merr.WrapErrServiceInternalMsg("calc_distance: binary vector field %d data length %d is not a multiple of %d", vectorFieldID, len(data), bytesPerRow)
		}
		totalRows = len(data) / bytesPerRow
		calcBlock = func(start, end int) ([]float32, error) {
			return distance.CalcBinaryDistance(fieldDim, refVector, data[start*bytesPerRow:end*bytesPerRow], metricType)
		}
	case schemapb.DataType_SparseFloatVector:
		contents := vectorData.GetSparseFloatVector().GetContents()
		totalRows = len(contents)
		calcBlock = func(start, end int) ([]float32, error) {
			return distance.CalcSparseDistance(refVector, contents[start:end], metricType)
		}
	default:
		return nil, merr.WrapErrServiceInternalMsg("calc_distance: unsupported vector field type %s", refVectorType.String())
	}

	if totalRows == 0 {
		return []float32{}, nil
	}
	return runCalcDistanceBlocks(totalRows, blockRowLimit(totalRows, refVectorType == schemapb.DataType_SparseFloatVector), calcBlock)
}

// runCalcDistanceBlocks scores [0, totalRows) with a bounded worker pool. The
// rows are cut into fixed-size blocks of at most blockRowLimit rows; a worker
// takes the next block, runs calcBlock (which decodes and scores that block in
// this goroutine) and writes its result range. The worker count never exceeds
// GOMAXPROCS; surplus blocks queue and are consumed in order, so the pool stays
// busy without spawning one goroutine per block. Transient working set is at
// most GOMAXPROCS block outputs (blockRowLimit floats each) plus one reused
// dim-sized SIMD decode buffer per worker.
func runCalcDistanceBlocks(totalRows, blockRowLimit int, calcBlock func(start, end int) ([]float32, error)) ([]float32, error) {
	distances := make([]float32, totalRows)
	numBlocks := (totalRows + blockRowLimit - 1) / blockRowLimit
	workers := min(runtime.GOMAXPROCS(0), numBlocks)
	if workers <= 1 {
		d, err := calcBlock(0, totalRows)
		if err != nil {
			return nil, err
		}
		copy(distances, d)
		return distances, nil
	}
	var (
		next     atomic.Int64
		wg       sync.WaitGroup
		mu       sync.Mutex
		firstErr error
	)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				block := int(next.Add(1)) - 1
				start := block * blockRowLimit
				if start >= totalRows {
					return
				}
				end := min(start+blockRowLimit, totalRows)
				d, err := calcBlock(start, end)
				if err != nil {
					mu.Lock()
					if firstErr == nil {
						firstErr = err
					}
					mu.Unlock()
					return
				}
				copy(distances[start:end], d)
			}
		}()
	}
	wg.Wait()
	if firstErr != nil {
		return nil, firstErr
	}
	return distances, nil
}

// int8BytesToFloat32Vector decodes a signed int8 byte vector into float32.
func int8BytesToFloat32Vector(b []byte) []float32 {
	vec := make([]float32, len(b))
	for i, v := range b {
		vec[i] = float32(int8(v))
	}
	return vec
}
