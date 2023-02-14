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

package indexparamcheck

import (
	"fmt"
	"strconv"

	"github.com/milvus-io/milvus-proto/go-api/schemapb"
	"github.com/milvus-io/milvus/internal/util/funcutil"
)

type ConfAdapter interface {
	// CheckTrain returns true if the index can be built with the specific index parameters.
	CheckTrain(map[string]string) error
	CheckValidDataType(dType schemapb.DataType) bool
	CheckUnnecessaryParams(map[string]string) error
}

// BaseConfAdapter checks if a `FLAT` index can be built.
type BaseConfAdapter struct {
}

// CheckTrain check whether the params contains supported metrics types
func (adapter *BaseConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return fmt.Errorf("invild param with: %s", DIM)
	}

	return CheckStrByValues(params, Metric, METRICS)
}

// CheckValidDataType check whether the field data type is supported for the index type
func (adapter *BaseConfAdapter) CheckValidDataType(dType schemapb.DataType) bool {
	return true
}

func (adapter *BaseConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(FLATParams, params)
}

func newBaseConfAdapter() *BaseConfAdapter {
	return &BaseConfAdapter{}
}

// IVFConfAdapter checks if a IVF index can be built.
type IVFConfAdapter struct {
	BaseConfAdapter
}

// CheckTrain returns true if the index can be built with the specific index parameters.
func (adapter *IVFConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return fmt.Errorf("invild param with: %s", NLIST)
	}

	// skip check number of rows

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func (adapter *IVFConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(IVFFLATParams, params)
}

func newIVFConfAdapter() *IVFConfAdapter {
	return &IVFConfAdapter{}
}

// IVFPQConfAdapter checks if a IVF_PQ index can be built.
type IVFPQConfAdapter struct {
	IVFConfAdapter
}

// CheckTrain checks if ivf-pq index can be built with the specific index parameters.
func (adapter *IVFPQConfAdapter) CheckTrain(params map[string]string) error {
	if err := adapter.IVFConfAdapter.CheckTrain(params); err != nil {
		return err
	}

	return adapter.checkPQParams(params)
}

func (adapter *IVFPQConfAdapter) checkPQParams(params map[string]string) error {
	dimStr, dimensionExist := params[DIM]
	if !dimensionExist {
		return fmt.Errorf("lack necessary parameter: %s", DIM)
	}

	dimension, err := strconv.Atoi(dimStr)
	if err != nil { // invalid dimension
		return fmt.Errorf("invalid param with: %s", DIM)
	}

	// nbits can be set to default: 8
	nbitsStr, nbitsExist := params[NBITS]
	var nbits int
	if !nbitsExist {
		nbits = 8
	} else {
		nbits, err = strconv.Atoi(nbitsStr)
		if err != nil { // invalid nbits
			return fmt.Errorf("invalid param with: %s", NBITS)
		}
	}

	mStr, ok := params[IVFM]
	if !ok {
		return fmt.Errorf("lack necessary parameter: %s", IVFM)
	}
	m, err := strconv.Atoi(mStr)
	if err != nil || m == 0 { // invalid m
		return fmt.Errorf("invalid param with: %s", IVFM)
	}

	mode, ok := params[IndexMode]
	if !ok {
		mode = CPUMode
	}

	if mode == GPUMode && !adapter.checkGPUPQParams(dimension, m, nbits) {
		return fmt.Errorf("invalid param in %s mode", mode)
	}

	if !adapter.checkCPUPQParams(dimension, m) {
		return fmt.Errorf("invalid param in %s mode", mode)
	}

	return nil
}

func (adapter *IVFPQConfAdapter) checkGPUPQParams(dimension, m, nbits int) bool {
	/*
	 * Faiss 1.6
	 * Only 1, 2, 3, 4, 6, 8, 10, 12, 16, 20, 24, 28, 32 dims per sub-quantizer are currently supported with
	 * no precomputed codes. Precomputed codes supports any number of dimensions, but will involve memory overheads.
	 */

	subDim := dimension / m
	return funcutil.SliceContain(supportSubQuantizer, m) && funcutil.SliceContain(supportDimPerSubQuantizer, subDim) && nbits == 8
}

func (adapter *IVFPQConfAdapter) checkCPUPQParams(dimension, m int) bool {
	return (dimension % m) == 0
}

func (adapter *IVFPQConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(IVFPQParams, params)
}

func newIVFPQConfAdapter() *IVFPQConfAdapter {
	return &IVFPQConfAdapter{}
}

// IVFSQConfAdapter checks if a IVF_SQ index can be built.
type IVFSQConfAdapter struct {
	IVFConfAdapter
}

func (adapter *IVFSQConfAdapter) checkNBits(params map[string]string) bool {
	// cgo will set this key to DefaultNBits (8), which is the only value Milvus supports.
	_, exist := params[NBITS]
	if exist {
		// 8 is the only supported nbits.
		return CheckIntByRange(params, NBITS, DefaultNBits, DefaultNBits)
	}
	return true
}

// CheckTrain returns true if the index can be built with the specific index parameters.
func (adapter *IVFSQConfAdapter) CheckTrain(params map[string]string) error {
	if !adapter.checkNBits(params) {
		return fmt.Errorf("invild param with: %s", NBITS)
	}
	return adapter.IVFConfAdapter.CheckTrain(params)
}

func (adapter *IVFSQConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(IVFSQ8Params, params)
}

func newIVFSQConfAdapter() *IVFSQConfAdapter {
	return &IVFSQConfAdapter{}
}

type BinIDMAPConfAdapter struct {
	BaseConfAdapter
}

// CheckTrain checks if a binary flat index can be built with the specific parameters.
func (adapter *BinIDMAPConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return fmt.Errorf("invild param with: %s", DIM)
	}

	return CheckStrByValues(params, Metric, BinIDMapMetrics)
}

func (adapter *BinIDMAPConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(BINFLATParams, params)
}

func newBinIDMAPConfAdapter() *BinIDMAPConfAdapter {
	return &BinIDMAPConfAdapter{}
}

// BinIVFConfAdapter checks if a bin IFV index can be built.
type BinIVFConfAdapter struct {
	BaseConfAdapter
}

// CheckTrain checks if a binary ivf index can be built with specific parameters.
func (adapter *BinIVFConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return fmt.Errorf("invild param with: %s", DIM)
	}

	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return fmt.Errorf("invild param with: %s", DIM)
	}

	// skip checking the number of rows

	return CheckStrByValues(params, Metric, BinIvfMetrics)
}

func (adapter *BinIVFConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(BINIVFFLATParams, params)
}

func newBinIVFConfAdapter() *BinIVFConfAdapter {
	return &BinIVFConfAdapter{}
}

type NSGConfAdapter struct {
	BaseConfAdapter
}

// CheckTrain checks if a nsg index can be built with specific parameters.
func (adapter *NSGConfAdapter) CheckTrain(params map[string]string) error {
	if err := CheckStrByValues(params, Metric, METRICS); err != nil {
		return err
	}

	if !CheckIntByRange(params, KNNG, MinKNNG, MaxKNNG) {
		return fmt.Errorf("invild param with: %s", KNNG)
	}

	if !CheckIntByRange(params, SearchLength, MinSearchLength, MaxSearchLength) {
		return fmt.Errorf("invild param with: %s", SearchLength)
	}

	if !CheckIntByRange(params, OutDegree, MinOutDegree, MaxOutDegree) {
		return fmt.Errorf("invild param with: %s", OutDegree)
	}

	if !CheckIntByRange(params, CANDIDATE, MinCandidatePoolSize, MaxCandidatePoolSize) {
		return fmt.Errorf("invild param with: %s", CANDIDATE)
	}

	// skip checking the number of rows

	return nil
}

func (adapter *NSGConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(NSGParams, params)
}

func newNSGConfAdapter() *NSGConfAdapter {
	return &NSGConfAdapter{}
}

// HNSWConfAdapter checks if a hnsw index can be built.
type HNSWConfAdapter struct {
	BaseConfAdapter
}

// CheckTrain checks if a hnsw index can be built with specific parameters.
func (adapter *HNSWConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return fmt.Errorf("invild param with: %s", EFConstruction)
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return fmt.Errorf("invild param with: %s", HNSWM)
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func (adapter *HNSWConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(HNSWParams, params)
}

func newHNSWConfAdapter() *HNSWConfAdapter {
	return &HNSWConfAdapter{}
}

// ANNOYConfAdapter checks if an ANNOY index can be built.
type ANNOYConfAdapter struct {
	BaseConfAdapter
}

// CheckTrain checks if an annoy index can be built with specific parameters.
func (adapter *ANNOYConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, NTREES, MinNTrees, MaxNTrees) {
		return fmt.Errorf("invild param with: %s", NTREES)
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func (adapter *ANNOYConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(ANNOYParams, params)
}

func newANNOYConfAdapter() *ANNOYConfAdapter {
	return &ANNOYConfAdapter{}
}

// RHNSWFlatConfAdapter checks if a rhnsw flat index can be built.
type RHNSWFlatConfAdapter struct {
	HNSWConfAdapter
}

// CheckTrain checks if a rhnsw flat index can be built with specific parameters.
func (adapter *RHNSWFlatConfAdapter) CheckTrain(params map[string]string) error {
	return adapter.HNSWConfAdapter.CheckTrain(params)
}

func (adapter *RHNSWFlatConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(HNSWParams, params)
}

func newRHNSWFlatConfAdapter() *RHNSWFlatConfAdapter {
	return &RHNSWFlatConfAdapter{}
}

// RHNSWPQConfAdapter checks if a rhnsw pq index can be built.
type RHNSWPQConfAdapter struct {
	IVFPQConfAdapter
	HNSWConfAdapter
}

// CheckTrain checks if a rhnsw pq index can be built with specific parameters.
func (adapter *RHNSWPQConfAdapter) CheckTrain(params map[string]string) error {
	if err := adapter.HNSWConfAdapter.CheckTrain(params); err != nil {
		return err
	}

	dimension, _ := strconv.Atoi(params[DIM])
	pqmStr, ok := params[PQM]
	if !ok {
		return fmt.Errorf("lack necessary param: %s", PQM)
	}
	pqm, err := strconv.Atoi(pqmStr)
	if err != nil || pqm == 0 {
		return fmt.Errorf("invild param with: %s", PQM)
	}

	if !adapter.IVFPQConfAdapter.checkCPUPQParams(dimension, pqm) {
		return fmt.Errorf("invild param with: %s", PQM)
	}

	return nil
}

func (adapter *RHNSWPQConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(RHNSWPQParams, params)
}

func newRHNSWPQConfAdapter() *RHNSWPQConfAdapter {
	return &RHNSWPQConfAdapter{}
}

// RHNSWSQConfAdapter checks if a rhnsw sq index can be built.
type RHNSWSQConfAdapter struct {
	BaseConfAdapter
	HNSWConfAdapter
}

// CheckTrain checks if a rhnsw sq index can be built with specific parameters.
func (adapter *RHNSWSQConfAdapter) CheckTrain(params map[string]string) error {
	return adapter.HNSWConfAdapter.CheckTrain(params)
}

func (adapter *RHNSWSQConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(HNSWParams, params)
}

func newRHNSWSQConfAdapter() *RHNSWSQConfAdapter {
	return &RHNSWSQConfAdapter{}
}

// NGTPANNGConfAdapter checks if a NGT_PANNG index can be built.
type NGTPANNGConfAdapter struct {
	BaseConfAdapter
}

func (adapter *NGTPANNGConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return fmt.Errorf("invild param with: %s", EdgeSize)
	}

	if !CheckIntByRange(params, ForcedlyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return fmt.Errorf("invild param with: %s", ForcedlyPrunedEdgeSize)
	}

	if !CheckIntByRange(params, SelectivelyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return fmt.Errorf("invild param with: %s", SelectivelyPrunedEdgeSize)
	}

	selectivelyPrunedEdgeSize, _ := strconv.Atoi(params[SelectivelyPrunedEdgeSize])
	forcedlyPrunedEdgeSize, _ := strconv.Atoi(params[ForcedlyPrunedEdgeSize])
	if selectivelyPrunedEdgeSize >= forcedlyPrunedEdgeSize {
		return fmt.Errorf("invild param with: %s and %s", ForcedlyPrunedEdgeSize, ForcedlyPrunedEdgeSize)
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func (adapter *NGTPANNGConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(NGTPANNGParams, params)
}

func newNGTPANNGConfAdapter() *NGTPANNGConfAdapter {
	return &NGTPANNGConfAdapter{}
}

// NGTONNGConfAdapter checks if a NGT_ONNG index can be built.
type NGTONNGConfAdapter struct {
	BaseConfAdapter
}

func (adapter *NGTONNGConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return fmt.Errorf("invild param with: %s", EdgeSize)
	}

	if !CheckIntByRange(params, OutgoingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return fmt.Errorf("invild param with: %s", OutgoingEdgeSize)
	}

	if !CheckIntByRange(params, IncomingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return fmt.Errorf("invild param with: %s", IncomingEdgeSize)
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func (adapter *NGTONNGConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(NGTONNGParams, params)
}

func newNGTONNGConfAdapter() *NGTONNGConfAdapter {
	return &NGTONNGConfAdapter{}
}

type DISKANNConfAdapter struct {
	BaseConfAdapter
}

func (adapter *DISKANNConfAdapter) CheckTrain(params map[string]string) error {
	if !CheckIntByRange(params, DIM, DiskAnnMinDim, DiskAnnMaxDim) {
		return fmt.Errorf("invild param with: %s", DIM)
	}
	return nil
}

func (adapter *DISKANNConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(DISKANNParams, params)
}

// CheckValidDataType check whether the field data type is supported for the index type
func (adapter *DISKANNConfAdapter) CheckValidDataType(dType schemapb.DataType) bool {
	vecDataTypes := []schemapb.DataType{
		schemapb.DataType_FloatVector,
	}
	return funcutil.SliceContain(vecDataTypes, dType)
}

func newDISKANNConfAdapter() *DISKANNConfAdapter {
	return &DISKANNConfAdapter{}
}

type ScalarConfAdapter struct {
	BaseConfAdapter
}

func (adapter *ScalarConfAdapter) CheckTrain(params map[string]string) error {
	return nil
}

func (adapter *ScalarConfAdapter) CheckUnnecessaryParams(params map[string]string) error {
	return checkUnnecessaryParams(ScalarIndexParams, params)
}

func newScalarConfAdapter() *ScalarConfAdapter {
	return &ScalarConfAdapter{}
}
