package proxynode

import (
	"strconv"

	"github.com/zilliztech/milvus-distributed/internal/util/funcutil"
)

const (
	L2             = "L2"
	IP             = "IP"
	HAMMING        = "HAMMING"
	JACCARD        = "JACCARD"
	TANIMOTO       = "TANIMOTO"
	SUBSTRUCTURE   = "SUBSTRUCTURE"
	SUPERSTRUCTURE = "SUPERSTRUCTURE"

	MinNBits     = 1
	MaxNBits     = 16
	DefaultNBits = 8

	MinNList = 1
	MaxNList = 65536

	DefaultMinDim = 1
	DefaultMaxDim = 32768

	NgtMinEdgeSize = 1
	NgtMaxEdgeSize = 200

	HNSWMinEfConstruction = 8
	HNSWMaxEfConstruction = 512
	HNSWMinM              = 4
	HNSWMaxM              = 64

	MinKNNG              = 5
	MaxKNNG              = 300
	MinSearchLength      = 10
	MaxSearchLength      = 300
	MinOutDegree         = 5
	MaxOutDegree         = 300
	MinCandidatePoolSize = 50
	MaxCandidatePoolSize = 1000

	MinNTrees = 1
	// too large of n_trees takes much time, if there is real requirement, change this threshold.
	MaxNTrees = 1024

	DIM    = "dim"
	Metric = "metric_type"
	NLIST  = "nlist"
	NBITS  = "nbits"
	IVFM   = "m"

	KNNG         = "knng"
	SearchLength = "search_length"
	OutDegree    = "out_degree"
	CANDIDATE    = "candidate_pool_size"

	EFConstruction = "efConstruction"
	HNSWM          = "M"

	PQM    = "PQM"
	NTREES = "n_trees"

	EdgeSize                  = "edge_size"
	ForcedlyPrunedEdgeSize    = "forcedly_pruned_edge_size"
	SelectivelyPrunedEdgeSize = "selectively_pruned_edge_size"

	OutgoingEdgeSize = "outgoing_edge_size"
	IncomingEdgeSize = "incoming_edge_size"

	IndexMode = "index_mode"
	CPUMode   = "CPU"
	GPUMode   = "GPU"
)

var METRICS = []string{L2, IP}                                                             // const
var BinIDMapMetrics = []string{HAMMING, JACCARD, TANIMOTO, SUBSTRUCTURE, SUBSTRUCTURE}     // const
var BinIvfMetrics = []string{HAMMING, JACCARD, TANIMOTO}                                   // const
var supportDimPerSubQuantizer = []int{32, 28, 24, 20, 16, 12, 10, 8, 6, 4, 3, 2, 1}        // const
var supportSubQuantizer = []int{96, 64, 56, 48, 40, 32, 28, 24, 20, 16, 12, 8, 4, 3, 2, 1} // const

type ConfAdapter interface {
	CheckTrain(map[string]string) bool
}

func CheckIntByRange(params map[string]string, key string, min, max int) bool {
	valueStr, ok := params[key]
	if !ok {
		return false
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		return false
	}

	return value >= min && value <= max
}

func CheckStrByValues(params map[string]string, key string, container []string) bool {
	value, ok := params[key]
	if !ok {
		return false
	}

	return funcutil.SliceContain(container, value)
}

type BaseConfAdapter struct {
}

func (adapter *BaseConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return false
	}

	return CheckStrByValues(params, Metric, METRICS)
}

func newBaseConfAdapter() *BaseConfAdapter {
	return &BaseConfAdapter{}
}

type IVFConfAdapter struct {
	BaseConfAdapter
}

func (adapter *IVFConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return false
	}

	// skip check number of rows

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newIVFConfAdapter() *IVFConfAdapter {
	return &IVFConfAdapter{}
}

type IVFPQConfAdapter struct {
	IVFConfAdapter
}

func (adapter *IVFPQConfAdapter) CheckTrain(params map[string]string) bool {
	if !adapter.IVFConfAdapter.CheckTrain(params) {
		return false
	}

	if !CheckIntByRange(params, NBITS, MinNBits, MaxNBits) {
		return false
	}

	return adapter.checkPQParams(params)
}

func (adapter *IVFPQConfAdapter) checkPQParams(params map[string]string) bool {
	dimension, _ := strconv.Atoi(params[DIM])
	nbits, _ := strconv.Atoi(params[NBITS])
	mStr, ok := params[IVFM]
	if !ok {
		return false
	}
	m, err := strconv.Atoi(mStr)
	if err != nil {
		return false
	}

	mode, ok := params[IndexMode]
	if !ok {
		mode = CPUMode
	}

	if mode == GPUMode && !adapter.checkGPUPQParams(dimension, m, nbits) {
		return false
	}

	return adapter.checkCPUPQParams(dimension, m)
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

func newIVFPQConfAdapter() *IVFPQConfAdapter {
	return &IVFPQConfAdapter{}
}

type IVFSQConfAdapter struct {
	IVFConfAdapter
}

func (adapter *IVFSQConfAdapter) CheckTrain(params map[string]string) bool {
	params[NBITS] = strconv.Itoa(DefaultNBits)
	return adapter.IVFConfAdapter.CheckTrain(params)
}

func newIVFSQConfAdapter() *IVFSQConfAdapter {
	return &IVFSQConfAdapter{}
}

type BinIDMAPConfAdapter struct {
}

func (adapter *BinIDMAPConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return false
	}

	return CheckStrByValues(params, Metric, BinIDMapMetrics)
}

func newBinIDMAPConfAdapter() *BinIDMAPConfAdapter {
	return &BinIDMAPConfAdapter{}
}

type BinIVFConfAdapter struct {
}

func (adapter *BinIVFConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, DIM, DefaultMinDim, DefaultMaxDim) {
		return false
	}

	if !CheckIntByRange(params, NLIST, MinNList, MaxNList) {
		return false
	}

	if !CheckStrByValues(params, Metric, BinIvfMetrics) {
		return false
	}

	// skip checking the number of rows

	return true
}

func newBinIVFConfAdapter() *BinIVFConfAdapter {
	return &BinIVFConfAdapter{}
}

type NSGConfAdapter struct {
}

func (adapter *NSGConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckStrByValues(params, Metric, METRICS) {
		return false
	}

	if !CheckIntByRange(params, KNNG, MinKNNG, MaxKNNG) {
		return false
	}

	if !CheckIntByRange(params, SearchLength, MinSearchLength, MaxSearchLength) {
		return false
	}

	if !CheckIntByRange(params, OutDegree, MinOutDegree, MaxOutDegree) {
		return false
	}

	if !CheckIntByRange(params, CANDIDATE, MinCandidatePoolSize, MaxCandidatePoolSize) {
		return false
	}

	// skip checking the number of rows

	return true
}

func newNSGConfAdapter() *NSGConfAdapter {
	return &NSGConfAdapter{}
}

type HNSWConfAdapter struct {
	BaseConfAdapter
}

func (adapter *HNSWConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return false
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return false
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newHNSWConfAdapter() *HNSWConfAdapter {
	return &HNSWConfAdapter{}
}

type ANNOYConfAdapter struct {
	BaseConfAdapter
}

func (adapter *ANNOYConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, NTREES, MinNTrees, MaxNTrees) {
		return false
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newANNOYConfAdapter() *ANNOYConfAdapter {
	return &ANNOYConfAdapter{}
}

type RHNSWFlatConfAdapter struct {
	BaseConfAdapter
}

func (adapter *RHNSWFlatConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return false
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return false
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newRHNSWFlatConfAdapter() *RHNSWFlatConfAdapter {
	return &RHNSWFlatConfAdapter{}
}

type RHNSWPQConfAdapter struct {
	BaseConfAdapter
	IVFPQConfAdapter
}

func (adapter *RHNSWPQConfAdapter) CheckTrain(params map[string]string) bool {
	if !adapter.BaseConfAdapter.CheckTrain(params) {
		return false
	}

	if !CheckIntByRange(params, EFConstruction, HNSWMinM, HNSWMaxM) {
		return false
	}

	dimension, _ := strconv.Atoi(params[DIM])
	pqmStr, ok := params[PQM]
	if !ok {
		return false
	}
	pqm, err := strconv.Atoi(pqmStr)
	if err != nil {
		return false
	}

	return adapter.IVFPQConfAdapter.checkCPUPQParams(dimension, pqm)
}

func newRHNSWPQConfAdapter() *RHNSWPQConfAdapter {
	return &RHNSWPQConfAdapter{}
}

type RHNSWSQConfAdapter struct {
	BaseConfAdapter
}

func (adapter *RHNSWSQConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, EFConstruction, HNSWMinEfConstruction, HNSWMaxEfConstruction) {
		return false
	}

	if !CheckIntByRange(params, HNSWM, HNSWMinM, HNSWMaxM) {
		return false
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newRHNSWSQConfAdapter() *RHNSWSQConfAdapter {
	return &RHNSWSQConfAdapter{}
}

type NGTPANNGConfAdapter struct {
	BaseConfAdapter
}

func (adapter *NGTPANNGConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return false
	}

	if !CheckIntByRange(params, ForcedlyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return false
	}

	if !CheckIntByRange(params, SelectivelyPrunedEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return false
	}

	selectivelyPrunedEdgeSize, _ := strconv.Atoi(params[SelectivelyPrunedEdgeSize])
	forcedlyPrunedEdgeSize, _ := strconv.Atoi(params[ForcedlyPrunedEdgeSize])
	if selectivelyPrunedEdgeSize >= forcedlyPrunedEdgeSize {
		return false
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newNGTPANNGConfAdapter() *NGTPANNGConfAdapter {
	return &NGTPANNGConfAdapter{}
}

type NGTONNGConfAdapter struct {
	BaseConfAdapter
}

func (adapter *NGTONNGConfAdapter) CheckTrain(params map[string]string) bool {
	if !CheckIntByRange(params, EdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return false
	}

	if !CheckIntByRange(params, OutgoingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return false
	}

	if !CheckIntByRange(params, IncomingEdgeSize, NgtMinEdgeSize, NgtMaxEdgeSize) {
		return false
	}

	return adapter.BaseConfAdapter.CheckTrain(params)
}

func newNGTONNGConfAdapter() *NGTONNGConfAdapter {
	return &NGTONNGConfAdapter{}
}
