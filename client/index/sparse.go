package index

import (
	"fmt"
)

const (
	dropRatio = `drop_ratio_build`

	sparseDropRatioSearchKey  = `drop_ratio_search`
	sparseSearchAlgoKey       = `search_algo`
	sparseRefineFactorKey     = `refine_factor`
	sparseDimMaxScoreRatioKey = `dim_max_score_ratio`
)

var _ Index = sparseInvertedIndex{}

// IndexSparseInverted index type for SPARSE_INVERTED_INDEX
type sparseInvertedIndex struct {
	baseIndex
	dropRatio float64
}

func (idx sparseInvertedIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(SparseInverted),
		dropRatio:     fmt.Sprintf("%v", idx.dropRatio),
	}
}

func NewSparseInvertedIndex(metricType MetricType, dropRatio float64) Index {
	return sparseInvertedIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  SparseInverted,
		},

		dropRatio: dropRatio,
	}
}

var _ Index = sparseWANDIndex{}

type sparseWANDIndex struct {
	baseIndex
	dropRatio float64
}

func (idx sparseWANDIndex) Params() map[string]string {
	return map[string]string{
		MetricTypeKey: string(idx.metricType),
		IndexTypeKey:  string(SparseWAND),
		dropRatio:     fmt.Sprintf("%v", idx.dropRatio),
	}
}

// IndexSparseWAND index type for SPARSE_WAND, weak-and
func NewSparseWANDIndex(metricType MetricType, dropRatio float64) Index {
	return sparseWANDIndex{
		baseIndex: baseIndex{
			metricType: metricType,
			indexType:  SparseWAND,
		},

		dropRatio: dropRatio,
	}
}

type sparseAnnParam struct {
	baseAnnParam
}

func NewSparseAnnParam() sparseAnnParam {
	return sparseAnnParam{
		baseAnnParam: baseAnnParam{
			params: make(map[string]any),
		},
	}
}

func (b sparseAnnParam) WithDropRatio(dropRatio float64) {
	b.WithExtraParam(sparseDropRatioSearchKey, dropRatio)
}

// WithSearchAlgo overrides the traversal algorithm for this search. Defaults to
// "INHERIT", i.e. whatever the index was built with.
func (b sparseAnnParam) WithSearchAlgo(algo string) sparseAnnParam {
	b.WithExtraParam(sparseSearchAlgoKey, algo)
	return b
}

// WithRefineFactor sets how many extra candidates are gathered before refining
// against the full values. Server default 1.
func (b sparseAnnParam) WithRefineFactor(refineFactor int) sparseAnnParam {
	b.WithExtraParam(sparseRefineFactorKey, refineFactor)
	return b
}

// WithDimMaxScoreRatio tunes the block-max pruning threshold. Server default
// 1.05.
func (b sparseAnnParam) WithDimMaxScoreRatio(ratio float64) sparseAnnParam {
	b.WithExtraParam(sparseDimMaxScoreRatioKey, ratio)
	return b
}
