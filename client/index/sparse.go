package index

import (
	"fmt"
)

const (
	dropRatio = `drop_ratio_build`
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
	b.WithExtraParam("drop_ratio_search", dropRatio)
}
