package indexbuilder

import (
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

const (
	indexType  = "IVF_PQ"
	dim        = 8
	nlist      = 100
	m          = 4
	nbits      = 8
	metricType = "L2"
)

func TestIndex_New(t *testing.T) {
	typeParams := make(map[string]string)
	indexParams := make(map[string]string)
	indexParams["index_type"] = indexType
	indexParams["dim"] = strconv.Itoa(dim)
	indexParams["nlist"] = strconv.Itoa(nlist)
	indexParams["m"] = strconv.Itoa(m)
	indexParams["nbits"] = strconv.Itoa(nbits)
	indexParams["metric_type"] = metricType

	index, err := NewCIndex(typeParams, indexParams)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, index, nil)

	err = index.Delete()
	assert.Equal(t, err, nil)
}
