package querynodeimp

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/zilliztech/milvus-distributed/internal/proto/commonpb"
)

func TestLoadIndexInfo(t *testing.T) {
	indexParams := make([]*commonpb.KeyValuePair, 0)
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   "index_type",
		Value: "IVF_PQ",
	})
	indexParams = append(indexParams, &commonpb.KeyValuePair{
		Key:   "index_mode",
		Value: "cpu",
	})

	indexBytes := make([][]byte, 0)
	indexValue := make([]byte, 10)
	indexBytes = append(indexBytes, indexValue)
	indexPaths := make([]string, 0)
	indexPaths = append(indexPaths, "index-0")

	loadIndexInfo, err := newLoadIndexInfo()
	assert.Nil(t, err)
	for _, indexParam := range indexParams {
		loadIndexInfo.appendIndexParam(indexParam.Key, indexParam.Value)
	}
	loadIndexInfo.appendFieldInfo("field0", 0)
	loadIndexInfo.appendIndex(indexBytes, indexPaths)

	deleteLoadIndexInfo(loadIndexInfo)
}
