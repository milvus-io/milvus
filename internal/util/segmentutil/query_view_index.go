package segmentutil

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v3/util/indexparams"
)

// PackQueryViewFieldIndexInfo combines the selected collection definition with
// the physical artifact. It builds private index parameters for one load.
func PackQueryViewFieldIndexInfo(definition *indexpb.IndexInfo, artifact *indexpb.IndexFilePathInfo) *querypb.FieldIndexInfo {
	indexParams := common.CloneKeyValuePairs(definition.GetIndexParams())
	indexParams = append(indexParams, common.CloneKeyValuePairs(definition.GetTypeParams())...)
	for _, param := range indexParams {
		if param.GetKey() == common.IndexTypeKey && artifact.GetIndexType() != "" && artifact.GetIndexType() != param.GetValue() {
			param.Value = artifact.GetIndexType()
			break
		}
	}
	params := funcutil.KeyValuePair2Map(indexParams)
	for _, kv := range definition.GetUserIndexParams() {
		if indexparams.IsConfigableIndexParam(kv.GetKey()) {
			params[kv.GetKey()] = kv.GetValue()
		}
	}
	indexParams = append(funcutil.Map2KeyValuePair(params), &commonpb.KeyValuePair{
		Key: common.LoadPriorityKey, Value: commonpb.LoadPriority_HIGH.String(),
	})
	indexName := definition.GetIndexName()
	if artifact.GetIndexType() != "" {
		indexName = artifact.GetIndexType()
	}
	return &querypb.FieldIndexInfo{
		FieldID:                   definition.GetFieldID(),
		EnableIndex:               true,
		IndexName:                 indexName,
		IndexID:                   artifact.GetIndexID(),
		BuildID:                   artifact.GetBuildID(),
		IndexParams:               indexParams,
		IndexFilePaths:            append([]string(nil), artifact.GetIndexFilePaths()...),
		IndexSize:                 int64(artifact.GetMemSize()),
		IndexVersion:              artifact.GetIndexVersion(),
		NumRows:                   artifact.GetNumRows(),
		CurrentIndexVersion:       artifact.GetCurrentIndexVersion(),
		CurrentScalarIndexVersion: artifact.GetCurrentScalarIndexVersion(),
		IndexStorePathVersion:     artifact.GetIndexStorePathVersion(),
	}
}
