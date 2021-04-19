package indexservice

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type ServiceBase = typeutil.Component

type Interface interface {
	ServiceBase
	RegisterNode(req indexpb.RegisterNodeRequest) (indexpb.RegisterNodeResponse, error)
	BuildIndex(req indexpb.BuildIndexRequest) (indexpb.BuildIndexResponse, error)
	GetIndexStates(req indexpb.IndexStatesRequest) (indexpb.IndexStatesResponse, error)
	GetIndexFilePaths(req indexpb.IndexFilePathRequest) (indexpb.IndexFilePathsResponse, error)
}
