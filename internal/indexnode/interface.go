package indexnode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

type ServiceBase = typeutil.Service

type Interface interface {
	ServiceBase
	BuildIndex(req indexpb.BuildIndexRequest) (indexpb.BuildIndexResponse, error)
}
