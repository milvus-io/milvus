package querynode

import (
	"github.com/zilliztech/milvus-distributed/internal/proto/datapb"
	"github.com/zilliztech/milvus-distributed/internal/proto/indexpb"
	"github.com/zilliztech/milvus-distributed/internal/proto/milvuspb"
	"github.com/zilliztech/milvus-distributed/internal/proto/querypb"
	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"
)

const rowIDFieldID = 0
const timestampFieldID = 1

type UniqueID = typeutil.UniqueID
type Timestamp = typeutil.Timestamp
type IntPrimaryKey = typeutil.IntPrimaryKey
type DSL = string

type TimeRange struct {
	timestampMin Timestamp
	timestampMax Timestamp
}

type MasterServiceInterface interface {
	DescribeSegment(in *milvuspb.DescribeSegmentRequest) (*milvuspb.DescribeSegmentResponse, error)
}

type QueryServiceInterface interface {
	RegisterNode(req *querypb.RegisterNodeRequest) (*querypb.RegisterNodeResponse, error)
}

type DataServiceInterface interface {
	GetInsertBinlogPaths(req *datapb.InsertBinlogPathRequest) (*datapb.InsertBinlogPathsResponse, error)
}

type IndexServiceInterface interface {
	GetIndexFilePaths(req *indexpb.IndexFilePathsRequest) (*indexpb.IndexFilePathsResponse, error)
}
