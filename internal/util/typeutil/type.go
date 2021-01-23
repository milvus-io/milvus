package typeutil

type Timestamp = uint64
type IntPrimaryKey = int64
type UniqueID = int64

const (
	MasterServiceRole = "MasterService"
	ProxyServiceRole  = "ProxyService"
	ProxyNodeRole     = "ProxyNode"
	QueryServiceRole  = "QueryService"
	QueryNodeRole     = "QueryNode"
	IndexServiceRole  = "IndexService"
	IndexNodeRole     = "IndexNode"
	DataServiceRole   = "DataService"
	DataNodeRole      = "DataNode"
)
