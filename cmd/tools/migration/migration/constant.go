package migration

import "github.com/milvus-io/milvus/pkg/v2/util/typeutil"

const (
	Role = "migration"
)

var Roles = []string{
	typeutil.RootCoordRole,
	typeutil.IndexCoordRole,
	typeutil.IndexNodeRole,
	typeutil.DataCoordRole,
	typeutil.DataNodeRole,
	typeutil.QueryCoordRole,
	typeutil.QueryNodeRole,
	typeutil.ProxyRole,
}
