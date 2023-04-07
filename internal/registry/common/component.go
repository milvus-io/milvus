package common

import "github.com/milvus-io/milvus/pkg/util/typeutil"

// specified type only
// mixcoord and standalone mode registers detail session in each component for now
var (
	coordinators = typeutil.NewSet(
		typeutil.RootCoordRole,
		typeutil.QueryCoordRole,
		typeutil.DataCoordRole,
		typeutil.IndexCoordRole,
	)
	nodes = typeutil.NewSet(
		typeutil.ProxyRole,
		typeutil.QueryNodeRole,
		typeutil.DataNodeRole,
		typeutil.IndexNodeRole,
	)
)

// IsCoordinator returns provided component type is in coordinators set.
func IsCoordinator(component string) bool {
	return coordinators.Contain(component)
}

// IsNode returns provided component type is in nodes set.
func IsNode(component string) bool {
	return nodes.Contain(component)
}
