package integration

import "github.com/milvus-io/milvus/tests/integration/cluster"

type clusterSuiteOption struct {
	dropAllCollectionsWhenTestTearDown bool
	notResetDeploymentWhenTestTearDown bool
	clusterOptions                     []cluster.MiniClusterV3Option
}

type ClusterSuiteOption func(o *clusterSuiteOption)

// WithDropAllCollectionsWhenTestTearDown drop all collections when test tear down
func WithDropAllCollectionsWhenTestTearDown() ClusterSuiteOption {
	return func(o *clusterSuiteOption) {
		o.dropAllCollectionsWhenTestTearDown = true
	}
}

// WithoutResetDeploymentWhenTestTearDown reset deployment when test tear down
// the reset operation will keep only 1 default component for milvus cluster.
func WithoutResetDeploymentWhenTestTearDown() ClusterSuiteOption {
	return func(o *clusterSuiteOption) {
		o.notResetDeploymentWhenTestTearDown = true
	}
}

// WithClusterOptions passes extra options to the cluster SetupSuite starts.
func WithClusterOptions(opts ...cluster.MiniClusterV3Option) ClusterSuiteOption {
	return func(o *clusterSuiteOption) {
		o.clusterOptions = append(o.clusterOptions, opts...)
	}
}
