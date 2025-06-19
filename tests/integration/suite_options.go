package integration

type clusterSuiteOption struct {
	dropAllCollectionsWhenTestTearDown bool
	notResetDeploymentWhenTestTearDown bool
}

type ClusterSuiteOption func(o *clusterSuiteOption)

// WithDropAllCollectionsWhenTestTearDown drop all collections when test tear down
func WithDropAllCollectionsWhenTestTearDown() ClusterSuiteOption {
	return func(o *clusterSuiteOption) {
		o.dropAllCollectionsWhenTestTearDown = true
	}
}

// WithoutResetDeploymentWhenTestTearDown reset deployment when test tear down
func WithoutResetDeploymentWhenTestTearDown() ClusterSuiteOption {
	return func(o *clusterSuiteOption) {
		o.notResetDeploymentWhenTestTearDown = true
	}
}
