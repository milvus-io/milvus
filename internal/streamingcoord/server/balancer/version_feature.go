package balancer

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// VersionFeature identifies a capability that is gated on the cluster-wide streaming
// version (channel.StreamingVersion*) and on every node of the roles it depends on being
// new enough to understand it.
//
// Adding a capability is one constant here, one descriptor in versionFeatureSpecs, and one
// call to Balancer.WaitUntilVersionFeatureReady. The constants start at 1 so the zero value
// can keep meaning "no dependency".
type VersionFeature int

const (
	// VersionFeatureWALBasedDDL accepts DDL through the streaming WAL; it needs every
	// StreamingNode to understand the WAL-based DDL messages.
	VersionFeatureWALBasedDDL VersionFeature = iota + 1
	// VersionFeatureSchemaDrop drops a schema field; it needs every Proxy to attach the
	// schema version to its writes, so a drop cannot race with a legacy write.
	VersionFeatureSchemaDrop
)

// roleVersionRequirement is one "every live node of Role must be newer than VersionRange"
// prerequisite of a version feature. VersionRange is the session version range string the
// resolver understands, e.g. "<2.6.6-dev"; a node whose session matches it is too old.
type roleVersionRequirement struct {
	Role         string
	VersionRange string
}

const (
	// versionChecker265 matches the sessions too old for the WAL-based DDL; the gate waits
	// until no such node is live.
	versionChecker265 = "<2.6.6-dev"
	// versionChecker300 matches the sessions too old for the schema-drop DDL.
	versionChecker300 = "<3.0.0-beta"
)

// versionFeatureSpec is the declarative description of one VersionFeature.
type versionFeatureSpec struct {
	// name is the stable identifier used in logs and errors.
	name string
	// marker is the sticky streaming version persisted once the feature is available.
	marker int64
	// requires lists the roles that must all be new enough before the feature may be
	// used. A feature usually needs exactly one role.
	requires []roleVersionRequirement
	// dependsOn chains the previous gate, so a feature can never be enabled before the
	// one it builds on.
	dependsOn VersionFeature
	// precondition is an optional extra wait that is neither a role version nor a
	// chained feature. Only the WAL-based DDL gate needs one today: the streaming
	// service must have been enabled at least once.
	precondition func(ctx context.Context, cm *channel.ChannelManager) error
}

var versionFeatureSpecs = map[VersionFeature]versionFeatureSpec{
	VersionFeatureWALBasedDDL: {
		name:   "wal-based-ddl",
		marker: channel.StreamingVersion265,
		requires: []roleVersionRequirement{
			{Role: typeutil.StreamingNodeRole, VersionRange: versionChecker265},
		},
		precondition: func(ctx context.Context, cm *channel.ChannelManager) error {
			return cm.WaitUntilStreamingEnabled(ctx)
		},
	},
	VersionFeatureSchemaDrop: {
		name:   "schema-drop",
		marker: channel.StreamingVersion300,
		requires: []roleVersionRequirement{
			{Role: typeutil.ProxyRole, VersionRange: versionChecker300},
		},
		dependsOn: VersionFeatureWALBasedDDL,
	},
}

// String returns the feature's name for logs and errors. An unregistered value is
// rendered numerically so a message that includes it can never come out empty.
func (f VersionFeature) String() string {
	if spec, ok := versionFeatureSpecs[f]; ok {
		return spec.name
	}
	return fmt.Sprintf("VersionFeature(%d)", int(f))
}

// spec returns the feature's descriptor. A constant without a descriptor is a programming
// error and must not panic on a request path, so it is reported as an internal error.
func (f VersionFeature) spec() (versionFeatureSpec, error) {
	spec, ok := versionFeatureSpecs[f]
	if !ok {
		return versionFeatureSpec{}, merr.WrapErrServiceInternalMsg("unknown streaming version feature %s", f)
	}
	return spec, nil
}
