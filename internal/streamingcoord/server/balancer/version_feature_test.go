package balancer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/channel"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The feature table must stay consistent with the constants: every feature the code can ask
// for has a descriptor with the expected marker, role, version range and dependency, an
// unregistered value is an internal error instead of a panic, and only the WAL-based DDL
// gate carries the extra "streaming enabled" precondition.
//
// This lives in the internal test package because it pins the unexported descriptor fields;
// the version ranges are asserted literally so changing one is a deliberate act.
func TestVersionFeatureRegistry(t *testing.T) {
	cases := []struct {
		feature      VersionFeature
		name         string
		marker       int64
		role         string
		versionRange string
		depends      VersionFeature
	}{
		{VersionFeatureWALBasedDDL, "wal-based-ddl", channel.StreamingVersion265, typeutil.StreamingNodeRole, "<2.6.6-dev", 0},
		{VersionFeatureSchemaDrop, "schema-drop", channel.StreamingVersion300, typeutil.ProxyRole, "<3.0.0-beta", VersionFeatureWALBasedDDL},
	}
	for _, tc := range cases {
		spec, err := tc.feature.spec()
		require.NoError(t, err)
		assert.Equal(t, tc.name, spec.name)
		assert.Equal(t, tc.name, tc.feature.String())
		assert.Equal(t, tc.marker, spec.marker)
		require.Len(t, spec.requires, 1)
		assert.Equal(t, tc.role, spec.requires[0].Role)
		assert.Equal(t, tc.versionRange, spec.requires[0].VersionRange)
		assert.Equal(t, tc.depends, spec.dependsOn)
	}

	walSpec, err := VersionFeatureWALBasedDDL.spec()
	require.NoError(t, err)
	assert.NotNil(t, walSpec.precondition, "the WAL-based DDL gate waits for streaming to be enabled")
	schemaDropSpec, err := VersionFeatureSchemaDrop.spec()
	require.NoError(t, err)
	assert.Nil(t, schemaDropSpec.precondition)

	unknown := VersionFeature(999)
	_, err = unknown.spec()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown streaming version feature")
	assert.Equal(t, "VersionFeature(999)", unknown.String())
}
