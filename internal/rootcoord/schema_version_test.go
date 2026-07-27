package rootcoord

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/pkg/v3/common"
)

// TestSchemaVersionBumpHasSingleEntry guards invariant I1 (see schema_version.go):
// nextSchemaSnapshot must stay the ONLY place in rootcoord that advances
// schema.Version. The bump used to be duplicated across 10 sites in 6 files, with
// the mask append and the bump sometimes in different functions — a shape that rots
// into "DDL broadcasts a snapshot but reuses the version", which QueryNode and
// segcore both silently drop as a no-op.
//
// If this fails: do not add another `schema.Version = coll.SchemaVersion + 1`, call
// nextSchemaSnapshot instead.
func TestSchemaVersionBumpHasSingleEntry(t *testing.T) {
	entries, err := os.ReadDir(".")
	require.NoError(t, err)

	fset := token.NewFileSet()
	offenders := []string{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		file, err := parser.ParseFile(fset, filepath.Join(".", name), nil, 0)
		require.NoError(t, err)

		ast.Inspect(file, func(n ast.Node) bool {
			assign, ok := n.(*ast.AssignStmt)
			if !ok {
				return true
			}
			for i, lhs := range assign.Lhs {
				sel, ok := lhs.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Version" {
					continue
				}
				// The single sanctioned site that ADVANCES the version.
				if name == "schema_version.go" {
					continue
				}
				// Seeding a brand-new collection at version 0 is not a bump.
				if i < len(assign.Rhs) {
					if lit, ok := assign.Rhs[i].(*ast.BasicLit); ok && lit.Value == "0" {
						continue
					}
				}
				offenders = append(offenders, fset.Position(assign.Pos()).String())
			}
			return true
		})
	}

	assert.Empty(t, offenders,
		"schema.Version must only be assigned by nextSchemaSnapshot in schema_version.go; found assignments at: %v",
		offenders)
}

// TestSchemaPropertiesChangedIsDenyListed guards invariant I2: the decision to
// broadcast a schema snapshot is derived from property CONTENT, so a property key
// added later refreshes on QueryNode without anyone registering it. Only the two
// QueryCoord-only keys are exempt.
func TestSchemaPropertiesChangedIsDenyListed(t *testing.T) {
	kv := func(pairs ...string) []*commonpb.KeyValuePair {
		out := make([]*commonpb.KeyValuePair, 0, len(pairs)/2)
		for i := 0; i < len(pairs); i += 2 {
			out = append(out, &commonpb.KeyValuePair{Key: pairs[i], Value: pairs[i+1]})
		}
		return out
	}

	t.Run("unknown property refreshes by default", func(t *testing.T) {
		// The point of the deny-list: a key nobody has classified still refreshes.
		assert.True(t, schemaPropertiesChanged(
			kv("some.future.key", "a"),
			kv("some.future.key", "b")))
	})

	t.Run("properties that reach the effective load schema refresh", func(t *testing.T) {
		for _, key := range []string{
			common.MmapEnabledKey,
			common.WarmupScalarFieldKey,
			common.WarmupVectorFieldKey,
			common.WarmupVectorIndexKey,
			common.PartitionKeyIsolationKey,
			common.CollectionTTLFieldKey,
		} {
			assert.True(t, schemaPropertiesChanged(kv(key, "a"), kv(key, "b")), "key %s must refresh", key)
			assert.True(t, schemaPropertiesChanged(nil, kv(key, "a")), "setting %s must refresh", key)
			assert.True(t, schemaPropertiesChanged(kv(key, "a"), nil), "deleting %s must refresh", key)
		}
	})

	t.Run("querycoord-only properties do not refresh", func(t *testing.T) {
		// These have their own AlterLoadConfig channel; bumping on them would fence
		// every in-flight segment load on the shard into a retry on each resize.
		for _, key := range []string{
			common.CollectionReplicaNumber,
			common.CollectionResourceGroups,
		} {
			assert.False(t, schemaPropertiesChanged(kv(key, "1"), kv(key, "2")), "key %s must not refresh", key)
		}
	})

	t.Run("deny-list must not grow past the AlterLoadConfig keys", func(t *testing.T) {
		// timezone is a deliberate NON-entry. QueryNode never reads it off the schema
		// (rootcoord rewrites TIMESTAMPTZ defaults at DDL time, proxy formats results
		// from its own collection cache, import reads it from the file schema), so on
		// a "does anyone read it" test it would qualify for the deny-list.
		//
		// It stays out on purpose: the deny-list holds only keys that are BOTH
		// QueryCoord-only AND driven by automation, because those are the ones whose
		// bumps would fence in-flight segment loads at machine frequency. Growing it on
		// "nobody reads this one either" turns it back into the hand-maintained
		// enumeration this whole design exists to avoid — and every entry added by that
		// reasoning is a chance to be wrong in the silent direction. One extra broadcast
		// on a rare DDL is the accepted cost.
		assert.True(t, schemaPropertiesChanged(
			kv(common.TimezoneKey, "UTC"),
			kv(common.TimezoneKey, "Asia/Shanghai")))

		assert.Len(t, coordOnlyPropertyKeys, 2,
			"adding a key here silently stops QueryNode from ever seeing it change; "+
				"only do it for a property QueryCoord consumes through its own channel "+
				"AND that automation rewrites frequently")
	})

	t.Run("order insensitive", func(t *testing.T) {
		assert.False(t, schemaPropertiesChanged(
			kv("a", "1", "b", "2"),
			kv("b", "2", "a", "1")))
	})

	t.Run("mixed change still refreshes", func(t *testing.T) {
		assert.True(t, schemaPropertiesChanged(
			kv(common.CollectionReplicaNumber, "1", common.MmapEnabledKey, "false"),
			kv(common.CollectionReplicaNumber, "2", common.MmapEnabledKey, "true")))
	})
}

func TestNextSchemaSnapshotAdvancesVersion(t *testing.T) {
	coll := &model.Collection{
		Name:          "c",
		SchemaVersion: 7,
		Properties:    []*commonpb.KeyValuePair{{Key: common.MmapEnabledKey, Value: "true"}},
	}
	assert.EqualValues(t, 8, nextSchemaSnapshot(coll).GetVersion())
	assert.EqualValues(t, 7, coll.SchemaVersion, "must not mutate the source collection")
}
