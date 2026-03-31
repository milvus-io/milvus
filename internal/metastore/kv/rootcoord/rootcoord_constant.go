package rootcoord

import (
	"bytes"
	"fmt"

	"github.com/milvus-io/milvus/pkg/v2/util"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	DatabaseMetaPrefix       = ComponentPrefix + "/database"
	DBInfoMetaPrefix         = DatabaseMetaPrefix + "/db-info"
	CollectionInfoMetaPrefix = DatabaseMetaPrefix + "/collection-info"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	PartitionMetaPrefix        = ComponentPrefix + "/partitions"
	AliasMetaPrefix            = ComponentPrefix + "/aliases"
	FieldMetaPrefix            = ComponentPrefix + "/fields"
	StructArrayFieldMetaPrefix = ComponentPrefix + "/struct-array-fields"
	FunctionMetaPrefix         = ComponentPrefix + "/functions"

	// CollectionAliasMetaPrefix210 prefix for collection alias meta
	CollectionAliasMetaPrefix210 = ComponentPrefix + "/collection-alias"

	SnapshotsSep   = "_ts"
	SnapshotPrefix = "snapshots"
	Aliases        = "aliases"

	// CommonCredentialPrefix subpath for common credential
	/* #nosec G101 */
	CommonCredentialPrefix = "/credential"

	// UserSubPrefix subpath for credential user
	UserSubPrefix = CommonCredentialPrefix + "/users"

	// CredentialPrefix prefix for credential user
	CredentialPrefix = ComponentPrefix + UserSubPrefix

	// RolePrefix prefix for role
	RolePrefix = ComponentPrefix + CommonCredentialPrefix + "/roles"

	// RoleMappingPrefix prefix for mapping between user and role
	RoleMappingPrefix = ComponentPrefix + CommonCredentialPrefix + "/user-role-mapping"

	// GranteePrefix prefix for mapping among role, resource type, resource name
	GranteePrefix = ComponentPrefix + CommonCredentialPrefix + "/grantee-privileges"

	// GranteeIDPrefix prefix for mapping among privilege and grantor
	GranteeIDPrefix = ComponentPrefix + CommonCredentialPrefix + "/grantee-id"

	// PrivilegeGroupPrefix prefix for privilege group
	PrivilegeGroupPrefix = ComponentPrefix + "/privilege-group"
)

func BuildDatabasePrefixWithDBID(dbID int64) string {
	return fmt.Sprintf("%s/%d", CollectionInfoMetaPrefix, dbID)
}

func BuildCollectionKeyWithDBID(dbID int64, collectionID int64) string {
	return fmt.Sprintf("%s/%d/%d", CollectionInfoMetaPrefix, dbID, collectionID)
}

func BuildDatabaseKey(dbID int64) string {
	return fmt.Sprintf("%s/%d", DBInfoMetaPrefix, dbID)
}

func getDatabasePrefix(dbID int64) string {
	if dbID != util.NonDBID {
		return BuildDatabasePrefixWithDBID(dbID)
	}
	return CollectionMetaPrefix
}

func BuildPrivilegeGroupkey(groupName string) string {
	return fmt.Sprintf("%s/%s", PrivilegeGroupPrefix, groupName)
}

// Legacy snapshot utilities — kept for migration tool compatibility only.

// SuffixSnapshotTombstone is the tombstone marker used in legacy snapshot keys.
var SuffixSnapshotTombstone = []byte{0xE2, 0x9B, 0xBC}

// IsTombstone checks whether the value is a legacy tombstone marker.
func IsTombstone(value string) bool {
	return bytes.Equal([]byte(value), SuffixSnapshotTombstone)
}

// ConstructTombstone returns a copy of the tombstone marker.
func ConstructTombstone() []byte {
	return append([]byte{}, SuffixSnapshotTombstone...)
}

// ComposeSnapshotKey builds a legacy snapshot key from prefix, key, separator, and timestamp.
func ComposeSnapshotKey(snapshotPrefix string, key string, separator string, ts typeutil.Timestamp) string {
	return snapshotPrefix + "/" + fmt.Sprintf("%s%s%d", key, separator, ts)
}
