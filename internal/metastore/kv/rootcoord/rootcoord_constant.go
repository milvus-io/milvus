package rootcoord

import (
	"fmt"

	"github.com/milvus-io/milvus/pkg/util"
)

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	DatabaseMetaPrefix       = ComponentPrefix + "/database"
	DBInfoMetaPrefix         = DatabaseMetaPrefix + "/db-info"
	CollectionInfoMetaPrefix = DatabaseMetaPrefix + "/collection-info"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	PartitionMetaPrefix = ComponentPrefix + "/partitions"
	AliasMetaPrefix     = ComponentPrefix + "/aliases"
	FieldMetaPrefix     = ComponentPrefix + "/fields"

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
