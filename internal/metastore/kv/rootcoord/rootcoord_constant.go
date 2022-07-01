package rootcoord

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	PartitionMetaPrefix = ComponentPrefix + "/partitions"
	AliasMetaPrefix     = ComponentPrefix + "/aliases"
	FieldMetaPrefix     = ComponentPrefix + "/fields"

	// SegmentIndexMetaPrefix prefix for segment index meta
	SegmentIndexMetaPrefix = ComponentPrefix + "/segment-index"

	// IndexMetaPrefix prefix for index meta
	IndexMetaPrefix = ComponentPrefix + "/index"

	// CollectionAliasMetaPrefix prefix for collection alias meta
	CollectionAliasMetaPrefix = ComponentPrefix + "/collection-alias"

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

	// GranteePrefix prefix for mapping among user or role, resource type, resource name
	GranteePrefix = ComponentPrefix + CommonCredentialPrefix + "/grantee-privileges"
)
