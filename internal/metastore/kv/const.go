package kv

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	// SegmentIndexPrefix prefix for segment index meta
	SegmentIndexPrefix = "segment-index"

	// FieldIndexPrefix prefix for index meta
	FieldIndexPrefix = "field-index"

	// CollectionAliasMetaPrefix prefix for collection alias meta
	CollectionAliasMetaPrefix = ComponentPrefix + "/collection-alias"

	// UserSubPrefix subpath for credential user
	UserSubPrefix = "/credential/users"

	// CredentialPrefix prefix for credential user
	CredentialPrefix = ComponentPrefix + UserSubPrefix
)
