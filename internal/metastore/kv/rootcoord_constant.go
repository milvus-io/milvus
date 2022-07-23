package kv

const (
	// ComponentPrefix prefix for rootcoord component
	ComponentPrefix = "root-coord"

	// CollectionMetaPrefix prefix for collection meta
	CollectionMetaPrefix = ComponentPrefix + "/collection"

	// SegmentIndexMetaPrefix prefix for segment index meta
	SegmentIndexMetaPrefix = ComponentPrefix + "/segment-index"

	// IndexMetaPrefix prefix for index meta
	IndexMetaPrefix = ComponentPrefix + "/index"

	// CollectionAliasMetaPrefix prefix for collection alias meta
	CollectionAliasMetaPrefix = ComponentPrefix + "/collection-alias"

	// UserSubPrefix subpath for credential user
	UserSubPrefix = "/credential/users"

	// CredentialPrefix prefix for credential user
	CredentialPrefix = ComponentPrefix + UserSubPrefix
)
