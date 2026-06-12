// Package catalog defines the metadata catalog boundary for Milvus.
//
// EXPERIMENTAL: this API may break between minor versions until v1.0. Callers
// should pin a specific commit or tag and review the CHANGELOG before bumping.
package catalog

import (
	"context"
)

type Catalog interface {
	Metadata() MetadataCatalog
	AccessControl() AccessControlCatalog
	Migration() MigrationCatalog
	Close(ctx context.Context) error
}

type Implementation string

const (
	ImplLegacy         Implementation = "legacy"
	ImplCatalogService Implementation = "catalog-service"
	ImplMigration      Implementation = "migration"
)
