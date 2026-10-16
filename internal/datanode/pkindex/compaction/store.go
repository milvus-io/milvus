package compaction

import "context"

// Store moves SST files between the local disk and object storage. The
// object store of package internal/pkindex/objstore implements it.
type Store interface {
	// UploadFile writes the local file at path to key.
	UploadFile(ctx context.Context, key, path string) error
	// DownloadFile writes the object at key to the local file at path.
	DownloadFile(ctx context.Context, key, path string) error
}
