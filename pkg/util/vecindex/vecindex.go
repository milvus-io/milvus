// Copyright (c) KIOXIA Corporation. All rights reserved.
// Licensed under the MIT license.

// Package vecindex provides index type constants and lightweight predicate
// functions that can be used from both pkg/ and internal/ without pulling in
// CGO dependencies.
package vecindex

// IndexType is a string alias for vector index type names.
type IndexType = string

// Well-known vector index type constants.
const (
	IndexDiskANN IndexType = "DISKANN"
	IndexAISAQ   IndexType = "AISAQ"
)

// IsDiskANN returns true when indexType is the DiskANN index.
func IsDiskANN(indexType IndexType) bool {
	return indexType == IndexDiskANN
}

// IsAISAQ returns true when indexType is the AISAQ index.
func IsAISAQ(indexType IndexType) bool {
	return indexType == IndexAISAQ
}
