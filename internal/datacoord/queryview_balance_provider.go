package datacoord

import "github.com/milvus-io/milvus/internal/views/coord/balancer"

// DataViewProvider exposes committed, immutable entries and exact-version references.
func (s *Server) DataViewProvider() balancer.DataViewProvider {
	return s.dataViewManager
}
