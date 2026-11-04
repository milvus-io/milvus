package datacoord

import "github.com/milvus-io/milvus/internal/datacoord/snapshot"

type RestoreCollectionManager = snapshot.RestoreCollectionManager

func (s *Server) SetRestoreCollectionManager(manager RestoreCollectionManager) {
	s.restoreCollectionManager = manager
}
