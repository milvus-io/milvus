package shardview

import (
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/recovery"
	"github.com/milvus-io/milvus/internal/coordinator/view/qviews/syncer"
)

var (
	recoveryStorage recovery.RecoveryStorage
	coordSyncer     syncer.CoordSyncer
)
