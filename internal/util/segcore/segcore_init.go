package segcore

import (
	_ "github.com/milvus-io/milvus/internal/util/cgo"
)

/*
#cgo pkg-config: milvus_core

#include "segcore/segcore_init_c.h"

*/
import "C"

// IndexEngineInfo contains all the information about the index engine.
type IndexEngineInfo struct {
	MinIndexVersion     int32
	CurrentIndexVersion int32
	MaxIndexVersion     int32
}

// GetIndexEngineInfo returns the minimal, current, and maximum version of the index engine.
func GetIndexEngineInfo() IndexEngineInfo {
	cMinimal, cCurrent, cMaximum := C.GetMinimalIndexVersion(), C.GetCurrentIndexVersion(), C.GetMaximumIndexVersion()
	return IndexEngineInfo{
		MinIndexVersion:     int32(cMinimal),
		CurrentIndexVersion: int32(cCurrent),
		MaxIndexVersion:     int32(cMaximum),
	}
}
