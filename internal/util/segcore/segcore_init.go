package segcore

/*
#cgo pkg-config: milvus_core

#include "segcore/segcore_init_c.h"

*/
import "C"

// IndexEngineInfo contains all the information about the index engine.
type IndexEngineInfo struct {
	MinIndexVersion     int32
	CurrentIndexVersion int32
}

// GetIndexEngineInfo returns the minimal and current version of the index engine.
func GetIndexEngineInfo() IndexEngineInfo {
	cMinimal, cCurrent := C.GetMinimalIndexVersion(), C.GetCurrentIndexVersion()
	return IndexEngineInfo{
		MinIndexVersion:     int32(cMinimal),
		CurrentIndexVersion: int32(cCurrent),
	}
}
