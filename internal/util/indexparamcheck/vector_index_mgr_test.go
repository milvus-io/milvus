package indexparamcheck

import (
	"testing"
)

func Test_VecIndex_Init(t *testing.T) {
	mgr := GetVecIndexMgrInstance()
	mgr.init()
}
