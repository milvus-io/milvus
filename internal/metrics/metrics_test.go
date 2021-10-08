package metrics

import (
	"testing"
)

func TestRegisterMetrics(t *testing.T) {
	// Make sure it doesn't panic.
	RegisterRootCoord()
	RegisterDataNode()
	RegisterDataCoord()
	RegisterIndexNode()
	RegisterIndexCoord()
	RegisterProxy()
	RegisterQueryNode()
	RegisterQueryCoord()
	RegisterMsgStreamCoord()
}
