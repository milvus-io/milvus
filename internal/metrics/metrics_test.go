package metrics

import (
	"testing"
)

func TestRegisterMetrics(t *testing.T) {
	// Make sure it doesn't panic.
	RegisterMaster()
	RegisterDataNode()
	RegisterDataService()
	RegisterIndexNode()
	RegisterIndexService()
	RegisterProxyNode()
	RegisterProxyService()
	RegisterQueryNode()
	RegisterQueryService()
	RegisterMsgStreamService()
}
