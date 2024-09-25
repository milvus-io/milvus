package walimplstest

import (
	"testing"

	"github.com/milvus-io/milvus/pkg/streaming/walimpls"
)

func TestWALImplsTest(t *testing.T) {
	walimpls.NewWALImplsTestFramework(t, 100, &openerBuilder{}).Run()
}
