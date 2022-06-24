package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPConfig_Init(t *testing.T) {
	cf := new(HTTPConfig)
	cf.InitOnce()
	assert.Equal(t, cf.Enabled, true)
	assert.Equal(t, cf.DebugMode, false)
}
