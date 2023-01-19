package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init()
	cf := params.HTTPCfg
	assert.Equal(t, cf.Enabled.GetAsBool(), true)
	assert.Equal(t, cf.DebugMode.GetAsBool(), false)
}
