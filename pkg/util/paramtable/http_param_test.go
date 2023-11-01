package paramtable

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHTTPConfig_Init(t *testing.T) {
	params := ComponentParam{}
	params.Init(NewBaseTable(SkipRemote(true)))
	cfg := &params.HTTPCfg
	assert.Equal(t, cfg.Enabled.GetAsBool(), true)
	assert.Equal(t, cfg.DebugMode.GetAsBool(), false)
	assert.Equal(t, cfg.Port.GetValue(), "")
	assert.Equal(t, cfg.AcceptTypeAllowInt64.GetValue(), "false")
}
