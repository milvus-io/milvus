package paramtable

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHTTPConfig_Init(t *testing.T) {
	cf := new(HTTPConfig)
	cf.InitOnce()
	assert.Equal(t, cf.Enabled, true)
	assert.Equal(t, cf.DebugMode, false)
	assert.Equal(t, cf.Port, 8080)
	assert.Equal(t, cf.ReadTimeout, time.Second*30)
	assert.Equal(t, cf.WriteTimeout, time.Second*30)
}
