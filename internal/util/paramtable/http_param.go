package paramtable

import (
	"sync"
)

type HTTPConfig struct {
	BaseTable

	once      sync.Once
	Enabled   bool
	DebugMode bool
	Port      int
}

// InitOnce initialize HTTPConfig
func (p *HTTPConfig) InitOnce() {
	p.once.Do(func() {
		p.init()
	})
}

func (p *HTTPConfig) init() {
	p.BaseTable.Init()

	p.initHTTPEnabled()
	p.initHTTPDebugMode()
	p.initPort()
}

func (p *HTTPConfig) initHTTPEnabled() {
	p.Enabled = p.ParseBool("proxy.http.enabled", true)
}

func (p *HTTPConfig) initHTTPDebugMode() {
	p.DebugMode = p.ParseBool("proxy.http.debug_mode", false)
}

func (p *HTTPConfig) initPort() {
	p.Port = p.ParseIntWithDefault("proxy.http.port", -1)
}
