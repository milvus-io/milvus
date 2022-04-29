package paramtable

import (
	"sync"
	"time"
)

type HTTPConfig struct {
	BaseTable

	once         sync.Once
	Enabled      bool
	DebugMode    bool
	Port         int
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
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
	p.initHTTPPort()
	p.initHTTPReadTimeout()
	p.initHTTPWriteTimeout()
}

func (p *HTTPConfig) initHTTPEnabled() {
	p.Enabled = p.ParseBool("proxy.http.enabled", true)
}

func (p *HTTPConfig) initHTTPDebugMode() {
	p.DebugMode = p.ParseBool("proxy.http.debug_mode", false)
}

func (p *HTTPConfig) initHTTPPort() {
	p.Port = p.ParseIntWithDefault("proxy.http.port", 8080)
}

func (p *HTTPConfig) initHTTPReadTimeout() {
	interval := p.ParseIntWithDefault("proxy.http.readTimeout", 30000)
	p.ReadTimeout = time.Duration(interval) * time.Millisecond
}

func (p *HTTPConfig) initHTTPWriteTimeout() {
	interval := p.ParseIntWithDefault("proxy.http.writeTimeout", 30000)
	p.WriteTimeout = time.Duration(interval) * time.Millisecond
}
