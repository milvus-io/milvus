// Copyright 2017 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package config

import (
	"sync/atomic"
	"time"
	//"github.com/czs007/suvlim/master/kv"
	"github.com/czs007/suvlim/master/meta"

)

// PersistOptions wraps all configurations that need to persist to storage and
// allows to access them safely.
type PersistOptions struct {
	pdServerConfig  atomic.Value
}

// NewPersistOptions creates a new PersistOptions instance.
func NewPersistOptions(cfg *Config) *PersistOptions {
	o := &PersistOptions{}
	o.pdServerConfig.Store(&cfg.PDServerCfg)
	return o
}

// GetPDServerConfig returns pd server configurations.
func (o *PersistOptions) GetPDServerConfig() *PDServerConfig {
	return o.pdServerConfig.Load().(*PDServerConfig)
}

// SetPDServerConfig sets the PD configuration.
func (o *PersistOptions) SetPDServerConfig(cfg *PDServerConfig) {
	o.pdServerConfig.Store(cfg)
}


// GetMaxResetTSGap gets the max gap to reset the tso.
func (o *PersistOptions) GetMaxResetTSGap() time.Duration {
	return o.GetPDServerConfig().MaxResetTSGap.Duration
}


// Persist saves the configuration to the storage.
func (o *PersistOptions) Persist(storage *meta.Storage) error {
	cfg := &Config{
		PDServerCfg:     *o.GetPDServerConfig(),
	}
	err := storage.SaveConfig(cfg)
	return err
}

// Reload reloads the configuration from the storage.
func (o *PersistOptions) Reload(storage *meta.Storage) error {
	cfg := &Config{}
	// pass nil to initialize cfg to default values (all items undefined)
	cfg.Adjust(nil)

	isExist, err := storage.LoadConfig(cfg)
	if err != nil {
		return err
	}
	if isExist {
		o.pdServerConfig.Store(&cfg.PDServerCfg)
	}
	return nil
}
