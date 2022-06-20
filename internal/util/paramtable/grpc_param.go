// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

package paramtable

import (
	"fmt"
	"math"
	"strconv"
	"sync"
	"time"

	"github.com/go-basic/ipv4"
	"github.com/milvus-io/milvus/internal/log"
	"go.uber.org/zap"
)

const (
	// DefaultServerMaxSendSize defines the maximum size of data per grpc request can send by server side.
	DefaultServerMaxSendSize = math.MaxInt32

	// DefaultServerMaxRecvSize defines the maximum size of data per grpc request can receive by server side.
	DefaultServerMaxRecvSize = math.MaxInt32

	// DefaultClientMaxSendSize defines the maximum size of data per grpc request can send by client side.
	DefaultClientMaxSendSize = 100 * 1024 * 1024

	// DefaultClientMaxRecvSize defines the maximum size of data per grpc request can receive by client side.
	DefaultClientMaxRecvSize = 100 * 1024 * 1024

	// DefaultLogLevel defines the log level of grpc
	DefaultLogLevel = "WARNING"

	// Grpc Timeout related configs
	DefaultDialTimeout      = 5000 * time.Millisecond
	DefaultKeepAliveTime    = 10000 * time.Millisecond
	DefaultKeepAliveTimeout = 20000 * time.Millisecond

	// Grpc retry policy
	DefaultMaxAttempts               = 5
	DefaultInitialBackoff    float32 = 1.0
	DefaultMaxBackoff        float32 = 60.0
	DefaultBackoffMultiplier float32 = 2.0

	ProxyInternalPort = 19529
	ProxyExternalPort = 19530
)

///////////////////////////////////////////////////////////////////////////////
// --- grpc ---
type grpcConfig struct {
	ServiceParam

	once          sync.Once
	Domain        string
	IP            string
	TLSMode       int
	Port          int
	InternalPort  int
	ServerPemPath string
	ServerKeyPath string
	CaPemPath     string
}

func (p *grpcConfig) init(domain string) {
	p.ServiceParam.Init()
	p.Domain = domain

	p.LoadFromEnv()
	p.LoadFromArgs()
	p.initPort()
	p.initTLSPath()
}

// LoadFromEnv is used to initialize configuration items from env.
func (p *grpcConfig) LoadFromEnv() {
	p.IP = ipv4.LocalIP()
}

// LoadFromArgs is used to initialize configuration items from args.
func (p *grpcConfig) LoadFromArgs() {

}

func (p *grpcConfig) initPort() {
	p.Port = p.ParseIntWithDefault(p.Domain+".port", ProxyExternalPort)
	p.InternalPort = p.ParseIntWithDefault(p.Domain+".internalPort", ProxyInternalPort)
}

func (p *grpcConfig) initTLSPath() {
	p.TLSMode = p.ParseIntWithDefault("common.security.tlsMode", 0)
	p.ServerPemPath = p.Get("tls.serverPemPath")
	p.ServerKeyPath = p.Get("tls.serverKeyPath")
	p.CaPemPath = p.Get("tls.caPemPath")
}

// GetAddress return grpc address
func (p *grpcConfig) GetAddress() string {
	return p.IP + ":" + strconv.Itoa(p.Port)
}

func (p *grpcConfig) GetInternalAddress() string {
	return p.IP + ":" + strconv.Itoa(p.InternalPort)
}

// GrpcServerConfig is configuration for grpc server.
type GrpcServerConfig struct {
	grpcConfig

	ServerMaxSendSize int
	ServerMaxRecvSize int
}

// InitOnce initialize grpc server config once
func (p *GrpcServerConfig) InitOnce(domain string) {
	p.once.Do(func() {
		p.init(domain)
	})
}

func (p *GrpcServerConfig) init(domain string) {
	p.grpcConfig.init(domain)

	p.initServerMaxSendSize()
	p.initServerMaxRecvSize()
}

func (p *GrpcServerConfig) initServerMaxSendSize() {
	var err error

	valueStr, err := p.Load("grpc.serverMaxSendSize")
	if err != nil {
		valueStr, err = p.Load(p.Domain + ".grpc.serverMaxSendSize")
	}
	if err != nil {
		p.ServerMaxSendSize = DefaultServerMaxSendSize
	} else {
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			log.Warn("Failed to parse grpc.serverMaxSendSize, set to default",
				zap.String("role", p.Domain), zap.String("grpc.serverMaxSendSize", valueStr),
				zap.Error(err))
			p.ServerMaxSendSize = DefaultServerMaxSendSize
		} else {
			p.ServerMaxSendSize = value
		}
	}

	log.Debug("initServerMaxSendSize",
		zap.String("role", p.Domain), zap.Int("grpc.serverMaxSendSize", p.ServerMaxSendSize))
}

func (p *GrpcServerConfig) initServerMaxRecvSize() {
	var err error
	valueStr, err := p.Load("grpc.serverMaxRecvSize")
	if err != nil {
		valueStr, err = p.Load(p.Domain + ".grpc.serverMaxRecvSize")
	}
	if err != nil {
		p.ServerMaxRecvSize = DefaultServerMaxRecvSize
	} else {
		value, err := strconv.Atoi(valueStr)
		if err != nil {
			log.Warn("Failed to parse grpc.serverMaxRecvSize, set to default",
				zap.String("role", p.Domain), zap.String("grpc.serverMaxRecvSize", valueStr),
				zap.Error(err))
			p.ServerMaxRecvSize = DefaultServerMaxRecvSize
		} else {
			p.ServerMaxRecvSize = value
		}
	}

	log.Debug("initServerMaxRecvSize",
		zap.String("role", p.Domain), zap.Int("grpc.serverMaxRecvSize", p.ServerMaxRecvSize))
}

// GrpcClientConfig is configuration for grpc client.
type GrpcClientConfig struct {
	grpcConfig

	ClientMaxSendSize int
	ClientMaxRecvSize int

	DialTimeout      time.Duration
	KeepAliveTime    time.Duration
	KeepAliveTimeout time.Duration

	MaxAttempts       int
	InitialBackoff    float32
	MaxBackoff        float32
	BackoffMultiplier float32
}

// InitOnce initialize grpc client config once
func (p *GrpcClientConfig) InitOnce(domain string) {
	p.once.Do(func() {
		p.init(domain)
	})
}

func (p *GrpcClientConfig) init(domain string) {
	p.grpcConfig.init(domain)

	p.initClientMaxSendSize()
	p.initClientMaxRecvSize()
	p.initDialTimeout()
	p.initKeepAliveTimeout()
	p.initKeepAliveTime()
	p.initMaxAttempts()
	p.initInitialBackoff()
	p.initMaxBackoff()
	p.initBackoffMultiplier()
}

func (p *GrpcClientConfig) ParseConfig(funcDesc string, key string, backKey string, parseValue func(string) (interface{}, error), applyValue func(interface{}, error)) {
	var err error

	valueStr, err := p.Load(key)
	if err != nil && backKey != "" {
		valueStr, err = p.Load(backKey)
	}
	if err != nil {
		log.Warn(fmt.Sprintf("Failed to load %s, set to default", key), zap.String("role", p.Domain), zap.Error(err))
		applyValue(nil, err)
	} else {
		value, err := parseValue(valueStr)
		if err != nil {
			log.Warn(fmt.Sprintf("Failed to parse %s, set to default", key),
				zap.String("role", p.Domain), zap.String(key, valueStr), zap.Error(err))
			applyValue(nil, err)
		} else {
			applyValue(value, nil)
		}
	}

	log.Debug(funcDesc, zap.String("role", p.Domain), zap.Int(key, p.ClientMaxSendSize))
}

func (p *GrpcClientConfig) initClientMaxSendSize() {
	funcDesc := "Init client max send size"
	key := "grpc.clientMaxSendSize"
	p.ParseConfig(funcDesc, key, fmt.Sprintf("%s.%s", p.Domain, key),
		func(s string) (interface{}, error) {
			return strconv.Atoi(s)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.ClientMaxSendSize = DefaultClientMaxSendSize
				return
			}
			v, ok := i.(int)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert int when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.ClientMaxSendSize = DefaultClientMaxSendSize
				return
			}
			p.ClientMaxSendSize = v
		})
}

func (p *GrpcClientConfig) initClientMaxRecvSize() {
	funcDesc := "Init client max recv size"
	key := "grpc.clientMaxRecvSize"
	p.ParseConfig(funcDesc, key, fmt.Sprintf("%s.%s", p.Domain, key),
		func(s string) (interface{}, error) {
			return strconv.Atoi(s)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.ClientMaxRecvSize = DefaultClientMaxRecvSize
				return
			}
			v, ok := i.(int)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert int when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.ClientMaxRecvSize = DefaultClientMaxRecvSize
				return
			}
			p.ClientMaxRecvSize = v
		})
}

func (p *GrpcClientConfig) initDialTimeout() {
	funcDesc := "Init dial timeout"
	key := "grpc.client.dialTimeout"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.Atoi(s)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.DialTimeout = DefaultDialTimeout
				return
			}
			v, ok := i.(int)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert int when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.DialTimeout = DefaultDialTimeout
				return
			}
			p.DialTimeout = time.Duration(v) * time.Millisecond
		})
}

func (p *GrpcClientConfig) initKeepAliveTime() {
	funcDesc := "Init keep alive time"
	key := "grpc.client.keepAliveTime"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.Atoi(s)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.KeepAliveTime = DefaultKeepAliveTime
				return
			}
			v, ok := i.(int)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert int when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.KeepAliveTime = DefaultKeepAliveTime
				return
			}
			p.KeepAliveTime = time.Duration(v) * time.Millisecond
		})
}

func (p *GrpcClientConfig) initKeepAliveTimeout() {
	funcDesc := "Init keep alive timeout"
	key := "grpc.client.keepAliveTimeout"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.Atoi(s)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.KeepAliveTimeout = DefaultKeepAliveTimeout
				return
			}
			v, ok := i.(int)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert int when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.KeepAliveTimeout = DefaultKeepAliveTimeout
				return
			}
			p.KeepAliveTimeout = time.Duration(v) * time.Millisecond
		})
}

func (p *GrpcClientConfig) initMaxAttempts() {
	funcDesc := "Init max attempts"
	key := "grpc.client.maxMaxAttempts"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.Atoi(s)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.MaxAttempts = DefaultMaxAttempts
				return
			}
			// This field is required and must be greater than 1.
			// Any value greater than 5 will be treated as if it were 5.
			// See: https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto#L138
			v, ok := i.(int)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert int when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.MaxAttempts = DefaultMaxAttempts
				return
			}
			if v < 2 || v > 5 {
				log.Warn(fmt.Sprintf("The value of %s should be greater than 1 and less than 6, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.MaxAttempts = DefaultMaxAttempts
				return
			}
			p.MaxAttempts = v
		})
}

func (p *GrpcClientConfig) initInitialBackoff() {
	funcDesc := "Init initial back off"
	key := "grpc.client.initialBackOff"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.ParseFloat(s, 32)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.InitialBackoff = DefaultInitialBackoff
				return
			}
			v, ok := i.(float64)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert float64 when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.InitialBackoff = DefaultInitialBackoff
				return
			}
			p.InitialBackoff = float32(v)
		})
}

func (p *GrpcClientConfig) initMaxBackoff() {
	funcDesc := "Init max back off"
	key := "grpc.client.maxBackoff"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.ParseFloat(s, 32)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.MaxBackoff = DefaultMaxBackoff
				return
			}
			v, ok := i.(float64)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert float64 when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.MaxBackoff = DefaultMaxBackoff
				return
			}
			p.MaxBackoff = float32(v)
		})
}

func (p *GrpcClientConfig) initBackoffMultiplier() {
	funcDesc := "Init back off multiplier"
	key := "grpc.client.backoffMultiplier"
	p.ParseConfig(funcDesc, key, "",
		func(s string) (interface{}, error) {
			return strconv.ParseFloat(s, 32)
		},
		func(i interface{}, err error) {
			if err != nil {
				p.BackoffMultiplier = DefaultBackoffMultiplier
				return
			}
			v, ok := i.(float64)
			if !ok {
				log.Warn(fmt.Sprintf("Failed to convert float64 when parsing %s, set to default", key),
					zap.String("role", p.Domain), zap.Any(key, i))
				p.BackoffMultiplier = DefaultBackoffMultiplier
				return
			}
			p.BackoffMultiplier = float32(v)
		})
}
