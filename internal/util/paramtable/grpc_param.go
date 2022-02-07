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
	"math"
	"strconv"
	"sync"

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
)

///////////////////////////////////////////////////////////////////////////////
// --- grpc ---
type grpcConfig struct {
	ServiceParam

	once   sync.Once
	Domain string
	IP     string
	Port   int
}

func (p *grpcConfig) init(domain string) {
	p.ServiceParam.Init()
	p.Domain = domain

	p.LoadFromEnv()
	p.LoadFromArgs()
	p.initPort()
}

// LoadFromEnv is used to initialize configuration items from env.
func (p *grpcConfig) LoadFromEnv() {
	p.IP = ipv4.LocalIP()
}

// LoadFromArgs is used to initialize configuration items from args.
func (p *grpcConfig) LoadFromArgs() {

}

func (p *grpcConfig) initPort() {
	p.Port = p.ParseInt(p.Domain + ".port")
}

// GetAddress return grpc address
func (p *grpcConfig) GetAddress() string {
	return p.IP + ":" + strconv.Itoa(p.Port)
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

	valueStr, err := p.Load(p.Domain + ".grpc.serverMaxSendSize")
	if err != nil {
		p.ServerMaxSendSize = DefaultServerMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.serverMaxSendSize, set to default",
			zap.String("rol", p.Domain), zap.String("grpc.serverMaxSendSize", valueStr),
			zap.Error(err))

		p.ServerMaxSendSize = DefaultServerMaxSendSize
	} else {
		p.ServerMaxSendSize = value
	}

	log.Debug("initServerMaxSendSize",
		zap.String("role", p.Domain), zap.Int("grpc.serverMaxSendSize", p.ServerMaxSendSize))
}

func (p *GrpcServerConfig) initServerMaxRecvSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.serverMaxRecvSize")
	if err != nil {
		p.ServerMaxRecvSize = DefaultServerMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.serverMaxRecvSize, set to default",
			zap.String("role", p.Domain), zap.String("grpc.serverMaxRecvSize", valueStr),
			zap.Error(err))

		p.ServerMaxRecvSize = DefaultServerMaxRecvSize
	} else {
		p.ServerMaxRecvSize = value
	}

	log.Debug("initServerMaxRecvSize",
		zap.String("role", p.Domain), zap.Int("grpc.serverMaxRecvSize", p.ServerMaxRecvSize))
}

// GrpcClientConfig is configuration for grpc client.
type GrpcClientConfig struct {
	grpcConfig

	ClientMaxSendSize int
	ClientMaxRecvSize int
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
}

func (p *GrpcClientConfig) initClientMaxSendSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.clientMaxSendSize")
	if err != nil {
		p.ClientMaxSendSize = DefaultClientMaxSendSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.clientMaxSendSize, set to default",
			zap.String("role", p.Domain), zap.String("grpc.clientMaxSendSize", valueStr),
			zap.Error(err))

		p.ClientMaxSendSize = DefaultClientMaxSendSize
	} else {
		p.ClientMaxSendSize = value
	}

	log.Debug("initClientMaxSendSize",
		zap.String("role", p.Domain), zap.Int("grpc.clientMaxSendSize", p.ClientMaxSendSize))
}

func (p *GrpcClientConfig) initClientMaxRecvSize() {
	var err error

	valueStr, err := p.Load(p.Domain + ".grpc.clientMaxRecvSize")
	if err != nil {
		p.ClientMaxRecvSize = DefaultClientMaxRecvSize
	}

	value, err := strconv.Atoi(valueStr)
	if err != nil {
		log.Warn("Failed to parse grpc.clientMaxRecvSize, set to default",
			zap.String("role", p.Domain), zap.String("grpc.clientMaxRecvSize", valueStr),
			zap.Error(err))

		p.ClientMaxRecvSize = DefaultClientMaxRecvSize
	} else {
		p.ClientMaxRecvSize = value
	}

	log.Debug("initClientMaxRecvSize",
		zap.String("role", p.Domain), zap.Int("grpc.clientMaxRecvSize", p.ClientMaxRecvSize))
}
