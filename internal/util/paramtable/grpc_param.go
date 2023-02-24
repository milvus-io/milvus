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
	"strconv"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/funcutil"
	"go.uber.org/zap"
)

const (
	// DefaultServerMaxSendSize defines the maximum size of data per grpc request can send by server side.
	DefaultServerMaxSendSize = 512 * 1024 * 1024

	// DefaultServerMaxRecvSize defines the maximum size of data per grpc request can receive by server side.
	DefaultServerMaxRecvSize = 512 * 1024 * 1024

	// DefaultClientMaxSendSize defines the maximum size of data per grpc request can send by client side.
	DefaultClientMaxSendSize = 256 * 1024 * 1024

	// DefaultClientMaxRecvSize defines the maximum size of data per grpc request can receive by client side.
	DefaultClientMaxRecvSize = 256 * 1024 * 1024

	// DefaultLogLevel defines the log level of grpc
	DefaultLogLevel = "WARNING"

	// Grpc Timeout related configs
	DefaultDialTimeout      = 5000
	DefaultKeepAliveTime    = 10000
	DefaultKeepAliveTimeout = 20000

	// Grpc retry policy
	DefaultMaxAttempts               = 5
	DefaultInitialBackoff    float64 = 1.0
	DefaultMaxBackoff        float64 = 60.0
	DefaultBackoffMultiplier float64 = 2.0

	DefaultCompressionEnabled bool = false

	ProxyInternalPort = 19529
	ProxyExternalPort = 19530
)

// /////////////////////////////////////////////////////////////////////////////
// --- grpc ---
type grpcConfig struct {
	Domain        string    `refreshable:"false"`
	IP            string    `refreshable:"false"`
	TLSMode       ParamItem `refreshable:"false"`
	Port          ParamItem `refreshable:"false"`
	InternalPort  ParamItem `refreshable:"false"`
	ServerPemPath ParamItem `refreshable:"false"`
	ServerKeyPath ParamItem `refreshable:"false"`
	CaPemPath     ParamItem `refreshable:"false"`
}

func (p *grpcConfig) init(domain string, base *BaseTable) {
	p.Domain = domain
	p.IP = funcutil.GetLocalIP()

	p.Port = ParamItem{
		Key:          p.Domain + ".port",
		Version:      "2.0.0",
		DefaultValue: strconv.FormatInt(ProxyExternalPort, 10),
		Export:       true,
	}
	p.Port.Init(base.mgr)

	p.InternalPort = ParamItem{
		Key:          p.Domain + ".internalPort",
		Version:      "2.0.0",
		DefaultValue: strconv.FormatInt(ProxyInternalPort, 10),
	}
	p.InternalPort.Init(base.mgr)

	p.TLSMode = ParamItem{
		Key:          "common.security.tlsMode",
		Version:      "2.0.0",
		DefaultValue: "0",
		Export:       true,
	}
	p.TLSMode.Init(base.mgr)

	p.ServerPemPath = ParamItem{
		Key:     "tls.serverPemPath",
		Version: "2.0.0",
		Export:  true,
	}
	p.ServerPemPath.Init(base.mgr)

	p.ServerKeyPath = ParamItem{
		Key:     "tls.serverKeyPath",
		Version: "2.0.0",
		Export:  true,
	}
	p.ServerKeyPath.Init(base.mgr)

	p.CaPemPath = ParamItem{
		Key:     "tls.caPemPath",
		Version: "2.0.0",
		Export:  true,
	}
	p.CaPemPath.Init(base.mgr)
}

// GetAddress return grpc address
func (p *grpcConfig) GetAddress() string {
	return p.IP + ":" + p.Port.GetValue()
}

func (p *grpcConfig) GetInternalAddress() string {
	return p.IP + ":" + p.InternalPort.GetValue()
}

// GrpcServerConfig is configuration for grpc server.
type GrpcServerConfig struct {
	grpcConfig

	ServerMaxSendSize ParamItem `refreshable:"false"`
	ServerMaxRecvSize ParamItem `refreshable:"false"`
}

func (p *GrpcServerConfig) Init(domain string, base *BaseTable) {
	p.grpcConfig.init(domain, base)

	maxSendSize := strconv.FormatInt(DefaultServerMaxSendSize, 10)
	p.ServerMaxSendSize = ParamItem{
		Key:          p.Domain + ".grpc.serverMaxSendSize",
		DefaultValue: maxSendSize,
		FallbackKeys: []string{"grpc.serverMaxSendSize"},
		Formatter: func(v string) string {
			if v == "" {
				return maxSendSize
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.serverMaxSendSize, set to default",
					zap.String("role", p.Domain), zap.String("grpc.serverMaxSendSize", v),
					zap.Error(err))
				return maxSendSize
			}
			return v
		},
		Export: true,
	}
	p.ServerMaxSendSize.Init(base.mgr)

	maxRecvSize := strconv.FormatInt(DefaultServerMaxRecvSize, 10)
	p.ServerMaxRecvSize = ParamItem{
		Key:          p.Domain + ".grpc.serverMaxRecvSize",
		DefaultValue: maxRecvSize,
		FallbackKeys: []string{"grpc.serverMaxRecvSize"},
		Formatter: func(v string) string {
			if v == "" {
				return maxRecvSize
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.serverMaxRecvSize, set to default",
					zap.String("role", p.Domain), zap.String("grpc.serverMaxRecvSize", v),
					zap.Error(err))
				return maxRecvSize
			}
			return v
		},
		Export: true,
	}
	p.ServerMaxRecvSize.Init(base.mgr)
}

// GrpcClientConfig is configuration for grpc client.
type GrpcClientConfig struct {
	grpcConfig

	CompressionEnabled ParamItem `refreshable:"false"`

	ClientMaxSendSize ParamItem `refreshable:"false"`
	ClientMaxRecvSize ParamItem `refreshable:"false"`

	DialTimeout      ParamItem `refreshable:"false"`
	KeepAliveTime    ParamItem `refreshable:"false"`
	KeepAliveTimeout ParamItem `refreshable:"false"`

	MaxAttempts       ParamItem `refreshable:"false"`
	InitialBackoff    ParamItem `refreshable:"false"`
	MaxBackoff        ParamItem `refreshable:"false"`
	BackoffMultiplier ParamItem `refreshable:"false"`
}

func (p *GrpcClientConfig) Init(domain string, base *BaseTable) {
	p.grpcConfig.init(domain, base)

	maxSendSize := strconv.FormatInt(DefaultClientMaxSendSize, 10)
	p.ClientMaxSendSize = ParamItem{
		Key:          p.Domain + ".grpc.clientMaxSendSize",
		DefaultValue: maxSendSize,
		FallbackKeys: []string{"grpc.clientMaxSendSize"},
		Formatter: func(v string) string {
			if v == "" {
				return maxSendSize
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.clientMaxSendSize, set to default",
					zap.String("role", p.Domain), zap.String("grpc.clientMaxSendSize", v),
					zap.Error(err))
				return maxSendSize
			}
			return v
		},
		Export: true,
	}
	p.ClientMaxSendSize.Init(base.mgr)

	maxRecvSize := strconv.FormatInt(DefaultClientMaxRecvSize, 10)
	p.ClientMaxRecvSize = ParamItem{
		Key:          p.Domain + ".grpc.clientMaxRecvSize",
		DefaultValue: maxRecvSize,
		FallbackKeys: []string{"grpc.clientMaxRecvSize"},
		Formatter: func(v string) string {
			if v == "" {
				return maxRecvSize
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.clientMaxRecvSize, set to default",
					zap.String("role", p.Domain), zap.String("grpc.clientMaxRecvSize", v),
					zap.Error(err))
				return maxRecvSize
			}
			return v
		},
		Export: true,
	}
	p.ClientMaxRecvSize.Init(base.mgr)

	dialTimeout := strconv.FormatInt(DefaultDialTimeout, 10)
	p.DialTimeout = ParamItem{
		Key:     "grpc.client.dialTimeout",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return dialTimeout
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.dialTimeout, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.dialTimeout", v))
				return dialTimeout
			}
			return v
		},
		Export: true,
	}
	p.DialTimeout.Init(base.mgr)

	keepAliveTimeout := strconv.FormatInt(DefaultKeepAliveTimeout, 10)
	p.KeepAliveTimeout = ParamItem{
		Key:     "grpc.client.keepAliveTimeout",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return keepAliveTimeout
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.keepAliveTimeout, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.keepAliveTimeout", v))
				return keepAliveTimeout
			}
			return v
		},
		Export: true,
	}
	p.KeepAliveTimeout.Init(base.mgr)

	keepAliveTime := strconv.FormatInt(DefaultKeepAliveTime, 10)
	p.KeepAliveTime = ParamItem{
		Key:     "grpc.client.keepAliveTime",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return keepAliveTime
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.keepAliveTime, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.keepAliveTime", v))
				return keepAliveTime
			}
			return v
		},
		Export: true,
	}
	p.KeepAliveTime.Init(base.mgr)

	maxAttempts := strconv.FormatInt(DefaultMaxAttempts, 10)
	p.MaxAttempts = ParamItem{
		Key:     "grpc.client.maxMaxAttempts",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return maxAttempts
			}
			iv, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.maxMaxAttempts, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.maxMaxAttempts", v))
				return maxAttempts
			}
			if iv < 2 || iv > 5 {
				log.Warn("The value of %s should be greater than 1 and less than 6, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.maxMaxAttempts", v))
				return maxAttempts
			}
			return v
		},
		Export: true,
	}
	p.MaxAttempts.Init(base.mgr)

	initialBackoff := fmt.Sprintf("%f", DefaultInitialBackoff)
	p.InitialBackoff = ParamItem{
		Key:     "grpc.client.initialBackoff",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return initialBackoff
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.initialBackoff, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.initialBackoff", v))
				return initialBackoff
			}
			return v
		},
		Export: true,
	}
	p.InitialBackoff.Init(base.mgr)

	maxBackoff := fmt.Sprintf("%f", DefaultMaxBackoff)
	p.MaxBackoff = ParamItem{
		Key:     "grpc.client.maxBackoff",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return maxBackoff
			}
			_, err := strconv.ParseFloat(v, 64)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.maxBackoff, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.maxBackoff", v))
				return maxBackoff
			}
			return v
		},
		Export: true,
	}
	p.MaxBackoff.Init(base.mgr)

	backoffMultiplier := fmt.Sprintf("%f", DefaultBackoffMultiplier)
	p.BackoffMultiplier = ParamItem{
		Key:     "grpc.client.backoffMultiplier",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return backoffMultiplier
			}
			_, err := strconv.ParseFloat(v, 64)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.backoffMultiplier, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.backoffMultiplier", v))
				return backoffMultiplier
			}
			return v
		},
		Export: true,
	}
	p.BackoffMultiplier.Init(base.mgr)

	compressionEnabled := fmt.Sprintf("%t", DefaultCompressionEnabled)
	p.CompressionEnabled = ParamItem{
		Key:     "grpc.client.compressionEnabled",
		Version: "2.0.0",
		Formatter: func(v string) string {
			if v == "" {
				return compressionEnabled
			}
			_, err := strconv.ParseBool(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.compressionEnabled, set to default",
					zap.String("role", p.Domain),
					zap.String("grpc.client.compressionEnabled", v))
				return backoffMultiplier
			}
			return v
		},
		Export: true,
	}
	p.CompressionEnabled.Init(base.mgr)
}
