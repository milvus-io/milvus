// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package paramtable

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/backoff"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

const (
	// DefaultServerMaxSendSize defines the maximum size of data per grpc request can send by server side.
	DefaultServerMaxSendSize = 512 * 1024 * 1024

	// DefaultServerMaxRecvSize defines the maximum size of data per grpc request can receive by server side.
	DefaultServerMaxRecvSize = 256 * 1024 * 1024

	// DefaultClientMaxSendSize defines the maximum size of data per grpc request can send by client side.
	DefaultClientMaxSendSize = 256 * 1024 * 1024

	// DefaultClientMaxRecvSize defines the maximum size of data per grpc request can receive by client side.
	DefaultClientMaxRecvSize = 512 * 1024 * 1024

	// DefaultLogLevel defines the log level of grpc
	DefaultLogLevel = "WARNING"

	// Grpc Timeout related configs
	DefaultDialTimeout      = 200
	DefaultKeepAliveTime    = 10000
	DefaultKeepAliveTimeout = 20000

	// Grpc retry policy
	DefaultMaxAttempts                = 10
	DefaultInitialBackoff     float64 = 0.2
	DefaultMaxBackoff         float64 = 10
	DefaultCompressionEnabled bool    = false

	ProxyInternalPort = 19529
	ProxyExternalPort = 19530
)

// /////////////////////////////////////////////////////////////////////////////
// --- grpc ---
type grpcConfig struct {
	Domain        string    `refreshable:"false"`
	IP            string    `refreshable:"false"`
	TLSMode       ParamItem `refreshable:"false"`
	IPItem        ParamItem `refreshable:"false"`
	Port          ParamItem `refreshable:"false"`
	InternalPort  ParamItem `refreshable:"false"`
	ServerPemPath ParamItem `refreshable:"false"`
	ServerKeyPath ParamItem `refreshable:"false"`
	CaPemPath     ParamItem `refreshable:"false"`
}

func (p *grpcConfig) init(domain string, base *BaseTable) {
	p.Domain = domain
	p.IPItem = ParamItem{
		Key:     p.Domain + ".ip",
		Version: "2.3.3",
		Doc:     "TCP/IP address of " + p.Domain + ". If not specified, use the first unicastable address",
		Export:  true,
	}
	p.IPItem.Init(base.mgr)
	p.IP = funcutil.GetIP(p.IPItem.GetValue())

	p.Port = ParamItem{
		Key:          p.Domain + ".port",
		Version:      "2.0.0",
		DefaultValue: strconv.FormatInt(ProxyExternalPort, 10),
		Doc:          "TCP port of " + p.Domain,
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

	GracefulStopTimeout ParamItem `refreshable:"true"`
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
		Doc:    "The maximum size of each RPC request that the " + domain + " can send, unit: byte",
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
		Doc:    "The maximum size of each RPC request that the " + domain + " can receive, unit: byte",
		Export: true,
	}
	p.ServerMaxRecvSize.Init(base.mgr)

	p.GracefulStopTimeout = ParamItem{
		Key:          "grpc.gracefulStopTimeout",
		Version:      "2.3.1",
		DefaultValue: "3",
		Doc:          "second, time to wait graceful stop finish",
		Export:       true,
	}
	p.GracefulStopTimeout.Init(base.mgr)
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

	MaxAttempts             ParamItem `refreshable:"false"`
	InitialBackoff          ParamItem `refreshable:"false"`
	MaxBackoff              ParamItem `refreshable:"false"`
	BackoffMultiplier       ParamItem `refreshable:"false"`
	MinResetInterval        ParamItem `refreshable:"false"`
	MaxCancelError          ParamItem `refreshable:"false"`
	MinSessionCheckInterval ParamItem `refreshable:"false"`
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
		Doc:    "The maximum size of each RPC request that the clients on " + domain + " can send, unit: byte",
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
		Doc:    "The maximum size of each RPC request that the clients on " + domain + " can receive, unit: byte",
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
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to convert int when parsing grpc.client.maxMaxAttempts, set to default",
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
			_, err := strconv.ParseFloat(v, 64)
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

	p.BackoffMultiplier = ParamItem{
		Key:          "grpc.client.backoffMultiplier",
		Version:      "2.5.0",
		DefaultValue: "2.0",
		Export:       true,
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
				return compressionEnabled
			}
			return v
		},
		Export: true,
	}
	p.CompressionEnabled.Init(base.mgr)

	p.MinResetInterval = ParamItem{
		Key:          "grpc.client.minResetInterval",
		DefaultValue: "1000",
		Formatter: func(v string) string {
			if v == "" {
				return "1000"
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.client.minResetInterval, set to default",
					zap.String("role", p.Domain), zap.String("grpc.client.minResetInterval", v),
					zap.Error(err))
				return "1000"
			}
			return v
		},
		Export: true,
	}
	p.MinResetInterval.Init(base.mgr)

	p.MinSessionCheckInterval = ParamItem{
		Key:          "grpc.client.minSessionCheckInterval",
		DefaultValue: "200",
		Formatter: func(v string) string {
			if v == "" {
				return "200"
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.client.minSessionCheckInterval, set to default",
					zap.String("role", p.Domain), zap.String("grpc.client.minSessionCheckInterval", v),
					zap.Error(err))
				return "200"
			}
			return v
		},
		Export: true,
	}
	p.MinSessionCheckInterval.Init(base.mgr)

	p.MaxCancelError = ParamItem{
		Key:          "grpc.client.maxCancelError",
		DefaultValue: "32",
		Formatter: func(v string) string {
			if v == "" {
				return "32"
			}
			_, err := strconv.Atoi(v)
			if err != nil {
				log.Warn("Failed to parse grpc.client.maxCancelError, set to default",
					zap.String("role", p.Domain), zap.String("grpc.client.maxCancelError", v),
					zap.Error(err))
				return "32"
			}
			return v
		},
		Export: true,
	}
	p.MaxCancelError.Init(base.mgr)
}

// GetDialOptionsFromConfig returns grpc dial options from config.
func (p *GrpcClientConfig) GetDialOptionsFromConfig() []grpc.DialOption {
	compress := ""
	if p.CompressionEnabled.GetAsBool() {
		compress = "zstd"
	}
	return []grpc.DialOption{
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(p.ClientMaxRecvSize.GetAsInt()),
			grpc.MaxCallSendMsgSize(p.ClientMaxSendSize.GetAsInt()),
			grpc.UseCompressor(compress),
		),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                p.KeepAliveTime.GetAsDuration(time.Millisecond),
			Timeout:             p.KeepAliveTimeout.GetAsDuration(time.Millisecond),
			PermitWithoutStream: true,
		}),
		grpc.WithConnectParams(grpc.ConnectParams{
			Backoff: backoff.Config{
				BaseDelay:  100 * time.Millisecond,
				Multiplier: 1.6,
				Jitter:     0.2,
				MaxDelay:   3 * time.Second,
			},
			MinConnectTimeout: p.DialTimeout.GetAsDuration(time.Millisecond),
		}),
	}
}

// GetDefaultRetryPolicy returns default grpc retry policy.
func (p *GrpcClientConfig) GetDefaultRetryPolicy() map[string]interface{} {
	return map[string]interface{}{
		"maxAttempts":       p.MaxAttempts.GetAsInt(),
		"initialBackoff":    fmt.Sprintf("%fs", p.InitialBackoff.GetAsFloat()),
		"maxBackoff":        fmt.Sprintf("%fs", p.MaxBackoff.GetAsFloat()),
		"backoffMultiplier": p.BackoffMultiplier.GetAsFloat(),
	}
}

type InternalTLSConfig struct {
	InternalTLSEnabled       ParamItem `refreshable:"false"`
	InternalTLSServerPemPath ParamItem `refreshable:"false"`
	InternalTLSServerKeyPath ParamItem `refreshable:"false"`
	InternalTLSCaPemPath     ParamItem `refreshable:"false"`
	InternalTLSSNI           ParamItem `refreshable:"false"`
}

func (p *InternalTLSConfig) Init(base *BaseTable) {
	p.InternalTLSEnabled = ParamItem{
		Key:          "common.security.internaltlsEnabled",
		Version:      "2.5.0",
		DefaultValue: "false",
		Export:       true,
	}
	p.InternalTLSEnabled.Init(base.mgr)

	p.InternalTLSServerPemPath = ParamItem{
		Key:     "internaltls.serverPemPath",
		Version: "2.5.0",
		Export:  true,
	}
	p.InternalTLSServerPemPath.Init(base.mgr)

	p.InternalTLSServerKeyPath = ParamItem{
		Key:     "internaltls.serverKeyPath",
		Version: "2.5.0",
		Export:  true,
	}
	p.InternalTLSServerKeyPath.Init(base.mgr)

	p.InternalTLSCaPemPath = ParamItem{
		Key:     "internaltls.caPemPath",
		Version: "2.5.0",
		Export:  true,
	}
	p.InternalTLSCaPemPath.Init(base.mgr)

	p.InternalTLSSNI = ParamItem{
		Key:     "internaltls.sni",
		Version: "2.5.0",
		Export:  true,
		Doc:     "The server name indication (SNI) for internal TLS, should be the same as the name provided by the certificates ref: https://en.wikipedia.org/wiki/Server_Name_Indication",
	}
	p.InternalTLSSNI.Init(base.mgr)
}

func (p *InternalTLSConfig) GetClientCreds(ctx context.Context) (credentials.TransportCredentials, error) {
	if !p.InternalTLSEnabled.GetAsBool() {
		return insecure.NewCredentials(), nil
	}
	caPemPath := p.InternalTLSCaPemPath.GetValue()
	sni := p.InternalTLSSNI.GetValue()
	creds, err := credentials.NewClientTLSFromFile(caPemPath, sni)
	if err != nil {
		log.Ctx(ctx).Error("Failed to create internal TLS credentials", zap.Error(err))
		return nil, fmt.Errorf("failed to create internal TLS credentials: %w", err)
	}
	return creds, nil
}
