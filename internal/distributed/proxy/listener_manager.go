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

package grpcproxy

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"

	"github.com/cockroachdb/errors"
	"github.com/soheilhy/cmux"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/netutil"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

// newListenerManager creates a new listener
func newListenerManager(ctx context.Context) (l *listenerManager, err error) {
	defer func() {
		if err != nil && l != nil {
			l.Close()
		}
	}()

	log := log.Ctx(ctx)
	externalGrpcListener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().ProxyGrpcServerCfg.IP),
		netutil.OptPort(paramtable.Get().ProxyGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		log.Warn("Proxy fail to create external grpc listener", zap.Error(err))
		return
	}
	log.Info("Proxy listen on external grpc listener", zap.String("address", externalGrpcListener.Address()), zap.Int("port", externalGrpcListener.Port()))

	internalGrpcListener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().ProxyGrpcServerCfg.IP),
		netutil.OptPort(paramtable.Get().ProxyGrpcServerCfg.InternalPort.GetAsInt()),
	)
	if err != nil {
		log.Warn("Proxy fail to create internal grpc listener", zap.Error(err))
		return
	}
	log.Info("Proxy listen on internal grpc listener", zap.String("address", internalGrpcListener.Address()), zap.Int("port", internalGrpcListener.Port()))

	l = &listenerManager{
		externalGrpcListener: externalGrpcListener,
		internalGrpcListener: internalGrpcListener,
	}
	if err = newHTTPListner(ctx, l); err != nil {
		return
	}
	return
}

// newHTTPListner creates a new http listener
func newHTTPListner(ctx context.Context, l *listenerManager) error {
	log := log.Ctx(ctx)
	HTTPParams := &paramtable.Get().HTTPCfg
	if !HTTPParams.Enabled.GetAsBool() {
		// http server is disabled
		log.Info("Proxy server(http) is disabled, skip initialize http listener")
		return nil
	}
	tlsMode := paramtable.Get().ProxyGrpcServerCfg.TLSMode.GetAsInt()
	if tlsMode != 0 && tlsMode != 1 && tlsMode != 2 {
		return errors.New("tls mode must be 0: no authentication, 1: one way authentication or 2: two way authentication")
	}

	httpPortString := HTTPParams.Port.GetValue()
	httpPort := HTTPParams.Port.GetAsInt()
	externGrpcPort := l.externalGrpcListener.Port()
	if len(httpPortString) == 0 || externGrpcPort == httpPort {
		if tlsMode != 0 {
			err := errors.New("proxy server(http) and external grpc server share the same port, tls mode must be 0")
			log.Warn("can not initialize http listener", zap.Error(err))
			return err
		}
		log.Info("Proxy server(http) and external grpc server share the same port")
		l.portShareMode = true
		l.cmux = cmux.New(l.externalGrpcListener)
		l.cmuxClosed = make(chan struct{})
		l.cmuxExternGrpcListener = l.cmux.MatchWithWriters(cmux.HTTP2MatchHeaderFieldSendSettings("content-type", "application/grpc"))
		l.cmuxExternHTTPListener = l.cmux.Match(cmux.Any())
		go func() {
			defer close(l.cmuxClosed)
			if err := l.cmux.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warn("Proxy cmux server closed", zap.Error(err))
				return
			}
			log.Info("Proxy tcp server exited")
		}()
		return nil
	}

	Params := &paramtable.Get().ProxyGrpcServerCfg
	var tlsConf *tls.Config
	switch tlsMode {
	case 1:
		creds, err := tls.LoadX509KeyPair(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			log.Error("proxy can't create creds", zap.Error(err))
			return err
		}
		tlsConf = &tls.Config{Certificates: []tls.Certificate{creds}}
	case 2:
		cert, err := tls.LoadX509KeyPair(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			log.Error("proxy cant load x509 key pair", zap.Error(err))
			return err
		}
		certPool := x509.NewCertPool()
		rootBuf, err := storage.ReadFile(Params.CaPemPath.GetValue())
		if err != nil {
			log.Error("failed read ca pem", zap.Error(err))
			return err
		}
		if !certPool.AppendCertsFromPEM(rootBuf) {
			log.Warn("fail to append ca to cert")
			return fmt.Errorf("fail to append ca to cert")
		}
		tlsConf = &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool,
			MinVersion:   tls.VersionTLS13,
		}
	}

	var err error
	l.portShareMode = false
	l.httpListener, err = netutil.NewListener(netutil.OptIP(Params.IP), netutil.OptPort(httpPort), netutil.OptTLS(tlsConf))
	if err != nil {
		log.Warn("Proxy server(http) failed to listen on", zap.Error(err))
		return err
	}
	log.Info("Proxy server(http) listen on", zap.Int("port", l.httpListener.Port()))
	return nil
}

type listenerManager struct {
	externalGrpcListener *netutil.NetListener
	internalGrpcListener *netutil.NetListener

	portShareMode bool
	// portShareMode == true
	cmux                   cmux.CMux
	cmuxClosed             chan struct{}
	cmuxExternGrpcListener net.Listener
	cmuxExternHTTPListener net.Listener

	// portShareMode == false
	httpListener *netutil.NetListener
}

func (l *listenerManager) ExternalGrpcListener() net.Listener {
	if l.portShareMode {
		return l.cmuxExternGrpcListener
	}
	return l.externalGrpcListener
}

func (l *listenerManager) InternalGrpcListener() net.Listener {
	return l.internalGrpcListener
}

func (l *listenerManager) HTTPListener() net.Listener {
	if l.portShareMode {
		return l.cmuxExternHTTPListener
	}
	// httpListener maybe nil if http server is disabled
	if l.httpListener == nil {
		return nil
	}
	return l.httpListener
}

func (l *listenerManager) Close() {
	log := log.Ctx(context.TODO())
	if l.portShareMode {
		if l.cmux != nil {
			log.Info("Proxy close cmux grpc listener")
			if err := l.cmuxExternGrpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warn("Proxy failed to close cmux grpc listener", zap.Error(err))
			}
			log.Info("Proxy close cmux http listener")
			if err := l.cmuxExternHTTPListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warn("Proxy failed to close cmux http listener", zap.Error(err))
			}
			log.Info("Proxy close cmux...")
			l.cmux.Close()
			<-l.cmuxClosed
			log.Info("Proxy cmux closed")
		}
	} else {
		if l.httpListener != nil {
			log.Info("Proxy close http listener", zap.String("address", l.httpListener.Address()))
			if err := l.httpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				log.Warn("Proxy failed to close http listener", zap.Error(err))
			}
		}
	}

	if l.internalGrpcListener != nil {
		log.Info("Proxy close internal grpc listener", zap.String("address", l.internalGrpcListener.Address()))
		if err := l.internalGrpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warn("Proxy failed to close internal grpc listener", zap.Error(err))
		}
	}

	if l.externalGrpcListener != nil {
		log.Info("Proxy close external grpc listener", zap.String("address", l.externalGrpcListener.Address()))
		if err := l.externalGrpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			log.Warn("Proxy failed to close external grpc listener", zap.Error(err))
		}
	}
}
