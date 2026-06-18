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
	"net"

	"github.com/cockroachdb/errors"
	"github.com/soheilhy/cmux"
	"golang.org/x/net/http2"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/netutil"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

var httpServerNextProtos = []string{http2.NextProtoTLS, "http/1.1"}

// newListenerManager creates a new listener
func newListenerManager(ctx context.Context) (l *listenerManager, err error) {
	defer func() {
		if err != nil && l != nil {
			l.Close()
		}
	}()

	log := mlog.With()
	externalGrpcListener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().ProxyGrpcServerCfg.IP),
		netutil.OptPort(paramtable.Get().ProxyGrpcServerCfg.Port.GetAsInt()),
	)
	if err != nil {
		mlog.Warn(context.TODO(), "Proxy fail to create external grpc listener", mlog.Err(err))
		return
	}
	mlog.Info(context.TODO(), "Proxy listen on external grpc listener", mlog.String("address", externalGrpcListener.Address()), mlog.Int("port", externalGrpcListener.Port()))

	internalGrpcListener, err := netutil.NewListener(
		netutil.OptIP(paramtable.Get().ProxyGrpcServerCfg.IP),
		netutil.OptPort(paramtable.Get().ProxyGrpcServerCfg.InternalPort.GetAsInt()),
	)
	if err != nil {
		mlog.Warn(context.TODO(), "Proxy fail to create internal grpc listener", mlog.Err(err))
		return
	}
	mlog.Info(context.TODO(), "Proxy listen on internal grpc listener", mlog.String("address", internalGrpcListener.Address()), mlog.Int("port", internalGrpcListener.Port()))

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
	log := mlog.With()
	HTTPParams := &paramtable.Get().HTTPCfg
	if !HTTPParams.Enabled.GetAsBool() {
		// http server is disabled
		mlog.Info(context.TODO(), "Proxy server(http) is disabled, skip initialize http listener")
		return nil
	}
	tlsMode := paramtable.Get().ProxyGrpcServerCfg.TLSMode.GetAsInt()
	if tlsMode != 0 && tlsMode != 1 && tlsMode != 2 {
		return merr.WrapErrParameterInvalidMsg("tls mode must be 0: no authentication, 1: one way authentication or 2: two way authentication")
	}

	httpPortString := HTTPParams.Port.GetValue()
	httpPort := HTTPParams.Port.GetAsInt()
	externGrpcPort := l.externalGrpcListener.Port()
	if len(httpPortString) == 0 || externGrpcPort == httpPort {
		if tlsMode != 0 {
			err := merr.WrapErrParameterInvalidMsg("proxy server(http) and external grpc server share the same port, tls mode must be 0")
			mlog.Warn(context.TODO(), "can not initialize http listener", mlog.Err(err))
			return err
		}
		mlog.Info(context.TODO(), "Proxy server(http) and external grpc server share the same port")
		l.portShareMode = true
		l.cmux = cmux.New(l.externalGrpcListener)
		l.cmuxClosed = make(chan struct{})
		l.cmuxExternHTTP2Listener = l.cmux.Match(cmux.HTTP2())
		l.cmuxExternHTTPListener = l.cmux.Match(cmux.Any())
		go func() {
			defer close(l.cmuxClosed)
			if err := l.cmux.Serve(); err != nil && !errors.Is(err, net.ErrClosed) {
				mlog.Warn(context.TODO(), "Proxy cmux server closed", mlog.Err(err))
				return
			}
			mlog.Info(context.TODO(), "Proxy tcp server exited")
		}()
		return nil
	}

	Params := &paramtable.Get().ProxyGrpcServerCfg
	var tlsConf *tls.Config
	switch tlsMode {
	case 1:
		creds, err := tls.LoadX509KeyPair(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			mlog.Error(context.TODO(), "proxy can't create creds", mlog.Err(err))
			return err
		}
		tlsConf = &tls.Config{Certificates: []tls.Certificate{creds}, NextProtos: httpServerNextProtos}
	case 2:
		cert, err := tls.LoadX509KeyPair(Params.ServerPemPath.GetValue(), Params.ServerKeyPath.GetValue())
		if err != nil {
			mlog.Error(context.TODO(), "proxy cant load x509 key pair", mlog.Err(err))
			return err
		}
		certPool := x509.NewCertPool()
		rootBuf, err := storage.ReadFile(Params.CaPemPath.GetValue())
		if err != nil {
			mlog.Error(context.TODO(), "failed read ca pem", mlog.Err(err))
			return err
		}
		if !certPool.AppendCertsFromPEM(rootBuf) {
			mlog.Warn(context.TODO(), "fail to append ca to cert")
			return merr.WrapErrParameterInvalidMsg("fail to append ca to cert")
		}
		tlsConf = &tls.Config{
			ClientAuth:   tls.RequireAndVerifyClientCert,
			Certificates: []tls.Certificate{cert},
			ClientCAs:    certPool,
			MinVersion:   tls.VersionTLS13,
			NextProtos:   httpServerNextProtos,
		}
	}

	var err error
	l.portShareMode = false
	l.httpListener, err = netutil.NewListener(netutil.OptIP(Params.IP), netutil.OptPort(httpPort), netutil.OptTLS(tlsConf))
	if err != nil {
		mlog.Warn(context.TODO(), "Proxy server(http) failed to listen on", mlog.Err(err))
		return err
	}
	mlog.Info(context.TODO(), "Proxy server(http) listen on", mlog.Int("port", l.httpListener.Port()))
	return nil
}

type listenerManager struct {
	externalGrpcListener *netutil.NetListener
	internalGrpcListener *netutil.NetListener

	portShareMode bool
	// portShareMode == true
	cmux                    cmux.CMux
	cmuxClosed              chan struct{}
	cmuxExternHTTP2Listener net.Listener
	cmuxExternHTTPListener  net.Listener

	// portShareMode == false
	httpListener *netutil.NetListener
}

func (l *listenerManager) ExternalGrpcListener() net.Listener {
	if l.portShareMode {
		return l.cmuxExternHTTP2Listener
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

func (l *listenerManager) HTTP2Listener() net.Listener {
	if l.portShareMode {
		return l.cmuxExternHTTP2Listener
	}
	return nil
}

func (l *listenerManager) Close() {
	log := mlog.With()
	if l.portShareMode {
		if l.cmux != nil {
			mlog.Info(context.TODO(), "Proxy close cmux http2 listener")
			if err := l.cmuxExternHTTP2Listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				mlog.Warn(context.TODO(), "Proxy failed to close cmux http2 listener", mlog.Err(err))
			}
			mlog.Info(context.TODO(), "Proxy close cmux http listener")
			if err := l.cmuxExternHTTPListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				mlog.Warn(context.TODO(), "Proxy failed to close cmux http listener", mlog.Err(err))
			}
			mlog.Info(context.TODO(), "Proxy close cmux...")
			l.cmux.Close()
			<-l.cmuxClosed
			mlog.Info(context.TODO(), "Proxy cmux closed")
		}
	} else {
		if l.httpListener != nil {
			mlog.Info(context.TODO(), "Proxy close http listener", mlog.String("address", l.httpListener.Address()))
			if err := l.httpListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
				mlog.Warn(context.TODO(), "Proxy failed to close http listener", mlog.Err(err))
			}
		}
	}

	if l.internalGrpcListener != nil {
		mlog.Info(context.TODO(), "Proxy close internal grpc listener", mlog.String("address", l.internalGrpcListener.Address()))
		if err := l.internalGrpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			mlog.Warn(context.TODO(), "Proxy failed to close internal grpc listener", mlog.Err(err))
		}
	}

	if l.externalGrpcListener != nil {
		mlog.Info(context.TODO(), "Proxy close external grpc listener", mlog.String("address", l.externalGrpcListener.Address()))
		if err := l.externalGrpcListener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
			mlog.Warn(context.TODO(), "Proxy failed to close external grpc listener", mlog.Err(err))
		}
	}
}
