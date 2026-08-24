package utils

import (
	"context"
	"crypto/x509"
	"os"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func GracefulStopGRPCServer(s *grpc.Server) {
	if s == nil {
		return
	}
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		mlog.Info(context.TODO(), "try to graceful stop grpc server...")
		// will block until all rpc finished.
		s.GracefulStop()
	}()
	select {
	case <-ch:
	case <-time.After(paramtable.Get().ProxyGrpcServerCfg.GracefulStopTimeout.GetAsDuration(time.Second)):
		// took too long, manually close grpc server
		mlog.Info(context.TODO(), "force to stop grpc server...")
		s.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

func getTLSCreds(certFile string, keyFile string, nodeType string) credentials.TransportCredentials {
	mlog.Info(context.TODO(), "TLS Server PEM Path", mlog.String("path", certFile))
	mlog.Info(context.TODO(), "TLS Server Key Path", mlog.String("path", keyFile))
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		mlog.Warn(context.TODO(), nodeType+" can't create creds", mlog.Err(err))
		mlog.Warn(context.TODO(), nodeType+" can't create creds", mlog.Err(err))
	}
	return creds
}

func EnableInternalTLS(NodeType string) grpc.ServerOption {
	Params := paramtable.Get()
	certFile := Params.InternalTLSCfg.InternalTLSServerPemPath.GetValue()
	keyFile := Params.InternalTLSCfg.InternalTLSServerKeyPath.GetValue()
	internaltlsEnabled := Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool()

	mlog.Info(context.TODO(), "Internal TLS Enabled", mlog.Bool("value", internaltlsEnabled))

	if internaltlsEnabled {
		creds := getTLSCreds(certFile, keyFile, NodeType)
		return grpc.Creds(creds)
	}
	return grpc.Creds(nil)
}

func CreateCertPoolforClient(caFile string, nodeType string) (*x509.CertPool, error) {
	mlog.Info(context.TODO(), "Creating cert pool for "+nodeType)
	mlog.Info(context.TODO(), "Cert file path:", mlog.String("caFile", caFile))
	certPool := x509.NewCertPool()

	b, err := os.ReadFile(caFile)
	if err != nil {
		mlog.Error(context.TODO(), "Error reading cert file in client", mlog.Err(err))
		return nil, err
	}

	if !certPool.AppendCertsFromPEM(b) {
		mlog.Error(context.TODO(), "credentials: failed to append certificates")
		return nil, merr.WrapErrParameterInvalidMsg("failed to append certificates") // Cert pool is invalid, return nil and the error
	}
	return certPool, err
}
