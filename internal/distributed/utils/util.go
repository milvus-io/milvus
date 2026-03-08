package utils

import (
	"context"
	"crypto/x509"
	"os"
	"time"

	"github.com/cockroachdb/errors"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func GracefulStopGRPCServer(s *grpc.Server) {
	if s == nil {
		return
	}
	log := log.Ctx(context.TODO())
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		log.Info("try to graceful stop grpc server...")
		// will block until all rpc finished.
		s.GracefulStop()
	}()
	select {
	case <-ch:
	case <-time.After(paramtable.Get().ProxyGrpcServerCfg.GracefulStopTimeout.GetAsDuration(time.Second)):
		// took too long, manually close grpc server
		log.Info("force to stop grpc server...")
		s.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

func getTLSCreds(certFile string, keyFile string, nodeType string) credentials.TransportCredentials {
	log := log.Ctx(context.TODO())
	log.Info("TLS Server PEM Path", zap.String("path", certFile))
	log.Info("TLS Server Key Path", zap.String("path", keyFile))
	creds, err := credentials.NewServerTLSFromFile(certFile, keyFile)
	if err != nil {
		log.Warn(nodeType+" can't create creds", zap.Error(err))
		log.Warn(nodeType+" can't create creds", zap.Error(err))
	}
	return creds
}

func EnableInternalTLS(NodeType string) grpc.ServerOption {
	log := log.Ctx(context.TODO())
	var Params *paramtable.ComponentParam = paramtable.Get()
	certFile := Params.InternalTLSCfg.InternalTLSServerPemPath.GetValue()
	keyFile := Params.InternalTLSCfg.InternalTLSServerKeyPath.GetValue()
	internaltlsEnabled := Params.InternalTLSCfg.InternalTLSEnabled.GetAsBool()

	log.Info("Internal TLS Enabled", zap.Bool("value", internaltlsEnabled))

	if internaltlsEnabled {
		creds := getTLSCreds(certFile, keyFile, NodeType)
		return grpc.Creds(creds)
	}
	return grpc.Creds(nil)
}

func CreateCertPoolforClient(caFile string, nodeType string) (*x509.CertPool, error) {
	log := log.Ctx(context.TODO())
	log.Info("Creating cert pool for " + nodeType)
	log.Info("Cert file path:", zap.String("caFile", caFile))
	certPool := x509.NewCertPool()

	b, err := os.ReadFile(caFile)
	if err != nil {
		log.Error("Error reading cert file in client", zap.Error(err))
		return nil, err
	}

	if !certPool.AppendCertsFromPEM(b) {
		log.Error("credentials: failed to append certificates")
		return nil, errors.New("failed to append certificates") // Cert pool is invalid, return nil and the error
	}
	return certPool, err
}
