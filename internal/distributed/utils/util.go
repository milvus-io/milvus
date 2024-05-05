package utils

import (
	"crypto/x509"
	"os"
	"strings"
	"time"

	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func GracefulStopGRPCServer(s *grpc.Server) {
	if s == nil {
		return
	}
	ch := make(chan struct{})
	go func() {
		defer close(ch)
		log.Debug("try to graceful stop grpc server...")
		// will block until all rpc finished.
		s.GracefulStop()
	}()
	select {
	case <-ch:
	case <-time.After(paramtable.Get().ProxyGrpcServerCfg.GracefulStopTimeout.GetAsDuration(time.Second)):
		// took too long, manually close grpc server
		log.Debug("stop grpc server...")
		s.Stop()
		// concurrent GracefulStop should be interrupted
		<-ch
	}
}

func getTLSCreds(certFile string, keyFile string, nodeType string) credentials.TransportCredentials {
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
	var Params *paramtable.ComponentParam = paramtable.Get()
	var serverCfg *paramtable.GrpcServerConfig
	switch strings.ToLower(NodeType) {
	case "datacoord":
		serverCfg = &Params.DataCoordGrpcServerCfg
	case "datanode":
		serverCfg = &Params.DataNodeGrpcServerCfg
	case "indexnode":
		serverCfg = &Params.IndexNodeGrpcServerCfg
	case "proxy":
		serverCfg = &Params.ProxyGrpcServerCfg
	case "querycoord":
		serverCfg = &Params.QueryCoordGrpcServerCfg
	case "querynode":
		serverCfg = &Params.QueryNodeGrpcServerCfg
	case "rootcoord":
		serverCfg = &Params.RootCoordGrpcServerCfg
	default:
		log.Error("Unknown NodeType")
		return grpc.Creds(nil)
	}
	certFile := serverCfg.InternalTLSServerPemPath.GetValue()
	keyFile := serverCfg.InternalTLSServerKeyPath.GetValue()
	internaltlsEnabled := serverCfg.InternalTLSEnabled.GetAsBool()

	log.Info("internal TLS Enabled", zap.Bool("value", internaltlsEnabled))

	if internaltlsEnabled {
		creds := getTLSCreds(certFile, keyFile, NodeType)
		return grpc.Creds(creds)
	}
	return grpc.Creds(nil)
}

func CreateCertPoolforClient(caFile string, nodeType string) *x509.CertPool {
	log.Info("Creating cert pool for " + nodeType)
	b, err := os.ReadFile(caFile)
	if err != nil {
		log.Error("Error reading cert file in client", zap.Error(err))
	}
	cp := x509.NewCertPool()
	if !cp.AppendCertsFromPEM(b) {
		log.Error("credentials: failed to append certificates")
	}
	return cp
}
