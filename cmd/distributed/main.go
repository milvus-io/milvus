package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/go-kit/kit/log/level"
	"github.com/ilyakaznacheev/cleanenv"
	"github.com/oklog/run"
	"github.com/pkg/errors"
	"github.com/prometheus/common/promlog"
	grpcproxyservice "github.com/zilliztech/milvus-distributed/internal/distributed/proxyservice"
	"gopkg.in/alecthomas/kingpin.v2"
)

type MilvusRoles struct {
	EnableMaster           bool `env:"ENABLE_MASTER"`
	EnableProxyService     bool `env:"ENABLE_PROXY_SERVICE"`
	EnableProxyNode        bool `env:"ENABLE_PROXY_NODE"`
	EnableQueryService     bool `env:"ENABLE_QUERY_SERVICE"`
	EnableQueryNode        bool `env:"ENABLE_QUERY_NODE"`
	EnableDataService      bool `env:"ENABLE_DATA_SERVICE"`
	EnableDataNode         bool `env:"ENABLE_DATA_NODE"`
	EnableIndexService     bool `env:"ENABLE_INDEX_SERVICE"`
	EnableIndexNode        bool `env:"ENABLE_INDEX_NODE"`
	EnableMsgStreamService bool `env:"ENABLE_MSGSTREAM_SERVICE"`
}

func (mr *MilvusRoles) hasAnyRole() bool {
	return mr.EnableMaster || mr.EnableMsgStreamService ||
		mr.EnableProxyService || mr.EnableProxyNode ||
		mr.EnableQueryService || mr.EnableQueryNode ||
		mr.EnableDataService || mr.EnableDataNode ||
		mr.EnableIndexService || mr.EnableIndexNode
}

var roles MilvusRoles

func main() {
	a := kingpin.New(filepath.Base(os.Args[0]), "Milvus")

	a.HelpFlag.Short('h')

	a.Flag("master", "Run master service").Short('m').Default("false").BoolVar(&roles.EnableMaster)

	a.Flag("msgstream-service", "Run msgstream service").Short('M').Default("false").BoolVar(&roles.EnableMsgStreamService)

	a.Flag("proxy-service", "Run proxy service").Short('p').Default("false").BoolVar(&roles.EnableProxyService)

	a.Flag("proxy-node", "Run proxy node").Short('P').Default("false").BoolVar(&roles.EnableProxyNode)

	a.Flag("query-service", "Run query service").Short('q').Default("false").BoolVar(&roles.EnableQueryService)

	a.Flag("query-node", "Run query node").Short('Q').Default("false").BoolVar(&roles.EnableQueryNode)

	a.Flag("data-service", "Run data service").Short('d').Default("false").BoolVar(&roles.EnableDataService)

	a.Flag("data-node", "Run data node").Short('D').Default("false").BoolVar(&roles.EnableDataNode)

	a.Flag("index-service", "Run index service").Short('i').Default("false").BoolVar(&roles.EnableIndexService)

	a.Flag("index-node", "Run index node").Short('I').Default("false").BoolVar(&roles.EnableIndexNode)

	_, err := a.Parse(os.Args[1:])
	if err != nil {
		fmt.Fprintln(os.Stderr, errors.Wrapf(err, "Error parsing commandline arguments"))
		a.Usage(os.Args[1:])
		os.Exit(2)
	}

	if !roles.hasAnyRole() {
		err := cleanenv.ReadEnv(&roles)
		if err != nil {
			fmt.Println(err)
			os.Exit(-1)
		}
	}

	if !roles.hasAnyRole() {
		fmt.Println("Please select at least one service to start")
		os.Exit(-1)
	}

	logger := promlog.New(NewLogConfig())

	var (
		ctxProxyService, cancelProxyService = context.WithCancel(context.Background())
		proxyService                        = NewProxyService(ctxProxyService)
	)

	var g run.Group
	{
		// Termination handler.
		term := make(chan os.Signal, 1)
		signal.Notify(term, os.Interrupt, syscall.SIGTERM)
		cancel := make(chan struct{})
		g.Add(
			func() error {
				select {
				case <-term:
					level.Warn(logger).Log("msg", "Received SIGTERM, exiting gracefully...")
				case <-cancel:
				}
				return nil
			},
			func(err error) {
				close(cancel)
			},
		)
	}
	if roles.EnableProxyService {
		// ProxyService
		g.Add(
			func() error {
				err := proxyService.Run()
				level.Info(logger).Log("msg", "Proxy service stopped")
				return err
			},
			func(err error) {
				level.Info(logger).Log("msg", "Stopping proxy service...")
				cancelProxyService()
			},
		)
	}
	if roles.EnableProxyNode {
		// ProxyNode
	}

	if err := g.Run(); err != nil {
		level.Error(logger).Log("err", err)
		os.Exit(1)
	}
	level.Info(logger).Log("msg", "See you next time!")
}

func NewLogConfig() *promlog.Config {
	logConfig := promlog.Config{
		Level:  &promlog.AllowedLevel{},
		Format: &promlog.AllowedFormat{},
	}
	err := logConfig.Level.Set("debug")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	err = logConfig.Format.Set("logfmt")
	if err != nil {
		fmt.Println(err)
		os.Exit(-1)
	}

	return &logConfig
}

// Move to proxyservice package
func NewProxyService(ctx context.Context) *ProxyService {
	srv, _ := grpcproxyservice.NewServer(ctx)
	ps := &ProxyService{ctx: ctx, server: srv}
	return ps
}

type ProxyService struct {
	ctx    context.Context
	server *grpcproxyservice.Server
}

func (ps *ProxyService) Run() error {
	return ps.server.Run()
}
