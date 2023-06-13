package milvus

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/roles"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	RunCmd      = "run"
	roleMixture = "mixture"
)

type run struct {
	serverType string
	// flags
	svrAlias         string
	enableRootCoord  bool
	enableQueryCoord bool
	enableDataCoord  bool
	enableIndexCoord bool
	enableQueryNode  bool
	enableDataNode   bool
	enableIndexNode  bool
	enableProxy      bool
}

func (c *run) getHelp() string {
	return runLine + "\n" + serverTypeLine
}

func (c *run) execute(args []string, flags *flag.FlagSet) {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, c.getHelp())
		return
	}
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, c.getHelp())
	}
	c.serverType = args[2]
	c.formatFlags(args, flags)

	// make go ignore SIGPIPE when all cgo threads set mask of SIGPIPE
	signal.Ignore(syscall.SIGPIPE)

	var local = false
	role := roles.MilvusRoles{}
	switch c.serverType {
	case typeutil.RootCoordRole:
		role.EnableRootCoord = true
	case typeutil.ProxyRole:
		role.EnableProxy = true
	case typeutil.QueryCoordRole:
		role.EnableQueryCoord = true
	case typeutil.QueryNodeRole:
		role.EnableQueryNode = true
	case typeutil.DataCoordRole:
		role.EnableDataCoord = true
	case typeutil.DataNodeRole:
		role.EnableDataNode = true
	case typeutil.IndexCoordRole:
		role.EnableIndexCoord = true
	case typeutil.IndexNodeRole:
		role.EnableIndexNode = true
	case typeutil.StandaloneRole, typeutil.EmbeddedRole:
		role.EnableRootCoord = true
		role.EnableProxy = true
		role.EnableQueryCoord = true
		role.EnableQueryNode = true
		role.EnableDataCoord = true
		role.EnableDataNode = true
		role.EnableIndexCoord = true
		role.EnableIndexNode = true
		local = true
	case roleMixture:
		role.EnableRootCoord = c.enableRootCoord
		role.EnableQueryCoord = c.enableQueryCoord
		role.EnableDataCoord = c.enableDataCoord
		role.EnableIndexCoord = c.enableIndexCoord
		role.EnableQueryNode = c.enableQueryNode
		role.EnableDataNode = c.enableDataNode
		role.EnableIndexNode = c.enableIndexNode
		role.EnableProxy = c.enableProxy
	default:
		fmt.Fprintf(os.Stderr, "Unknown server type = %s\n%s", c.serverType, c.getHelp())
		os.Exit(-1)
	}

	// setup config for embedded milvus
	if c.serverType == typeutil.EmbeddedRole {
		var params paramtable.BaseTable
		params.GlobalInitWithYaml("embedded-milvus.yaml")
	}

	runtimeDir := createRuntimeDir(c.serverType)
	filename := getPidFileName(c.serverType, c.svrAlias)

	c.printBanner(flags.Output())
	c.injectVariablesToEnv()
	lock, err := createPidFile(flags.Output(), filename, runtimeDir)
	if err != nil {
		panic(err)
	}
	defer removePidFile(lock)
	role.Run(local, c.svrAlias)
}

func (c *run) formatFlags(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.svrAlias, "alias", "", "set alias")

	flags.BoolVar(&c.enableRootCoord, typeutil.RootCoordRole, false, "enable root coordinator")
	flags.BoolVar(&c.enableQueryCoord, typeutil.QueryCoordRole, false, "enable query coordinator")
	flags.BoolVar(&c.enableIndexCoord, typeutil.IndexCoordRole, false, "enable index coordinator")
	flags.BoolVar(&c.enableDataCoord, typeutil.DataCoordRole, false, "enable data coordinator")

	flags.BoolVar(&c.enableQueryNode, typeutil.QueryNodeRole, false, "enable query node")
	flags.BoolVar(&c.enableDataNode, typeutil.DataNodeRole, false, "enable data node")
	flags.BoolVar(&c.enableIndexNode, typeutil.IndexNodeRole, false, "enable index node")
	flags.BoolVar(&c.enableProxy, typeutil.ProxyRole, false, "enable proxy node")

	if c.serverType == typeutil.EmbeddedRole {
		flags.SetOutput(io.Discard)
	}
	hardware.InitMaxprocs(c.serverType, flags)
	if err := flags.Parse(args[3:]); err != nil {
		os.Exit(-1)
	}
}

func (c *run) printBanner(w io.Writer) {
	fmt.Fprintln(w)
	fmt.Fprintln(w, "    __  _________ _   ____  ______    ")
	fmt.Fprintln(w, "   /  |/  /  _/ /| | / / / / / __/    ")
	fmt.Fprintln(w, "  / /|_/ // // /_| |/ / /_/ /\\ \\    ")
	fmt.Fprintln(w, " /_/  /_/___/____/___/\\____/___/     ")
	fmt.Fprintln(w)
	fmt.Fprintln(w, "Welcome to use Milvus!")
	fmt.Fprintln(w, "Version:   "+BuildTags)
	fmt.Fprintln(w, "Built:     "+BuildTime)
	fmt.Fprintln(w, "GitCommit: "+GitCommit)
	fmt.Fprintln(w, "GoVersion: "+GoVersion)
	fmt.Fprintln(w)
}

func (c *run) injectVariablesToEnv() {
	// inject in need

	var err error

	err = os.Setenv(metricsinfo.GitCommitEnvKey, GitCommit)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.GitCommitEnvKey),
			zap.Error(err))
	}

	err = os.Setenv(metricsinfo.GitBuildTagsEnvKey, BuildTags)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.GitBuildTagsEnvKey),
			zap.Error(err))
	}

	err = os.Setenv(metricsinfo.MilvusBuildTimeEnvKey, BuildTime)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.MilvusBuildTimeEnvKey),
			zap.Error(err))
	}

	err = os.Setenv(metricsinfo.MilvusUsedGoVersion, GoVersion)
	if err != nil {
		log.Warn(fmt.Sprintf("failed to inject %s to environment variable", metricsinfo.MilvusUsedGoVersion),
			zap.Error(err))
	}
}
