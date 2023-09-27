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
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	RunCmd      = "run"
	roleMixture = "mixture"
)

type run struct {
	serverType string
	// flags
	svrAlias string
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

	role := roles.NewMilvusRoles()
	// init roles by serverType and flags
	err := role.Init(c.serverType, flags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n%s", err.Error(), c.getHelp())
		os.Exit(-1)
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
	role.Run(c.svrAlias)
}

func (c *run) formatFlags(args []string, flags *flag.FlagSet) {
	flags.StringVar(&c.svrAlias, "alias", "", "set alias")

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
