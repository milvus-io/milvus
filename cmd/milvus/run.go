package milvus

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/metrics"
	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/metricsinfo"
)

type run struct{}

func (c *run) execute(args []string, flags *flag.FlagSet) {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, getHelp())
		return
	}
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, getHelp())
	}
	// make go ignore SIGPIPE when all cgo thread set mask SIGPIPE
	signal.Ignore(syscall.SIGPIPE)

	serverType := args[2]
	roles := GetMilvusRoles(args, flags)
	// setup config for embedded milvus

	runtimeDir := createRuntimeDir(serverType)
	filename := getPidFileName(serverType, roles.Alias)

	c.printBanner(flags.Output())
	c.injectVariablesToEnv()
	c.printHardwareInfo(flags.Output())
	lock, err := createPidFile(flags.Output(), filename, runtimeDir)
	if err != nil {
		panic(err)
	}
	defer removePidFile(lock)
	roles.Run()
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
	metrics.BuildInfo.WithLabelValues(BuildTags, BuildTime, GitCommit).Set(1)
}

func (c *run) printHardwareInfo(w io.Writer) {
	totalMem := hardware.GetMemoryCount()
	usedMem := hardware.GetUsedMemoryCount()
	fmt.Fprintf(w, "TotalMem: %d\n", totalMem)
	fmt.Fprintf(w, "UsedMem: %d\n", usedMem)
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
