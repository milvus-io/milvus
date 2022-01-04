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

package main

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"syscall"

	// use auto max procs to set container CPU quota
	_ "go.uber.org/automaxprocs"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/roles"
	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
)

const (
	roleMixture = "mixture"
)

// inject variable at build-time
var (
	BuildTags = "unknown"
	BuildTime = "unknown"
	GitCommit = "unknown"
	GoVersion = "unknown"
)

// conf dir path for milvus
var milvusConf = ""

func printBanner() {
	fmt.Println()
	fmt.Println("    __  _________ _   ____  ______    ")
	fmt.Println("   /  |/  /  _/ /| | / / / / / __/    ")
	fmt.Println("  / /|_/ // // /_| |/ / /_/ /\\ \\    ")
	fmt.Println(" /_/  /_/___/____/___/\\____/___/     ")
	fmt.Println()
	fmt.Println("Welcome to use Milvus!")
	fmt.Println("Version:   " + BuildTags)
	fmt.Println("Built:     " + BuildTime)
	fmt.Println("GitCommit: " + GitCommit)
	fmt.Println("GoVersion: " + GoVersion)
	fmt.Println()
}

func injectVariablesToEnv() {
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

	// the `MILVUSCONF` will be overwritten if --config is provided
	if milvusConf != "" {
		err = os.Setenv(paramtable.MilvusConfEnvKey, milvusConf)
		if err != nil {
			log.Warn(fmt.Sprintf("failed to inject %s to environment variable", paramtable.MilvusConfEnvKey),
				zap.Error(err))
		}
	}
}

func getPidFileName(serverType string, alias string) string {
	var filename string
	if len(alias) != 0 {
		filename = fmt.Sprintf("%s-%s.pid", serverType, alias)
	} else {
		filename = serverType + ".pid"
	}
	return filename
}

func createPidFile(filename string, runtimeDir string) (*os.File, error) {
	fileFullName := path.Join(runtimeDir, filename)

	fd, err := os.OpenFile(fileFullName, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, fmt.Errorf("file %s is locked, error = %w", filename, err)
	}
	fmt.Println("open pid file:", fileFullName)

	err = syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return nil, fmt.Errorf("file %s is locked, error = %w", filename, err)
	}
	fmt.Println("lock pid file:", fileFullName)

	fd.Truncate(0)
	_, err = fd.WriteString(fmt.Sprintf("%d", os.Getpid()))
	if err != nil {
		return nil, fmt.Errorf("file %s write fail, error = %w", filename, err)
	}

	return fd, nil
}

func closePidFile(fd *os.File) {
	fd.Close()
}

func removePidFile(fd *os.File) {
	syscall.Close(int(fd.Fd()))
	os.Remove(fd.Name())
}

func stopPid(filename string, runtimeDir string) error {
	var pid int

	fd, err := os.OpenFile(path.Join(runtimeDir, filename), os.O_RDONLY, 0664)
	if err != nil {
		return err
	}
	defer closePidFile(fd)

	if _, err = fmt.Fscanf(fd, "%d", &pid); err != nil {
		return err
	}

	if process, err := os.FindProcess(pid); err == nil {
		return process.Signal(syscall.SIGTERM)
	}
	return nil
}

func makeRuntimeDir(dir string) error {
	perm := os.FileMode(0755)
	// os.MkdirAll equal to `mkdir -p`
	err := os.MkdirAll(dir, perm)
	if err != nil {
		// err will be raised only when dir exists and dir is a file instead of a directory.
		return fmt.Errorf("create runtime dir %s failed, err: %s", dir, err.Error())
	}

	tmpFile, err := ioutil.TempFile(dir, "tmp")
	if err != nil {
		return err
	}
	fileName := tmpFile.Name()
	tmpFile.Close()
	os.Remove(fileName)
	return nil
}

// simplified print from flag package
func printUsage(w io.Writer, f *flag.Flag) {
	s := fmt.Sprintf("  -%s", f.Name) // Two spaces before -; see next two comments.
	name, usage := flag.UnquoteUsage(f)
	if len(name) > 0 {
		s += " " + name
	}
	// Boolean flags of one ASCII letter are so common we
	// treat them specially, putting their usage on the same line.
	if len(s) <= 4 { // space, space, '-', 'x'.
		s += "\t"
	} else {
		// Four spaces before the tab triggers good alignment
		// for both 4- and 8-space tab stops.
		s += "\n    \t"
	}
	s += strings.ReplaceAll(usage, "\n", "\n    \t")

	fmt.Fprint(w, s, "\n")
}

func main() {
	if len(os.Args) < 3 {
		_, _ = fmt.Fprint(os.Stderr, "usage: milvus [command] [server type] [flags]\n")
		return
	}
	command := os.Args[1]
	serverType := os.Args[2]
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)

	var svrAlias string
	flags.StringVar(&svrAlias, "alias", "", "set alias")
	flags.StringVar(&milvusConf, "config", "", "set conf path")

	var enableRootCoord, enableQueryCoord, enableIndexCoord, enableDataCoord bool
	flags.BoolVar(&enableRootCoord, typeutil.RootCoordRole, false, "enable root coordinator")
	flags.BoolVar(&enableQueryCoord, typeutil.QueryCoordRole, false, "enable query coordinator")
	flags.BoolVar(&enableIndexCoord, typeutil.IndexCoordRole, false, "enable index coordinator")
	flags.BoolVar(&enableDataCoord, typeutil.DataCoordRole, false, "enable data coordinator")

	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage of %s:\n", os.Args[0])
		switch {
		case serverType == roleMixture:
			flags.VisitAll(func(f *flag.Flag) {
				printUsage(flags.Output(), f)
			})
		default:
			flags.VisitAll(func(f *flag.Flag) {
				if f.Name != "alias" {
					return
				}
				printUsage(flags.Output(), f)
			})
		}
	}

	if err := flags.Parse(os.Args[3:]); err != nil {
		os.Exit(-1)
	}

	var local = false
	role := roles.MilvusRoles{}
	switch serverType {
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
	case typeutil.StandaloneRole:
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
		role.EnableRootCoord = enableRootCoord
		role.EnableQueryCoord = enableQueryCoord
		role.EnableDataCoord = enableDataCoord
		role.EnableIndexCoord = enableIndexCoord
	default:
		fmt.Fprintf(os.Stderr, "Unknown server type = %s\n", serverType)
		os.Exit(-1)
	}

	runtimeDir := "/run/milvus"
	if err := makeRuntimeDir(runtimeDir); err != nil {
		fmt.Fprintf(os.Stderr, "Set runtime dir at %s failed, set it to /tmp/milvus directory\n", runtimeDir)
		runtimeDir = "/tmp/milvus"
		if err = makeRuntimeDir(runtimeDir); err != nil {
			fmt.Fprintf(os.Stderr, "Create runtime directory at %s failed\n", runtimeDir)
			os.Exit(-1)
		}
	}

	filename := getPidFileName(serverType, svrAlias)
	switch command {
	case "run":
		printBanner()
		injectVariablesToEnv()
		fd, err := createPidFile(filename, runtimeDir)
		if err != nil {
			panic(err)
		}
		defer removePidFile(fd)
		role.Run(local, svrAlias)
	case "stop":
		if err := stopPid(filename, runtimeDir); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n\n", err.Error())
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command : %s\n", command)
	}
}
