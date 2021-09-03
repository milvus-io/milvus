// Copyright (C) 2019-2020 Zilliz. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software distributed under the License
// is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
// or implied. See the License for the specific language governing permissions and limitations under the License.

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

	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"

	"github.com/milvus-io/milvus/internal/util/metricsinfo"

	"github.com/milvus-io/milvus/cmd/roles"
)

const (
	roleRootCoord  = "rootcoord"
	roleQueryCoord = "querycoord"
	roleIndexCoord = "indexcoord"
	roleDataCoord  = "datacoord"
	roleProxy      = "proxy"
	roleQueryNode  = "querynode"
	roleIndexNode  = "indexnode"
	roleDataNode   = "datanode"
	roleMixture    = "mixture"
	roleStandalone = "standalone"
)

// inject variable at build-time
var (
	BuildTags = "unknown"
	BuildTime = "unknown"
	GitCommit = "unknown"
	GoVersion = "unknown"
)

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

	err := os.Setenv(metricsinfo.GitCommitEnvKey, GitCommit)
	if err != nil {
		log.Warn("failed to inject git commit to environment variable",
			zap.Error(err))
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
	st, err := os.Stat(dir)
	if os.IsNotExist(err) {
		err = os.Mkdir(dir, 0755)
		if err != nil {
			return fmt.Errorf("create runtime dir %s failed", dir)
		}
		return nil
	}
	if !st.IsDir() {
		return fmt.Errorf("%s is not directory", dir)
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

	var enableRootCoord, enableQueryCoord, enableIndexCoord, enableDataCoord bool
	flags.BoolVar(&enableRootCoord, roleRootCoord, false, "enable root coordinator")
	flags.BoolVar(&enableQueryCoord, roleQueryCoord, false, "enable query coordinator")
	flags.BoolVar(&enableIndexCoord, roleIndexCoord, false, "enable index coordinator")
	flags.BoolVar(&enableDataCoord, roleDataCoord, false, "enable data coordinator")

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

	var localMsg = false
	role := roles.MilvusRoles{}
	switch serverType {
	case roleRootCoord:
		role.EnableRootCoord = true
	case roleProxy:
		role.EnableProxy = true
	case roleQueryCoord:
		role.EnableQueryCoord = true
	case roleQueryNode:
		role.EnableQueryNode = true
	case roleDataCoord:
		role.EnableDataCoord = true
	case roleDataNode:
		role.EnableDataNode = true
	case roleIndexCoord:
		role.EnableIndexCoord = true
	case roleIndexNode:
		role.EnableIndexNode = true
	case roleMixture:
		role.EnableRootCoord = enableRootCoord
		role.EnableQueryCoord = enableQueryCoord
		role.EnableDataCoord = enableDataCoord
		role.EnableIndexCoord = enableIndexCoord
	case roleStandalone:
		role.EnableRootCoord = true
		role.EnableProxy = true
		role.EnableQueryCoord = true
		role.EnableQueryNode = true
		role.EnableDataCoord = true
		role.EnableDataNode = true
		role.EnableIndexCoord = true
		role.EnableIndexNode = true
		role.EnableMsgStreamCoord = true
		localMsg = true
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
		role.Run(localMsg, svrAlias)
	case "stop":
		if err := stopPid(filename, runtimeDir); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n\n", err.Error())
		}
	default:
		fmt.Fprintf(os.Stderr, "unknown command : %s\n", command)
	}
}
