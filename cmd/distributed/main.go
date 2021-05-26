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
	"io/ioutil"
	"os"
	"path"
	"syscall"

	"github.com/milvus-io/milvus/cmd/distributed/roles"
)

func createPidFile(service string, alias string, runtimeDir string) (*os.File, error) {
	var fileName string
	if len(alias) != 0 {
		fileName = fmt.Sprintf("%s-%s.pid", service, alias)
	} else {
		fileName = service + ".pid"
	}

	fileFullName := path.Join(runtimeDir, fileName)

	fd, err := os.OpenFile(fileFullName, os.O_CREATE|os.O_RDWR, 0664)
	if err != nil {
		return nil, fmt.Errorf("service %s is running, error  = %w", service, err)
	}
	fmt.Println("open pid file: ", fileFullName)

	err = syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		return nil, fmt.Errorf("service %s is running, error = %w", service, err)
	}
	fmt.Println("lock pid file: ", fileFullName)

	fd.Truncate(0)
	_, err = fd.WriteString(fmt.Sprintf("%d", os.Getpid()))

	return fd, err
}

func removePidFile(fd *os.File) {
	_ = syscall.Close(int(fd.Fd()))
	_ = os.Remove(fd.Name())
}

func run(service string, alias string, runTimeDir string) error {
	fd, err := createPidFile(service, alias, runTimeDir)
	if err != nil {
		return fmt.Errorf("create pid file fail, service %s", service)
	}
	defer removePidFile(fd)

	role := roles.MilvusRoles{}
	switch service {
	case "master":
		role.EnableMaster = true
	case "msgstream":
		role.EnableMsgStreamService = true
	case "proxyservice":
		role.EnableProxyService = true
	case "proxynode":
		role.EnableProxyNode = true
	case "queryservice":
		role.EnableQueryService = true
	case "querynode":
		role.EnableQueryNode = true
	case "dataservice":
		role.EnableDataService = true
	case "datanode":
		role.EnableDataNode = true
	case "indexservice":
		role.EnableIndexService = true
	case "indexnode":
		role.EnableIndexNode = true
	default:
		return fmt.Errorf("unknown service = %s", service)
	}
	role.Run(false)
	return nil
}

func stop(service string, alias string, runTimeDir string) error {
	var fileName string
	if len(alias) != 0 {
		fileName = fmt.Sprintf("%s-%s.pid", service, alias)
	} else {
		fileName = service + ".pid"
	}
	var err error
	var fd *os.File
	if fd, err = os.OpenFile(path.Join(runTimeDir, fileName), os.O_RDONLY, 0664); err != nil {
		return err
	}
	defer func() {
		_ = fd.Close()
	}()
	var pid int
	_, err = fmt.Fscanf(fd, "%d", &pid)
	if err != nil {
		return err
	}
	p, err := os.FindProcess(pid)
	if err != nil {
		return err
	}
	err = p.Signal(syscall.SIGTERM)
	if err != nil {
		return err
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
		return fmt.Errorf("%s is exist, but is not directory", dir)
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

	if err := flags.Parse(os.Args[3:]); err != nil {
		os.Exit(-1)
	}

	runtimeDir := "/run/milvus"
	if err := makeRuntimeDir(runtimeDir); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set runtime dir at : %s failed, set it to /tmp/milvus directory\n", runtimeDir)
		runtimeDir = "/tmp/milvus"
		if err = makeRuntimeDir(runtimeDir); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create runtime directory at : %s failed\n", runtimeDir)
			os.Exit(-1)
		}
	}

	switch command {
	case "run":
		if err := run(serverType, svrAlias, runtimeDir); err != nil {
			panic(err)
		}
	case "stop":
		if err := stop(serverType, svrAlias, runtimeDir); err != nil {
			panic(err)
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown command : %s", command)
	}
}
