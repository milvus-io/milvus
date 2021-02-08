package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/roles"
	"github.com/zilliztech/milvus-distributed/internal/errors"
)

const (
	fileDir = "/run/milvus-distributed"
)

func run(serverType string) error {
	fileName := serverType + ".pid"
	_, err := os.Stat(path.Join(fileDir, fileName))
	var fd *os.File
	if os.IsNotExist(err) {
		if fd, err = os.OpenFile(path.Join(fileDir, fileName), os.O_CREATE|os.O_WRONLY, 0664); err != nil {
			return err
		}
		defer func() {
			_ = syscall.Close(int(fd.Fd()))
			_ = os.Remove(path.Join(fileDir, fileName))
		}()

		if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return err
		}
		_, _ = fd.WriteString(fmt.Sprintf("%d", os.Getpid()))

	} else {
		return errors.Errorf("service %s is running", serverType)
	}

	role := roles.MilvusRoles{}
	switch serverType {
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
	case "standalone":
		role.EnableStandalone = true
	default:
		return errors.Errorf("unknown server type = %s", serverType)
	}
	role.Run(false)
	return nil
}

func stop(serverType string) error {
	fileName := serverType + ".pid"
	var err error
	var fd *os.File
	if fd, err = os.OpenFile(path.Join(fileDir, fileName), os.O_RDONLY, 0664); err != nil {
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

func main() {
	if len(os.Args) < 3 {
		_, _ = fmt.Fprint(os.Stderr, "usage: milvus-distributed [command] [server type] [flags]\n")
		return
	}
	command := os.Args[1]
	serverType := os.Args[2]
	flags := flag.NewFlagSet(os.Args[0], flag.ExitOnError)
	//flags.BoolVar()

	if err := flags.Parse(os.Args[3:]); err != nil {
		os.Exit(-1)
	}

	if _, err := os.Stat(fileDir); os.IsNotExist(err) {
		_, _ = fmt.Fprintf(os.Stderr, "please create dirctory /run/milvus-distributed, and set owner to current user")
		os.Exit(-1)
	}
	switch command {
	case "run":
		if err := run(serverType); err != nil {
			panic(err)
		}
	case "stop":
		if err := stop(serverType); err != nil {
			panic(err)
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown command : %s", command)
	}
}
