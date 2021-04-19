package main

import (
	"flag"
	"fmt"
	"os"
	"path"
	"syscall"

	"github.com/zilliztech/milvus-distributed/cmd/distributed/roles"
)

func run(serverType, runtTimeDir string) error {
	fileName := serverType + ".pid"
	_, err := os.Stat(path.Join(runtTimeDir, fileName))
	var fd *os.File
	if os.IsNotExist(err) {
		if fd, err = os.OpenFile(path.Join(runtTimeDir, fileName), os.O_CREATE|os.O_WRONLY, 0664); err != nil {
			return err
		}
		defer func() {
			_ = syscall.Close(int(fd.Fd()))
			_ = os.Remove(path.Join(runtTimeDir, fileName))
		}()

		if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return err
		}
		_, _ = fd.WriteString(fmt.Sprintf("%d", os.Getpid()))

	} else {
		if fd, err = os.OpenFile(path.Join(runtTimeDir, fileName), os.O_WRONLY, 0664); err != nil {
			return fmt.Errorf("service %s is running, error  = %w", serverType, err)
		}
		if err := syscall.Flock(int(fd.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
			return fmt.Errorf("service %s is running, error = %w", serverType, err)
		}
		defer func() {
			_ = syscall.Close(int(fd.Fd()))
			_ = os.Remove(path.Join(runtTimeDir, fileName))
		}()
		fd.Truncate(0)
		_, _ = fd.WriteString(fmt.Sprintf("%d", os.Getpid()))
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
		return fmt.Errorf("unknown server type = %s", serverType)
	}
	role.Run(false)
	return nil
}

func stop(serverType, runtimeDir string) error {
	fileName := serverType + ".pid"
	var err error
	var fd *os.File
	if fd, err = os.OpenFile(path.Join(runtimeDir, fileName), os.O_RDONLY, 0664); err != nil {
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
	tmpFile := path.Join(dir, "testTmp")

	var fd *os.File

	if fd, err = os.OpenFile(tmpFile, os.O_CREATE|os.O_WRONLY, 0664); err != nil {
		return err
	}
	syscall.Close(int(fd.Fd()))
	os.Remove(tmpFile)
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

	runtimeDir := "/run/milvus-distributed"
	if err := makeRuntimeDir(runtimeDir); err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "set runtime dir at : %s failed, set it to /tmp/milvus-distributed directory\n", runtimeDir)
		runtimeDir = "/tmp/milvus-distributed"
		if err = makeRuntimeDir(runtimeDir); err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "create runtime director at : %s failed\n", runtimeDir)
			os.Exit(-1)
		}
	}

	switch command {
	case "run":
		if err := run(serverType, runtimeDir); err != nil {
			panic(err)
		}
	case "stop":
		if err := stop(serverType, runtimeDir); err != nil {
			panic(err)
		}
	default:
		_, _ = fmt.Fprintf(os.Stderr, "unknown command : %s", command)
	}
}
