package milvus

import (
	"context"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/gofrs/flock"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/cmd/roles"
	"github.com/milvus-io/milvus/internal/util/sessionutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/etcd"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func makeRuntimeDir(dir string) error {
	perm := os.FileMode(0o755)
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

// create runtime folder
func createRuntimeDir(sType string) string {
	var writer io.Writer
	if sType == typeutil.EmbeddedRole {
		writer = io.Discard
	} else {
		writer = os.Stderr
	}
	runtimeDir := "/run/milvus"
	if runtime.GOOS == "windows" {
		runtimeDir = "run"
		if err := makeRuntimeDir(runtimeDir); err != nil {
			fmt.Fprintf(writer, "Create runtime directory at %s failed\n", runtimeDir)
			os.Exit(-1)
		}
	} else {
		if err := makeRuntimeDir(runtimeDir); err != nil {
			fmt.Fprintf(writer, "Set runtime dir at %s failed, set it to /tmp/milvus directory\n", runtimeDir)
			runtimeDir = "/tmp/milvus"
			if err = makeRuntimeDir(runtimeDir); err != nil {
				fmt.Fprintf(writer, "Create runtime directory at %s failed\n", runtimeDir)
				os.Exit(-1)
			}
		}
	}
	return runtimeDir
}

func createPidFile(w io.Writer, filename string, runtimeDir string) (*flock.Flock, error) {
	fileFullName := path.Join(runtimeDir, filename)

	fd, err := os.OpenFile(fileFullName, os.O_CREATE|os.O_RDWR, 0o664)
	if err != nil {
		return nil, fmt.Errorf("file %s is locked, error = %w", filename, err)
	}
	fmt.Fprintln(w, "open pid file:", fileFullName)

	defer fd.Close()

	fd.Truncate(0)
	_, err = fd.WriteString(fmt.Sprintf("%d", os.Getpid()))
	if err != nil {
		return nil, fmt.Errorf("file %s write fail, error = %w", filename, err)
	}

	lock := flock.New(fileFullName)
	_, err = lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("file %s is locked, error = %w", filename, err)
	}

	fmt.Fprintln(w, "lock pid file:", fileFullName)
	return lock, nil
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

func closePidFile(fd *os.File) {
	fd.Close()
}

func removePidFile(lock *flock.Flock) {
	filename := lock.Path()
	lock.Close()
	os.Remove(filename)
}

func GetMilvusRoles(args []string, flags *flag.FlagSet) *roles.MilvusRoles {
	alias, enableRootCoord, enableQueryCoord, enableIndexCoord, enableDataCoord, enableQueryNode,
		enableDataNode, enableIndexNode, enableProxy := formatFlags(args, flags)

	serverType := args[2]
	role := roles.NewMilvusRoles()
	role.Alias = alias

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
	case typeutil.StandaloneRole, typeutil.EmbeddedRole:
		role.EnableRootCoord = true
		role.EnableProxy = true
		role.EnableQueryCoord = true
		role.EnableQueryNode = true
		role.EnableDataCoord = true
		role.EnableDataNode = true
		role.EnableIndexCoord = true
		role.EnableIndexNode = true
		role.Local = true
		role.Embedded = serverType == typeutil.EmbeddedRole
	case RoleMixture:
		role.EnableRootCoord = enableRootCoord
		role.EnableQueryCoord = enableQueryCoord
		role.EnableDataCoord = enableDataCoord
		role.EnableIndexCoord = enableIndexCoord
		role.EnableQueryNode = enableQueryNode
		role.EnableDataNode = enableDataNode
		role.EnableIndexNode = enableIndexNode
		role.EnableProxy = enableProxy
	default:
		fmt.Fprintf(os.Stderr, "Unknown server type = %s\n%s", serverType, getHelp())
		os.Exit(-1)
	}

	return role
}

func formatFlags(args []string, flags *flag.FlagSet) (alias string, enableRootCoord, enableQueryCoord,
	enableIndexCoord, enableDataCoord, enableQueryNode, enableDataNode, enableIndexNode, enableProxy bool,
) {
	flags.StringVar(&alias, "alias", "", "set alias")

	flags.BoolVar(&enableRootCoord, typeutil.RootCoordRole, false, "enable root coordinator")
	flags.BoolVar(&enableQueryCoord, typeutil.QueryCoordRole, false, "enable query coordinator")
	flags.BoolVar(&enableIndexCoord, typeutil.IndexCoordRole, false, "enable index coordinator")
	flags.BoolVar(&enableDataCoord, typeutil.DataCoordRole, false, "enable data coordinator")

	flags.BoolVar(&enableQueryNode, typeutil.QueryNodeRole, false, "enable query node")
	flags.BoolVar(&enableDataNode, typeutil.DataNodeRole, false, "enable data node")
	flags.BoolVar(&enableIndexNode, typeutil.IndexNodeRole, false, "enable index node")
	flags.BoolVar(&enableProxy, typeutil.ProxyRole, false, "enable proxy node")

	serverType := args[2]
	if serverType == typeutil.EmbeddedRole {
		flags.SetOutput(io.Discard)
	}
	hardware.InitMaxprocs(serverType, flags)
	if err := flags.Parse(args[3:]); err != nil {
		os.Exit(-1)
	}
	return
}

func getHelp() string {
	return runLine + "\n" + serverTypeLine
}

func CleanSession(metaPath string, etcdEndpoints []string, sessionSuffix []string) error {
	if len(sessionSuffix) == 0 {
		log.Warn("not found session info , skip to clean sessions")
		return nil
	}

	keys := getSessionPaths(metaPath, sessionSuffix)
	if len(keys) == 0 {
		return nil
	}

	etcdCli, err := etcd.GetRemoteEtcdClient(etcdEndpoints)
	if err != nil {
		return err
	}
	defer etcdCli.Close()

	ctx, cancel := context.WithTimeout(context.TODO(), 5*time.Second)
	defer cancel()
	for _, key := range keys {
		_, _ = etcdCli.Delete(ctx, key, clientv3.WithPrefix())
	}
	log.Info("clean sessions from etcd", zap.Any("keys", keys))
	return nil
}

func getSessionPaths(metaPath string, sessionSuffix []string) []string {
	sessionKeys := make([]string, 0)
	sessionPathPrefix := path.Join(metaPath, sessionutil.DefaultServiceRoot)
	for _, suffix := range sessionSuffix {
		key := path.Join(sessionPathPrefix, suffix)
		sessionKeys = append(sessionKeys, key)
	}
	return sessionKeys
}
