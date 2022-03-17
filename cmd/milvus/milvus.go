package milvus

import (
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	syslog "log"
	"os"
	"path"
	"runtime"
	"strings"
	"syscall"

	"github.com/gofrs/flock"
	"github.com/milvus-io/milvus/cmd/roles"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/automaxprocs/maxprocs"

	// use auto max procs to set container CPU quota
	"go.uber.org/zap"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/metricsinfo"
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

func printBanner(w io.Writer) {
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

func createPidFile(w io.Writer, filename string, runtimeDir string) (*flock.Flock, error) {
	fileFullName := path.Join(runtimeDir, filename)

	fd, err := os.OpenFile(fileFullName, os.O_CREATE|os.O_RDWR, 0664)
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

func closePidFile(fd *os.File) {
	fd.Close()
}

func removePidFile(lock *flock.Flock) {
	filename := lock.Path()
	lock.Close()
	os.Remove(filename)
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

// create runtime folder
func createRuntimeDir() string {
	runtimeDir := "/run/milvus"
	if runtime.GOOS == "windows" {
		runtimeDir = "run"
		if err := makeRuntimeDir(runtimeDir); err != nil {
			fmt.Fprintf(os.Stderr, "Create runtime directory at %s failed\n", runtimeDir)
			os.Exit(-1)
		}
	} else {
		if err := makeRuntimeDir(runtimeDir); err != nil {
			fmt.Fprintf(os.Stderr, "Set runtime dir at %s failed, set it to /tmp/milvus directory\n", runtimeDir)
			runtimeDir = "/tmp/milvus"
			if err = makeRuntimeDir(runtimeDir); err != nil {
				fmt.Fprintf(os.Stderr, "Create runtime directory at %s failed\n", runtimeDir)
				os.Exit(-1)
			}
		}
	}
	return runtimeDir
}

func RunMilvus(args []string) {
	if len(args) < 3 {
		_, _ = fmt.Fprint(os.Stderr, "usage: milvus [command] [server type] [flags]\n")
		return
	}
	command := args[1]
	serverType := args[2]
	flags := flag.NewFlagSet(args[0], flag.ExitOnError)

	var svrAlias string
	flags.StringVar(&svrAlias, "alias", "", "set alias")

	var enableRootCoord, enableQueryCoord, enableIndexCoord, enableDataCoord bool
	flags.BoolVar(&enableRootCoord, typeutil.RootCoordRole, false, "enable root coordinator")
	flags.BoolVar(&enableQueryCoord, typeutil.QueryCoordRole, false, "enable query coordinator")
	flags.BoolVar(&enableIndexCoord, typeutil.IndexCoordRole, false, "enable index coordinator")
	flags.BoolVar(&enableDataCoord, typeutil.DataCoordRole, false, "enable data coordinator")

	// Discard Milvus welcome logs, init logs and maxprocs logs in embedded Milvus.
	if serverType == typeutil.EmbeddedRole {
		flags.SetOutput(io.Discard)
		// Initialize maxprocs while discarding log.
		maxprocs.Set(maxprocs.Logger(nil))
	} else {
		// Initialize maxprocs.
		maxprocs.Set(maxprocs.Logger(syslog.Printf))
	}
	flags.Usage = func() {
		fmt.Fprintf(flags.Output(), "Usage of %s:\n", args[0])
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

	if err := flags.Parse(args[3:]); err != nil {
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
		role.EnableRootCoord = enableRootCoord
		role.EnableQueryCoord = enableQueryCoord
		role.EnableDataCoord = enableDataCoord
		role.EnableIndexCoord = enableIndexCoord
	default:
		fmt.Fprintf(os.Stderr, "Unknown server type = %s\n", serverType)
		os.Exit(-1)
	}

	// Setup logger in advance for standalone and embedded Milvus.
	// Any log from this point on is under control.
	if serverType == typeutil.StandaloneRole || serverType == typeutil.EmbeddedRole {
		var params paramtable.BaseTable
		if serverType == typeutil.EmbeddedRole {
			params.GlobalInitWithYaml("embedded-milvus.yaml")
		} else {
			params.Init()
		}
		params.SetLogConfig()
		params.RoleName = serverType
		params.SetLogger(0)
	}

	runtimeDir := createRuntimeDir()
	filename := getPidFileName(serverType, svrAlias)
	switch command {
	case "run":
		printBanner(flags.Output())
		injectVariablesToEnv()
		lock, err := createPidFile(flags.Output(), filename, runtimeDir)
		if err != nil {
			panic(err)
		}
		defer removePidFile(lock)
		role.Run(local, svrAlias)
	case "stop":
		if err := stopPid(filename, runtimeDir); err != nil {
			fmt.Fprintf(os.Stderr, "%s\n\n", err.Error())
		}
	case "dry-run":
		// A dry run does not actually bring up the Milvus instance.
	default:
		fmt.Fprintf(os.Stderr, "unknown command : %s\n", command)
	}
}
