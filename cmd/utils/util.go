package utils

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"runtime"

	"github.com/gofrs/flock"

	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type RuntimeDir interface {
	GetDir() string
	GetOperationLogger() io.Writer
}

type RuntimeDirImpl struct {
	// runtime dir for pid file and sid file
	dir string

	// logger for pid/sid file operation
	outputWriter io.Writer
}

func (r *RuntimeDirImpl) GetDir() string {
	return r.dir
}

func (r *RuntimeDirImpl) GetOperationLogger() io.Writer {
	return r.outputWriter
}

var GlobalRuntimeDir RuntimeDir

// InitRuntimeDir create the runtime dir
func InitRuntimeDir(writer io.Writer, sType string) {
	if GlobalRuntimeDir != nil {
		return
	}

	GlobalRuntimeDir = &RuntimeDirImpl{
		dir:          createRuntimeDir(sType),
		outputWriter: writer,
	}
}

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

func GetPidFileName(serverType string, alias string) string {
	var filename string
	if len(alias) != 0 {
		filename = fmt.Sprintf("%s-%s.pid", serverType, alias)
	} else {
		filename = serverType + ".pid"
	}
	return filename
}

func CloseFile(fd *os.File) {
	fd.Close()
}

func RemoveFile(lock *flock.Flock) {
	filename := lock.Path()
	lock.Close()
	os.Remove(filename)
}

func CreateFileWithContent(outputWriter io.Writer, dir string, filename string, content string) (*flock.Flock, error) {
	fileFullName := path.Join(dir, filename)

	fd, err := os.OpenFile(fileFullName, os.O_CREATE|os.O_RDWR, 0o664)
	if err != nil {
		return nil, fmt.Errorf("file %s is locked, error = %w", filename, err)
	}
	fmt.Fprintln(outputWriter, "open file:", fileFullName)

	defer fd.Close()

	fd.Truncate(0)
	_, err = fd.WriteString(content)
	if err != nil {
		return nil, fmt.Errorf("file %s write fail, error = %w", filename, err)
	}

	lock := flock.New(fileFullName)
	_, err = lock.TryLock()
	if err != nil {
		return nil, fmt.Errorf("file %s is locked, error = %w", filename, err)
	}

	fmt.Fprintln(outputWriter, "lock file:", fileFullName)
	return lock, nil
}

func GetServerIDFileName(serverType string, alias string) string {
	var filename string
	if len(alias) != 0 {
		filename = fmt.Sprintf("%s-%s.sid", serverType, alias)
	} else {
		filename = serverType + ".sid"
	}
	return filename
}

func OpenFile(dir string, filename string) (*os.File, error) {
	filepath := path.Join(dir, filename)
	return os.OpenFile(filepath, os.O_RDONLY, 0o664)
}
