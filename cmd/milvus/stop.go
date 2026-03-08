package milvus

import (
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"syscall"

	"github.com/milvus-io/milvus/pkg/v2/util/hardware"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	StopCmd = "stop"
)

type stop struct {
	serverType string
	svrAlias   string
}

func (c *stop) getHelp() string {
	return stopLine + "\n" + serverTypeLine
}

func (c *stop) execute(args []string, flags *flag.FlagSet) {
	if len(args) < 3 {
		fmt.Fprintln(os.Stderr, c.getHelp())
		return
	}
	flags.Usage = func() {
		fmt.Fprintln(os.Stderr, c.getHelp())
	}
	c.serverType = args[2]
	if !typeutil.ServerTypeSet().Contain(c.serverType) {
		fmt.Fprintf(os.Stderr, "Unknown server type = %s\n", c.serverType)
		os.Exit(-1)
	}
	c.formatFlags(args, flags)

	runtimeDir := createRuntimeDir(c.serverType)
	filename := getPidFileName(c.serverType, c.svrAlias)
	if err := c.stopPid(filename, runtimeDir); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n\n", err.Error())
	}
}

func (c *stop) formatFlags(args []string, flags *flag.FlagSet) {
	flags.StringVar(&(c.svrAlias), "alias", "", "set alias")
	if c.serverType == typeutil.EmbeddedRole {
		flags.SetOutput(io.Discard)
	}
	hardware.InitMaxprocs(c.serverType, flags)
	if err := flags.Parse(args[3:]); err != nil {
		os.Exit(-1)
	}
}

func (c *stop) stopPid(filename string, runtimeDir string) error {
	var pid int

	fd, err := os.OpenFile(path.Join(runtimeDir, filename), os.O_RDONLY, 0o664)
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
