package milvus

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path"

	"github.com/milvus-io/milvus/cmd/roles"
	"github.com/milvus-io/milvus/pkg/util/hardware"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

const (
	cleanCmd = "clean"
)

type clean struct {
	serverType string
	svrAlias   string
}

func (c *clean) getHelp() string {
	return cleanLine + "\n" + serverTypeLine
}

func (c *clean) execute(args []string, flags *flag.FlagSet) {
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

	err := c.cleanSession(context.Background(), flags)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
	}
}

func (c *clean) cleanSession(ctx context.Context, flags *flag.FlagSet) error {
	runtimeDir := createRuntimeDir(c.serverType)
	filename := getPidFileName(c.serverType, c.svrAlias)

	serverID, err := c.getServerID(runtimeDir, filename)
	if err != nil {
		return err
	}

	role := roles.NewMilvusRoles()
	err = role.Init(c.serverType, flags)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n%s", err.Error(), c.getHelp())
		os.Exit(-1)
	}
	return role.Clean(ctx, int64(serverID))
}

func (c *clean) formatFlags(args []string, flags *flag.FlagSet) {
	flags.StringVar(&(c.svrAlias), "alias", "", "set alias")
	if c.serverType == typeutil.EmbeddedRole {
		flags.SetOutput(io.Discard)
	}
	hardware.InitMaxprocs(c.serverType, flags)
	if err := flags.Parse(args[3:]); err != nil {
		os.Exit(-1)
	}
}

func (c *clean) getServerID(runtimeDir string, filename string) (int, error) {
	var sid int

	fd, err := os.OpenFile(path.Join(runtimeDir, filename), os.O_RDONLY, 0o664)
	if err != nil {
		return 0, err
	}
	defer closePidFile(fd)

	if _, err = fmt.Fscanf(fd, "%d", &sid); err != nil {
		return 0, err
	}

	return sid, nil
}
