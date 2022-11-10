//go:build linux
// +build linux

package accesslog

import (
	"os"
	"syscall"
)

func chown(name string, info os.FileInfo) error {
	f, err := os.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}
	f.Close()
	stat := info.Sys().(*syscall.Stat_t)
	return os.Chown(name, int(stat.Uid), int(stat.Gid))
}
