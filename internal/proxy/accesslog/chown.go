//go:build !linux
// +build !linux

package accesslog

import (
	"os"
)

func chown(_ string, _ os.FileInfo) error {
	return nil
}
