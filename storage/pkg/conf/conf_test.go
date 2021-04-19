package conf_test

import (
	"fmt"
	"os"
	"storage/pkg/conf"
	"testing"
)

func TestMain(m *testing.M) {
	exitCode := m.Run()
	fmt.Println("haha")
	config := conf.GetConfig()
	fmt.Println(config.Driver)
	os.Exit(exitCode)
}
