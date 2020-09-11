package conf

import (
	"fmt"
	"path"
	"os"
	"github.com/BurntSushi/toml"
)

type StorageConfig struct {
	Driver string
}

var config *StorageConfig = new(StorageConfig)

func GetConfig() *StorageConfig {
	return config
}

func init() {
	//读取配置文件
	dirPath, _ := os.Getwd()
	filePath := path.Join(dirPath, "config/storage.toml")
	fmt.Println("aaa")
	fmt.Println(filePath)
	fmt.Println("bbb")
	_, err := toml.DecodeFile(filePath, config)
	if err != nil {
		fmt.Println(err)
	}
}
