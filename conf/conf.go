package conf

import (
	"github.com/czs007/suvlim/storage/pkg/types"
	yaml "gopkg.in/yaml.v2"
	"io/ioutil"
	"path"
	"runtime"
)

// yaml.MapSlice

type MasterConfig struct {
	Address string
	Port    int32
}

type EtcdConfig struct {
	Address      string
	Port         int32
	Rootpath     string
	Segthreshold int64
}

type TimeSyncConfig struct {
	Interval int32
}

type StorageConfig struct {
	Driver    types.DriverType
	Address   string
	Port      int32
	Accesskey string
	Secretkey string
}

type PulsarConfig struct {
	Address string
	Port    int32
}

//type ProxyConfig struct {
//	Timezone string
//	Address  string
//	Port     int32
//}

type ServerConfig struct {
	Master   MasterConfig
	Etcd     EtcdConfig
	Timesync TimeSyncConfig
	Storage  StorageConfig
	Pulsar   PulsarConfig
	//Proxy    ProxyConfig
}

var Config ServerConfig

func init() {
	load_config()
}

func getCurrentFileDir() string {
    _, fpath, _, _ := runtime.Caller(0)
    return path.Dir(fpath)
}

func load_config() {
	filePath := path.Join(getCurrentFileDir(), "config.yaml")
	source, err := ioutil.ReadFile(filePath)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(source, &Config)
	if err != nil {
		panic(err)
	}

	//fmt.Printf("Result: %v\n", Config)
}
