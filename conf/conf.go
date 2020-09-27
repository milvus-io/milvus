package conf

import (
	"io/ioutil"
	"path"
	"runtime"

	"github.com/czs007/suvlim/storage/pkg/types"
	yaml "gopkg.in/yaml.v2"
)

// yaml.MapSlice

type MasterConfig struct {
	Address               string
	Port                  int32
	PulsarMoniterInterval int32
	PulsarTopic           string
	SegmentThreshole      float32
	ProxyIdList           []int64
	QueryNodeNum          int
	WriteNodeNum          int
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
	Address  string
	Port     int32
	TopicNum int
}

//type ProxyConfig struct {
//	Timezone string
//	Address  string
//	Port     int32
//}

type Reader struct {
	ClientId        int
	StopFlag        int64
	ReaderQueueSize int
	SearchChanSize  int
	Key2SegChanSize int
	TopicStart      int
	TopicEnd        int
}

type Writer struct {
	ClientId           int
	StopFlag           int64
	ReaderQueueSize    int
	SearchByIdChanSize int
	Parallelism        int
	TopicStart         int
	TopicEnd           int
}

type ServerConfig struct {
	Master   MasterConfig
	Etcd     EtcdConfig
	Timesync TimeSyncConfig
	Storage  StorageConfig
	Pulsar   PulsarConfig
	Writer   Writer
	Reader   Reader
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
