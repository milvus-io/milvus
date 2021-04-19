package conf

import (
	"io/ioutil"
	"path"
	"runtime"

	"github.com/zilliztech/milvus-distributed/internal/util/typeutil"

	storagetype "github.com/zilliztech/milvus-distributed/internal/storage/type"
	yaml "gopkg.in/yaml.v2"
)

type UniqueID = typeutil.UniqueID

// yaml.MapSlice

type MasterConfig struct {
	Address               string
	Port                  int32
	PulsarMoniterInterval int32
	PulsarTopic           string
	SegmentThreshole      float32
	ProxyIDList           []UniqueID
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
	Driver    storagetype.DriverType
	Address   string
	Port      int32
	Accesskey string
	Secretkey string
}

type PulsarConfig struct {
	Authentication bool
	User           string
	Token          string
	Address        string
	Port           int32
	TopicNum       int
}

type ProxyConfig struct {
	Timezone         string `yaml:"timezone"`
	ProxyID          int    `yaml:"proxy_id"`
	NumReaderNodes   int    `yaml:"numReaderNodes"`
	TosSaveInterval  int    `yaml:"tsoSaveInterval"`
	TimeTickInterval int    `yaml:"timeTickInterval"`
	PulsarTopics     struct {
		ReaderTopicPrefix string `yaml:"readerTopicPrefix"`
		NumReaderTopics   int    `yaml:"numReaderTopics"`
		DeleteTopic       string `yaml:"deleteTopic"`
		QueryTopic        string `yaml:"queryTopic"`
		ResultTopic       string `yaml:"resultTopic"`
		ResultGroup       string `yaml:"resultGroup"`
		TimeTickTopic     string `yaml:"timeTickTopic"`
	} `yaml:"pulsarTopics"`
	Network struct {
		Address string `yaml:"address"`
		Port    int    `yaml:"port"`
	} `yaml:"network"`
	Logs struct {
		Level          string `yaml:"level"`
		TraceEnable    bool   `yaml:"trace.enable"`
		Path           string `yaml:"path"`
		MaxLogFileSize string `yaml:"max_log_file_size"`
		LogRotateNum   int    `yaml:"log_rotate_num"`
	} `yaml:"logs"`
	Storage struct {
		Path              string `yaml:"path"`
		AutoFlushInterval int    `yaml:"auto_flush_interval"`
	} `yaml:"storage"`
}

type Reader struct {
	ClientID        int
	StopFlag        int64
	ReaderQueueSize int
	SearchChanSize  int
	Key2SegChanSize int
	TopicStart      int
	TopicEnd        int
}

type Writer struct {
	ClientID           int
	StopFlag           int64
	ReaderQueueSize    int
	SearchByIDChanSize int
	Parallelism        int
	TopicStart         int
	TopicEnd           int
	Bucket             string
}

type ServerConfig struct {
	Master   MasterConfig
	Etcd     EtcdConfig
	Timesync TimeSyncConfig
	Storage  StorageConfig
	Pulsar   PulsarConfig
	Writer   Writer
	Reader   Reader
	Proxy    ProxyConfig
}

var Config ServerConfig

// func init() {
// 	load_config()
// }

func getConfigsDir() string {
	_, fpath, _, _ := runtime.Caller(0)
	configPath := path.Dir(fpath) + "/../../configs/"
	configPath = path.Dir(configPath)
	return configPath
}

func LoadConfigWithPath(yamlFilePath string) {
	source, err := ioutil.ReadFile(yamlFilePath)
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(source, &Config)
	if err != nil {
		panic(err)
	}
	//fmt.Printf("Result: %v\n", Config)
}

func LoadConfig(yamlFile string) {
	filePath := path.Join(getConfigsDir(), yamlFile)
	LoadConfigWithPath(filePath)
}
