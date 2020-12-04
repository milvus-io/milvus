package master

import (
	"fmt"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"

	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-distributed/internal/errors"
	etcdkv "github.com/zilliztech/milvus-distributed/internal/kv/etcd"
)

type SysConfig struct {
	kv *etcdkv.EtcdKV
}

// Initialize Configs from config files, and store them in Etcd.
func (conf *SysConfig) InitFromFile(filePath string) error {
	memConfigs, err := conf.getConfigFiles(filePath)
	if err != nil {
		return errors.Errorf("[Init SysConfig] %s\n", err.Error())
	}

	for _, memConfig := range memConfigs {
		if err := conf.saveToEtcd(memConfig, "config"); err != nil {
			return errors.Errorf("[Init SysConfig] %s\n", err.Error())
		}
	}
	return nil
}

func (conf *SysConfig) GetByPrefix(keyPrefix string) (keys []string, values []string, err error) {
	realPrefix := path.Join("config", strings.ToLower(keyPrefix))
	keys, values, err = conf.kv.LoadWithPrefix(realPrefix)
	for index := range keys {
		keys[index] = strings.Replace(keys[index], conf.kv.GetPath("config"), "", 1)
	}
	if err != nil {
		return nil, nil, err
	}
	log.Println("Loaded", len(keys), "pairs of configs with prefix", keyPrefix)
	return keys, values, err
}

// Get specific configs for keys.
func (conf *SysConfig) Get(keys []string) ([]string, error) {
	var keysToLoad []string
	for i := range keys {
		keysToLoad = append(keysToLoad, path.Join("config", strings.ToLower(keys[i])))
	}

	values, err := conf.kv.MultiLoad(keysToLoad)
	if err != nil {
		return nil, err
	}

	return values, nil
}

func (conf *SysConfig) getConfigFiles(filePath string) ([]*viper.Viper, error) {

	var vipers []*viper.Viper
	err := filepath.Walk(filePath,
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			// all names
			if !info.IsDir() && filepath.Ext(path) == ".yaml" {
				log.Println("Config files ", info.Name())

				currentConf := viper.New()
				currentConf.SetConfigFile(path)
				if err := currentConf.ReadInConfig(); err != nil {
					log.Panic("Config file error: ", err)
				}
				vipers = append(vipers, currentConf)
			}
			return nil
		})
	if err != nil {
		return nil, err
	}

	if len(vipers) == 0 {
		return nil, errors.Errorf("There are no config files in the path `%s`.\n", filePath)
	}
	return vipers, nil
}

func (conf *SysConfig) saveToEtcd(memConfig *viper.Viper, secondRootPath string) error {
	configMaps := map[string]string{}

	allKeys := memConfig.AllKeys()
	for _, key := range allKeys {
		etcdKey := strings.ReplaceAll(key, ".", "/")

		etcdKey = path.Join(secondRootPath, etcdKey)

		val := memConfig.Get(key)
		if val == nil {
			configMaps[etcdKey] = ""
			continue
		}
		configMaps[etcdKey] = fmt.Sprintf("%v", val)
	}

	if err := conf.kv.MultiSave(configMaps); err != nil {
		return err
	}

	return nil
}
