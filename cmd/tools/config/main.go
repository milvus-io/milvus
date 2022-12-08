package main

import (
	"encoding/csv"
	"os"
	"reflect"

	"github.com/milvus-io/milvus/internal/log"
	"github.com/milvus-io/milvus/internal/util/paramtable"
	"github.com/milvus-io/milvus/internal/util/typeutil"
	"go.uber.org/zap"
)

func collect(data *[][]string, val *reflect.Value) {
	if val.Kind() != reflect.Struct {
		return
	}
	for j := 0; j < val.NumField(); j++ {
		subVal := val.Field(j)
		tag := val.Type().Field(j).Tag
		log.Debug("subVal", zap.Any("subVal", subVal),
			zap.String("name", val.Type().Field(j).Name),
			zap.Any("tag", val.Type().Field(j).Tag),
			zap.Any("type", val.Type().Field(j).Type),
		)
		t := val.Type().Field(j).Type.String()
		if t == "paramtable.ParamItem" {
			item := subVal.Interface().(paramtable.ParamItem)
			refreshable := tag.Get("refreshable")
			if refreshable == "" {
				refreshable = "undefined"
			}
			*data = append(*data, []string{item.Key, item.GetValue(), item.Version, refreshable})
		} else if t == "paramtable.ParamGroup" {
			item := subVal.Interface().(paramtable.ParamGroup)
			refreshable := tag.Get("refreshable")
			if refreshable == "" {
				refreshable = "undefined"
			}
			*data = append(*data, []string{item.KeyPrefix, "", item.Version, refreshable})
		} else {
			collect(data, &subVal)
		}
	}
}

func main() {
	params := paramtable.ComponentParam{}
	params.Init()

	f, err := os.Create("configs.csv")
	defer f.Close()
	if err != nil {
		log.Error("create file failed", zap.Error(err))
		os.Exit(-1)
	}
	w := csv.NewWriter(f)
	w.Write([]string{"key", "default", "version", "refreshable"})

	val := reflect.ValueOf(params)
	data := make([][]string, 0)
	keySet := typeutil.NewSet[string]()
	for i := 0; i < val.NumField(); i++ {
		valueField := val.Field(i)
		// typeField := val.Type().Field(i)
		collect(&data, &valueField)
	}
	result := make([][]string, 0)
	for _, d := range data {
		if keySet.Contain(d[0]) {
			continue
		}
		keySet.Insert(d[0])
		result = append(result, d)
	}
	w.WriteAll(result)
	w.Flush()

}
