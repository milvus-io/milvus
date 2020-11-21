package proxy

import (
	"fmt"
	"testing"
)

func TestParamTable_InsertChannelRange(t *testing.T) {
	ret := Params.InsertChannelNames()
	fmt.Println(ret)
}

func TestParamTable_DeleteChannelNames(t *testing.T) {
	ret := Params.DeleteChannelNames()
	fmt.Println(ret)
}

func TestParamTable_K2SChannelNames(t *testing.T) {
	ret := Params.K2SChannelNames()
	fmt.Println(ret)
}

func TestParamTable_SearchChannelNames(t *testing.T) {
	ret := Params.SearchChannelNames()
	fmt.Println(ret)
}

func TestParamTable_SearchResultChannelNames(t *testing.T) {
	ret := Params.SearchResultChannelNames()
	fmt.Println(ret)
}

func TestParamTable_ProxySubName(t *testing.T) {
	ret := Params.ProxySubName()
	fmt.Println(ret)
}

func TestParamTable_ProxyTimeTickChannelNames(t *testing.T) {
	ret := Params.ProxyTimeTickChannelNames()
	fmt.Println(ret)
}

func TestParamTable_DataDefinitionChannelNames(t *testing.T) {
	ret := Params.DataDefinitionChannelNames()
	fmt.Println(ret)
}

func TestParamTable_MsgStreamInsertBufSize(t *testing.T) {
	ret := Params.MsgStreamInsertBufSize()
	fmt.Println(ret)
}

func TestParamTable_MsgStreamSearchBufSize(t *testing.T) {
	ret := Params.MsgStreamSearchBufSize()
	fmt.Println(ret)
}

func TestParamTable_MsgStreamSearchResultBufSize(t *testing.T) {
	ret := Params.MsgStreamSearchResultBufSize()
	fmt.Println(ret)
}

func TestParamTable_MsgStreamSearchResultPulsarBufSize(t *testing.T) {
	ret := Params.MsgStreamSearchResultPulsarBufSize()
	fmt.Println(ret)
}

func TestParamTable_MsgStreamTimeTickBufSize(t *testing.T) {
	ret := Params.MsgStreamTimeTickBufSize()
	fmt.Println(ret)
}
