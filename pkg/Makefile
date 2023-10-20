GO		  ?= go
PWD 	  := $(shell pwd)
GOPATH	:= $(shell $(GO) env GOPATH)
ROOTPATH := $(abspath $(dir $(PWD)))
SHELL 	:= /bin/bash
OBJPREFIX := "github.com/milvus-io/milvus/cmd/milvus"

INSTALL_PATH := $(ROOTPATH)/bin

# use parent getdeps
getdeps:
	$(MAKE) -C $(ROOTPATH) getdeps

generate-mockery: getdeps
	$(INSTALL_PATH)/mockery --name=MsgStream --dir=$(PWD)/mq/msgstream --output=$(PWD)/mq/msgstream --filename=mock_msgstream.go --with-expecter --structname=MockMsgStream --outpkg=msgstream --inpackage
	$(INSTALL_PATH)/mockery --name=Factory --dir=$(PWD)/mq/msgstream --output=$(PWD)/mq/msgstream --filename=mock_msgstream_factory.go --with-expecter --structname=MockFactory --outpkg=msgstream --inpackage
	$(INSTALL_PATH)/mockery --name=Client --dir=$(PWD)/mq/msgdispatcher --output=$(PWD)/mq/msgsdispatcher --filename=mock_client.go --with-expecter --structname=MockClient --outpkg=msgdispatcher --inpackage
	$(INSTALL_PATH)/mockery --name=Logger --dir=$(PWD)/eventlog --output=$(PWD)/eventlog --filename=mock_logger.go --with-expecter --structname=MockLogger --outpkg=eventlog --inpackage
	$(INSTALL_PATH)/mockery --name=MessageID --dir=$(PWD)/mq/msgstream/mqwrapper --output=$(PWD)/mq/msgstream/mqwrapper --filename=mock_id.go --with-expecter --structname=MockMessageID --outpkg=mqwrapper --inpackage
