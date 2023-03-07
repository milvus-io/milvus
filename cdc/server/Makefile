build:
	@echo "Building CDC ..."
	@CGO_CFLAGS="-I./rocksdb/include" \
     	CGO_LDFLAGS="-L./rocksdb -lrocksdb -lstdc++ -lm -lz -lbz2 -lsnappy -llz4 -lzstd -ldl" \
    	go get github.com/tecbot/gorocksdb && \
		go build -o cdc main/main.go