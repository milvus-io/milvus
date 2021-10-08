cd ..

echo "starting standalone"
nohup ./bin/milvus run standalone > /tmp/standalone.log 2>&1 &