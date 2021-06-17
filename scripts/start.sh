cd ..

echo "starting master"
nohup ./bin/milvus run master > ~/masterservice.out 2>&1 &

echo "starting dataservice"
nohup ./bin/milvus run dataservice > ~/dataservice.out 2>&1 &

echo "starting datanode"
nohup ./bin/milvus run datanode > ~/datanode.out 2>&1 &

echo "starting proxynode"
nohup ./bin/milvus run proxynode > ~/proxynode.out 2>&1 &

echo "starting queryservice"
nohup ./bin/milvus run queryservice > ~/queryservice.out 2>&1 &

echo "starting querynode1"
export QUERY_NODE_ID=1
nohup ./bin/milvus run querynode > ~/querynode1.out 2>&1 &


echo "starting indexservice"
nohup ./bin/milvus run indexservice > ~/indexservice.out 2>&1 &

echo "starting indexnode"
nohup ./bin/milvus run indexnode > ~/indexnode.out 2>&1 &
