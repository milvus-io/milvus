cd ..

echo "starting master"
nohup ./bin/masterservice > ~/masterservice.out 2>&1 &

echo "starting proxyservice"
nohup ./bin/proxyservice > ~/proxyservice.out 2>&1 &

echo "starting proxynode"
nohup ./bin/proxynode > ~/proxynode.out 2>&1 &

echo "starting queryservice"
nohup ./bin/queryservice > ~/queryservice.out 2>&1 &

echo "starting querynode1"
export QUERY_NODE_ID=1
nohup ./bin/querynode > ~/querynode1.out 2>&1 &

echo "starting querynode2"
export QUERY_NODE_ID=2
nohup ./bin/querynode > ~/querynode2.out 2>&1 &

echo "starting dataservice"
nohup ./bin/dataservice > ~/dataservice.out 2>&1 &

echo "starting datanode"
nohup ./bin/datanode > ~/datanode.out 2>&1 &

echo "starting indexservice"
nohup ./bin/indexservice > ~/indexservice.out 2>&1 &

echo "starting indexnode"
nohup ./bin/indexnode > ~/indexnode.out 2>&1 &
