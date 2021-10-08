cd ..

echo "starting rootcoord"
nohup ./bin/milvus run rootcoord > /tmp/rootcoord.log 2>&1 &

echo "starting datacoord"
nohup ./bin/milvus run datacoord > /tmp/datacoord.log 2>&1 &

echo "starting datanode"
nohup ./bin/milvus run datanode > /tmp/datanode.log 2>&1 &

echo "starting proxy"
nohup ./bin/milvus run proxy > /tmp/proxy.log 2>&1 &

echo "starting querycoord"
nohup ./bin/milvus run querycoord > /tmp/querycoord.log 2>&1 &

echo "starting querynode"
nohup ./bin/milvus run querynode > /tmp/querynode.log 2>&1 &

echo "starting indexcoord"
nohup ./bin/milvus run indexcoord > /tmp/indexcoord.log 2>&1 &

echo "starting indexnode"
nohup ./bin/milvus run indexnode > /tmp/indexnode.log 2>&1 &