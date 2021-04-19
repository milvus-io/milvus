echo "stopping masterservice"
kill -9 $(ps -e | grep masterservice | awk '{print $1}')

echo "stopping proxyservice"
kill -9 $(ps -e | grep proxyservice | awk '{print $1}')

echo "stopping proxynode"
kill -9 $(ps -e | grep proxynode | awk '{print $1}')

echo "stopping queryservice"
kill -9 $(ps -e | grep queryservice | awk '{print $1}')

echo "stopping querynode"
kill -9 $(ps -e | grep querynode | awk '{print $1}')

echo "stopping dataservice"
kill -9 $(ps -e | grep dataservice | awk '{print $1}')

echo "stopping datanode"
kill -9 $(ps -e | grep datanode | awk '{print $1}')

echo "stopping indexservice"
kill -9 $(ps -e | grep indexservice | awk '{print $1}')

echo "stopping indexnode"
kill -9 $(ps -e | grep indexnode | awk '{print $1}')

echo "completed"

