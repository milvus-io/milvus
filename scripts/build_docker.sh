cd ../build/docker/deploy/

docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 master
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 proxyservice
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 proxynode 
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 indexservice 
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 indexnode 
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 queryservice 
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 dataservice 
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 querynode 
docker-compose build --build-arg https_proxy=http://wakanda:Fantast1c@192.168.2.28:3339 datanode 
