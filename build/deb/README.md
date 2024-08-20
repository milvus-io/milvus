# Building Deb package with Docker

Building Milvus Deb package is easy if you take advantage of the containerized build environment. This document will guide you through this build process.

1. Docker, using one of the following configurations:

- **Linux with local Docker** Install Docker according to the [instructions](https://docs.docker.com/installation/#installation) for your OS.

2. Get the opensource milvus code
```bash
git clone https://github.com/milvus-io/milvus.git
cp -r milvus/build/deb .
cd deb
```

3. Start the milvus container and build the deb package
```bash
# Replace the VERSION with your own
sudo docker run -v .:/deb -ti --entrypoint /bin/bash milvusdb/milvus:v$VERSION
# in the container
cd /deb
bash build_deb.sh $VERSION $VERSION $MAINTAINER $DEBEMAIL
```

4. Install the deb package on ubuntu system
```bash
sudo apt-get update
sudo dpkg -i milvus_$VERSION-1_amd64.deb # This package is in the milvus-deb directory
sudo apt-get -f install
```

5. Check the status of Milvus
```bash
sudo systemctl status milvus
```

6. Connect to Milvus

Please refer to [Hello Milvus](https://milvus.io/docs/v2.3.x/example_code.md), then run the example code. 

7. Uninstall Milvus
```bash
sudo dpkg -P milvus
```

8. (Optional) By default, Milvus is started in embed mode. If you rely on external etcd and minio, you can modify the following configuration and then restart Milvus.
```bash
sudo vim /etc/milvus/configs/milvus.yaml
``` 
```yaml
etcd:
  endpoints: etcd-ip:2379
  ...
  use:
    embed: false
minio:
  address: minio-ip
  ...
common:
  storageType: remote
```
