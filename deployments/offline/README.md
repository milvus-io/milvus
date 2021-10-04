# Milvus offline installation

## Manually downloading Docker images

Milvus installation may fail when images are not properly loaded from public Docker registries. To pull all images and save them into a directory that can be moved to the target host and loaded manually, perform the following procedures:

### Step 1: Save Milvus manifests and Docker images

#### If you install Milvus with the **docker-compose.yml** file, use these commands:

#### 1. Download Milvus standalone docker-compose.yml
```shell
wget https://raw.githubusercontent.com/milvus-io/milvus/master/deployments/docker/standalone/docker-compose.yml -O docker-compose.yml
```

&nbsp;&nbsp;&nbsp; or download Milvus cluster docker-compose.yml

```shell
wget https://raw.githubusercontent.com/milvus-io/milvus/master/deployments/docker/cluster/docker-compose.yml -O docker-compose.yml
```

#### 2. Pull and save Docker images
```shell
pip3 install -r requirements.txt
python3 save_image.py --manifest docker-compose.yml
```

#### If you install Milvus with **Helm**, use these command:
#### 1. Update Helm repo
```shell
helm repo add milvus https://milvus-io.github.io/milvus-helm/
helm repo update
```

#### 2. Get Kubernetes manifests of Milvus standalone
```shell
helm template my-release milvus/milvus > milvus_manifest.yaml
```

&nbsp;&nbsp;&nbsp;or get Kubernetes manifests of Milvus cluster

```shell
helm template --set cluster.enabled=true my-release milvus/milvus > milvus_manifest.yaml
```

#### 3. Pull and save Docker images
```shell
pip3 install -r requirements.txt
python3 save_image.py --manifest milvus_manifest.yaml
```

The Docker images will be stored under **images** directory.

### Step 2: Load Docker images
Enter the following command to load the Docker images:

```shell
cd images/
for image in $(find . -type f -name "*.tar.gz") ; do gunzip -c $image | docker load; done
```

## Install Milvus

### With Docker Compose

```shell
docker-compose -f docker-compose.yml up -d
```

### On Kubernetes

```shell
kubectl apply -f milvus_manifest.yaml
```

## Uninstall Milvus

### With Docker Compose

```shell
docker-compose -f docker-compose.yml down
```

### On Kubernetes

```shell
kubectl delete -f milvus_manifest.yaml
```
