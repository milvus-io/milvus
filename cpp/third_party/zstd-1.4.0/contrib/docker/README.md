
## Requirement

The `Dockerfile` script requires a version of `docker` >= 17.05

## Installing docker

The official docker install docs use a ppa with a modern version available:
https://docs.docker.com/install/linux/docker-ce/ubuntu/

## How to run

`docker build -t zstd .`

## test

```
echo foo | docker run -i --rm zstd | docker run -i --rm zstd zstdcat
foo
```
