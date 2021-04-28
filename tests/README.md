## Tests

### E2E Test

#### 配置清单

##### 操作系统

| 操作系统   | 版本        |
| ------ | --------- |
| CentOS | 7.5 或以上   |
| Ubuntu | 16.04 或以上 |
| Mac    | 10.14 或以上 |

##### 硬件

| 硬件名称 | 建议配置                                                                                                |
| ---- | --------------------------------------------------------------------------------------------------- |
| CPU  | x86_64 平台<br> Intel CPU Sandy Bridge 或以上<br> CPU 指令集<br> _ SSE42<br> _ AVX<br> _ AVX2<br> _ AVX512 |
| 内存   | 16 GB 或以上                                                                                           |

##### 软件

| 软件名称           | 版本         |
| -------------- | ---------- |
| Docker         | 19.05 或以上  |
| Docker Compose | 1.25.5 或以上 |
| jq             | 1.3 或以上    |
| kubectl        | 1.14 或以上   |
| helm           | 3.0 或以上    |
| kind           | 0.10.0 或以上 |

#### 安装依赖

##### 检查 Docker 和 Docker Compose 状态

  1. 确认 Docker Daemon 正在运行：

```shell
$ docker info
```

-   安装 Docker 步骤见 [Docker CE/EE 官方安装说明](https://docs.docker.com/get-docker/)进行安装

-   如果无法正常打印 Docker 相关信息，请启动 Docker Daemon。

-   要在没有 `root` 权限的情况下运行 Docker 命令，请创建 `docker` 组并添加用户，以运行：`sudo usermod -aG docker $USER`， 退出终端并重新登录，以使更改生效 ，详见 [使用非 root 用户管理 docker](https://docs.docker.com/install/linux/linux-postinstall/)。

  2. 确认 Docker Compose 版本

```shell
$ docker-compose version

docker-compose version 1.25.5, build 8a1c60f6
docker-py version: 4.1.0
CPython version: 3.7.5
OpenSSL version: OpenSSL 1.1.1f  31 Mar 2020
```

-   安装 Docker Compose 步骤见 [Install Docker Compose](https://docs.docker.com/compose/install/)

##### 安装 jq

-   安装方式见 <https://stedolan.github.io/jq/download/>

##### 安装 kubectl

-   安装方式见 <https://kubernetes.io/docs/tasks/tools/>

##### 安装 helm

-   安装方式见 <https://helm.sh/docs/intro/install/>

##### 安装 kind

-   安装方式见 <https://kind.sigs.k8s.io/docs/user/quick-start/#installation>

#### 运行 E2E Test

```shell
$ cd tests/scripts
$ ./e2e-k8s.sh
```

> Getting help
>
> 你可以执行以下命令获取帮助
>
> ```shell
> $ ./e2e-k8s.sh --help
> ```
