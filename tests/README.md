## Tests

### E2E Test

#### Configuration Requirements

##### Operating System

| Operating System   | Version        |
| ------ | --------- |
| CentOS | 7.5 or above   |
| Ubuntu | 16.04 or above |
| Mac    | 10.14 or above |

##### Hardware

| Hardware Type | Recommended Configuration                                                                                                |
| ---- | --------------------------------------------------------------------------------------------------- |
| CPU  | x86_64 architechture <br> Intel CPU Sandy Bridge or above<br> CPU Instruction Set<br> _ SSE42<br> _ AVX<br> _ AVX2<br> _ AVX512 |
| Memory   | 16 GB or more                                                                                           |

##### Software

| Software Name           | Version         |
| -------------- | ---------- |
| Docker         | 19.05 or above  |
| Docker Compose | 1.25.5 or above |
| jq             | 1.3 or above    |
| kubectl        | 1.14 or above   |
| helm           | 3.0 or above    |
| kind           | 0.10.0 or above |

#### Installing Dependencies

##### Troubleshooting Docker and Docker Compose

  1. Confirm that Docker Daemon is running：

```shell
$ docker info
```

-   Ensure that Docker is installed. Refer to the official installation instructions for [Docker CE/EE](https://docs.docker.com/get-docker/).

-   Start the Docker Daemon if it is not already started.

-   To run Docker without `root` priveleges, create a user group labeled `docker`, then add a user to the group with `sudo usermod -aG docker $USER`. Log out and log back into the terminal for the changes to take effect. For more invormation, see the official Docker documentation for [Managing Docker as a Non-Root User](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

  2. Check the version of Docker-Compose

```shell
$ docker-compose version

docker-compose version 1.25.5, build 8a1c60f6
docker-py version: 4.1.0
CPython version: 3.7.5
OpenSSL version: OpenSSL 1.1.1f  31 Mar 2020
```

-   To install Docker-Compose, see [Install Docker Compose](https://docs.docker.com/compose/install/)

##### Install jq

-   Refer to <https://stedolan.github.io/jq/download/>

##### Install kubectl

-   Refer to <https://kubernetes.io/docs/tasks/tools/>

##### Install helm

-   Refer to <https://helm.sh/docs/intro/install/>

##### Install kind

-   Refer to <https://kind.sigs.k8s.io/docs/user/quick-start/#installation>

#### Run E2E Tests

```shell
$ cd tests/scripts
$ ./e2e-k8s.sh
```

> Getting help
>
> You can get help with the following command:
>
> ```shell
> $ ./e2e-k8s.sh --help
> ```
