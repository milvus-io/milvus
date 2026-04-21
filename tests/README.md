## Tests

### E2E Test

#### Configuration Requirements

##### Operating System

| Operating System | Version        |
| ---------------- | -------------- |
| Amazon Linux     | 2023 or above  |
| Ubuntu           | 20.04 or above |
| Mac              | 10.14 or above |

##### Hardware

| Hardware Type | Recommended Configuration                                                                                                                            |
| ------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------- |
| CPU           | x86_64 architecture <br> Intel CPU Sandy Bridge or above<br> CPU Instruction Set<br> - SSE4_2<br> - AVX<br> - AVX2<br> - AVX512 or arm64 Linux/MacOS |
| Memory        | 16 GB or more                                                                                                                                        |

##### Software

| Software Name  | Version         |
| -------------- | --------------- |
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

- Ensure that Docker is installed. Refer to the official installation instructions for [Docker CE/EE](https://docs.docker.com/get-docker/).

- Start the Docker Daemon if it is not already started.

- To run Docker without `root` privileges, create a user group labeled `docker`, then add a user to the group with `sudo usermod -aG docker $USER`. Log out and log back into the terminal for the changes to take effect. For more information, see the official Docker documentation for [Managing Docker as a Non-Root User](https://docs.docker.com/engine/install/linux-postinstall/#manage-docker-as-a-non-root-user).

2. Check the version of Docker-Compose

```shell
$ docker compose version

docker compose version 1.25.5, build 8a1c60f6
docker-py version: 4.1.0
CPython version: 3.7.5
OpenSSL version: OpenSSL 1.1.1f  31 Mar 2020
```

- To install Docker-Compose, see [Install Docker Compose](https://docs.docker.com/compose/install/)

##### Install jq

- Refer to <https://stedolan.github.io/jq/download/>

##### Install kubectl

- Refer to <https://kubernetes.io/docs/tasks/tools/>

##### Install helm

- Refer to <https://helm.sh/docs/intro/install/>

##### Install kind

- Refer to <https://kind.sigs.k8s.io/docs/user/quick-start/#installation>

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

### Python Code Quality (ruff via uv)

Ruff is configured at `tests/pyproject.toml` and covers all Python code under `tests/`
(`python_client/`, `restful_client/`, `restful_client_v2/`, `benchmark/`, `scripts/`).
[uv](https://docs.astral.sh/uv/) is only used to host the lint/format toolchain; each
sub-directory continues to manage its runtime dependencies via its own `requirements.txt`.

```shell
$ cd tests/
$ uv sync                       # install ruff into a local .venv
$ uv run ruff check .           # lint
$ uv run ruff check . --fix     # lint with auto-fix
$ uv run ruff format .          # format in place
$ uv run ruff format --check .  # format check only (CI-friendly)
```

Rules enabled: `E`, `F`, `W`, `I`, `UP`. Target Python version: `3.10`.

