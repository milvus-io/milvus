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
| CPU  | x86_64 平台<br> Intel CPU Sandy Bridge 或以上<br> CPU 指令集<br> - SSE4_2<br> - AVX<br> - AVX2<br> - AVX512 |
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
$ docker compose version

docker compose version 1.25.5, build 8a1c60f6
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

### Python 代码质量 (ruff)

Ruff 配置位于 `tests/ruff.toml`，覆盖 `tests/` 下所有 Python 代码
（`python_client/`、`restful_client/`、`restful_client_v2/`、`benchmark/`、`scripts/`）。
各子目录的运行时依赖仍通过各自的 `requirements.txt` 管理。

```shell
$ cd tests/
$ ruff check .                  # lint 检查
$ ruff check . --fix            # lint 检查并自动修复
$ ruff format .                 # 原地格式化
$ ruff format --check .         # 只检查不修改 (CI 友好)
```

启用的规则：`E`、`F`、`W`、`I`、`UP`；目标 Python 版本：`3.10`。

#### 仅检查 PR 修改的文件 (与 Python Lint CI 一致)

GitHub Actions workflow `.github/workflows/python-lint.yaml`
（job **"Python Lint (tests/) / Ruff (changed files only)"**）
只对 PR 修改的 `tests/**/*.py` 文件跑 `ruff check` 和 `ruff format --check`。
直接对整个 `tests/` 目录执行 `uv run ruff check .` 太粗——很多历史文件早于
当前 lint 配置，会因为无关规则失败。

要在本地复现 CI 这一步，使用本目录下的 `Makefile`：

```shell
$ cd tests/
$ make ci             # 对 PR 修改文件跑 ruff check + format --check（与 CI 等价）
$ make lint-fix       # 对 PR 修改文件跑 ruff check --fix
$ make format         # 对 PR 修改文件跑 ruff format
$ make help           # 显示所有 target 以及当前自动检测到的 BASE_REF
```

`BASE_REF` 通过 `gh pr view` 从当前分支的开放 PR 中自动检测：
解析 PR URL 得到 base `<owner>/<repo>`，再匹配本地 git remotes，
得到形如 `upstream/master`、`upstream/2.x` 或（直接 clone 主仓库时的）
`origin/main`。

如果当前分支没有开放 PR，**必须**显式设置 `BASE_REF`
（不再做猜测——base 选错会 diff 出无关 commit）：

```shell
$ make ci BASE_REF=upstream/master
```

需要本地已安装 `uv`（提供 `uvx`）和已认证的 `gh`
（`gh auth status`）。Makefile 通过 `RUFF_VERSION` 锁定与
workflow 一致的 ruff 版本。

