## 系统配置

Milvus 能够通过配置文件、命令行选项、环境变量进行配置。

优先级顺序： 命令行选项 > 环境变量 > 配置文件 > 默认值

如果提供了配置文件，则其他的命令行选项和环境变量都将被忽略。
例如： `milvus run rootcoord --config-file milvus.yaml --log-level debug` 将忽略 `--log-level` 选项。

### 语法

在控制台中使用以下语法运行 `milvus` 命令：

```shell
$ milvus [command] [server type] [flags]
```

例如：

```shell
$ MILVUS_CONFIG_FILE=/path/to/milvus/configs/milvus.yaml milvus run rootcoord
```

`command`， `server type`， `flags` 分别表示为

`command`： 指定要在程序上执行的操作。例如： `run`，`stop`

`server type`：指定执行程序的类型。`server type` 有：

- `rootcoord`
- `proxy`
- `querycoord`
- `querynode`
- `datacoord`
- `datanode`
- `indexcoord`
- `indexnode`
- `standalone`
- `mixture`

`flags`：指定命令行选项。例如，你可以使用 `-f` 或者 `--config-file` 选项去指定配置文件路径。

当 `server type` 为 `mixture` 时，必须附加以下几个 `flag` 中的一个或多个，表示这几个服务在一个进程内启动

- `-rootcoord`
- `-querycoord`
- `-datacoord`
- `-indexcoord`

> Getting help
>
> You can get help for CLI tool using the `--help` flag, or `-h` for short.
>
> ```shell
> $ milvus run rootcoord --help
> ```

### 命令行参数

**--version**

- 打印系统版本号和组件名并退出

**--config-check**

- 检查配置文件的有效性并退出
- 默认：false

**--config-file**

- 从文件中加载系统配置。如果设置了配置文件，则其他的命令行选项和环境变量都将被忽略。
- 默认值： ""
- 环境变量：MILVUS_CONFIG_FILE

**--log-level**

- 指定日志的输出级别。当前支持 `debug`，`info`，`warning`，`error`
- 默认值："info"
- 环境变量："MILVUS_LOG_LEVEL"

**--log-path**

- 指定日志的存储路径。
- 默认值："/var/lib/milvus/logs"
- 环境变量："MILVUS_LOG_PATH"

### 配置文件描述

配置文件比命令行参数支持更多的选项。你可以根据 milvus.yaml.sample 文件按照需要创建一个新的配置文件 milvus.yaml 即可。

| 名称                              | 描述                                                                                                        | 默认值                 |
| --------------------------------- | ----------------------------------------------------------------------------------------------------------- | ---------------------- |
| etcd.endpoints                    | etcd 服务接入端                                                                                             | "localhost:2379"       |
| minio.address                     | minio 服务地址                                                                                              | "localhost"            |
| minio.port                        | minio 服务端口                                                                                              | 9000                   |
| pulsar.address                    | pulsar 服务地址                                                                                             | "localhost"            |
| pulsar.port                       | pulsar 服务端口                                                                                             | 6650                   |
| log.level                         | 指定日志的输出级别。当前支持 `debug`，`info`，`warning`，`error`                                            | "info"                 |
| log.format                        | 指定日志的输出格式。当前支持 `text` 和 `json`                                                               | "text"                 |
| log.file.rootPath                 | 指定日志的存储路径                                                                                          | "/var/lib/milvus/logs" |
| log.file.maxSize                  | 日志文件的大小限制                                                                                          | 300MB                  |
| log.file.maxAge                   | 日志最大保留的天数。默认不清理旧的日志文件。如果设置该参数值，则会清理 `maxAge` 天前的日志文件。            | 0                      |
| log.file.maxBackups               | 保留日志文件的最大数量。默认保留所有旧的日志文件。如果设置该参数值为 `7`，则最多会保留 `7` 个旧的日志文件。 | 0                      |
| msgChannel.chanNamePrefix.cluster | 指定 pulsar 中 topic 前缀                                                                                   | "by-dev"               |
