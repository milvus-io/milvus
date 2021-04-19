## Entrypoint

### Syntax

Use the following syntax to run `milvus-distributed` commands from your terminal window:

```shell
milvus-distributed [command] [server type] [flags]
```

Example:

```bash
$ MILVUS_CONF_PATH=/path/to/milvus-distributed/configs milvus-distributed run master
```


<br/></br>
where `command`, `server type`, and `flags` are:
<br/></br>

`command`: Specifies the operation that you want to perform on server, for example `run`, `status`, `stop`

`server type`: Specifies the server type, `server type` are:

* `master`
* `msgstream`
* `proxyservice`
* `proxynode`
* `queryservice`
* `querynode`
* `dataservice`
* `datanode`
* `indexservice`
* `indexnode`
* `standalone`

`flags`: Specifies optional flags. For example, you can use the `-f` or `--config` flags to specify the configuration file.

> Getting help
> You can get help for CLI tool using the `--help` flag, or `-h` for short.
> ```shell
> $ milvus-distributed run master --help
> ```


### Environment


The table below lists the environment variables that you can use to configure the `milvus-distributed` tool.


|  Variable  | Description | Default |
| :-----:| :----: | :----: |
| MILVUS_CONF_PATH | Milvus configuration path | `/milvus-distributed/configs` |
