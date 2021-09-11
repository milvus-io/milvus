# 测试框架使用指南

## 简介

基于 pytest 编写的 **PyMilvus** 的测试框架。

**测试代码：** https://github.com/milvus-io/milvus/tree/master/tests/python_client

## 快速开始

### 部署 Milvus

Milvus 支持4种部署方式，请根据需求选择部署方式，PyMilvus 支持任意部署下的 Milvus。

* [源码编译部署](https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md)
* Docker Compose 部署([单机版本](https://milvus.io/cn/docs/v2.0.0/install_standalone-docker.md) [分布式版本](https://milvus.io/cn/docs/v2.0.0/install_cluster-docker.md))
* Kubernetes 部署([单机版本](https://milvus.io/cn/docs/v2.0.0/install_standalone-helm.md) [分布式版本](https://milvus.io/cn/docs/v2.0.0/install_cluster-helm.md))
* KinD 部署

    KinD部署提供一键安装部署：最新的Milvus服务和测试客户端。KinD部署非常适合开发/调试测试用例，功能验证等对数据规模要求不大的场景，但并不适合性能或压力等有较大数据规模的场景。

    1. 准备环境

    2. [安装 Docker 、Docker Compose、jq、kubectl、helm、kind](https://github.com/milvus-io/milvus/blob/master/tests/README.md)

    3. 脚本安装

        - 进入代码目录 `*/milvus/tests/scripts/`

        - 新建KinD环境，并自动执行CI Regression测试用例：

            ```shell
            ./e2e-k8s.sh 
            ```

            > **_NOTE:_**  默认参数下KinD环境将在执行完测试用例后被自动清理
            

        - 如果需保留KinD环境，请使用--skip-cleanup参数:

            ```shell
            ./e2e-k8s.sh --skip-cleanup
            ```

        - 如不需要自动执行测试用例，并保留KinD环境：

            ```shell
            ./e2e-k8s.sh --skip-cleanup --skip-test --manual
            ```

            > **_NOTE:_**  需要login到测试客户端的container进行手动执行或调试测试用例

        - 更多脚本运行参数，请使用--help查看：

            ```shell
            ./e2e-k8s.sh --help
            ```

        - 导出集群日志

            ```shell
            kind export logs .
            ```


### PyMilvus 测试环境部署及用例执行

推荐使用 **Python 3(>= 3.8)** ，与 **[PyMilvus](https://pymilvus.readthedocs.io/en/latest/install.html)** 支持的 Python 版本保持一致。

> **_NOTE:_**  如选择KinD部署方式，以下步骤可以自动完成

1. 安装测试所需的 Python 包，进入代码 `*/milvus/tests/python_client/` 目录，执行命令：

    ```shell
    pip install -r requirements.txt
    ```

2. 在`config`目录下，测试的日志目录默认为：`/tmp/ci_logs/`，可在启动测试用例之前添加环境变量来修改log的存放路径：

    ```shell
    export CI_LOG_PATH=/tmp/ci_logs/test/
    ```

    | **Log Level** | **Log File**      |
    | ------------- | ----------------- |
    | Debug         | ci_test_log.debug |
    | Info          | ci_test_log.log   |
    | Error         | ci_test_log.err   |

3. 在主目录 `pytest.ini` 文件内可设置默认传递的参数，如下例中 ip 为所需要设置的 milvus 服务的ip地址，`*.html` 为测试生成的 `report`：

    ```shell
    addopts = --host *.*.*.*  --html=/tmp/ci_logs/report.html
    ```

4. 进入 `testcases` 目录，命令与 [pytest](https://docs.pytest.org/en/6.2.x/) 框架的执行命令一致，运行如下命令执行测试用例：

    ```shell
    python3 -W ignore -m pytest <选择的测试文件>
    ```



## 模块介绍

**模块调用关系图**

[![img](https://github.com/milvus-io/milvus/raw/master/tests/python_client/graphs/module_call_diagram.jpg)](https://github.com/milvus-io/milvus/blob/master/tests/python_client/graphs/module_call_diagram.jpeg)

### 工作目录及文件介绍

- **base**：放置已封装好的 **PyMilvus 模块文件**，以及 Pytest 框架的 setup 和 teardown 处理等
- **check**：接口返回结果的**检查模块**
- **common**：测试用例**通用的方法和参数**
- **config**：**基础配置**内容
- **testcases**：存放**测试用例**脚本
- **utils**：**通用程序**，如可用于全局的日志类、或者环境检查的方法等
- **requirements**：执行测试文件所**依赖**的 python 包
- **conftest.py**：编写**装饰器函数**，或者自己实现的**本地插件**，作用范围为该文件存放的目录及其子目录
- **pytest.ini**：pytest 的主**配置**文件

 

### 主要设计思路

- **base/\*_warpper.py**: **封装被测接口**，统一处理接口请求，提取接口请求的返回结果，传入 **check/func_check.py** 模块进行结果检查。
- **check/func_check.py**: 模块编写各接口返回结果的检查方法，供测试用例调用。
- **base/client_base.py**: 模块使用pytest框架，进行相应的setup/teardown方法处理。
- **testcases** 目录下的测试文件，继承 **base/client_base.py** 里的 **TestcaseBase** 模块，进行相应的测试用例编写。用例里用到的通用参数和数据处理方法，写入**common**模块供用例调用。
- **config** 目录下加入一些全局配置，如日志的路径。
- **utils** 目录下负责实现全局的方法，如全局可用的日志模块。

 

## 代码添加

可参考添加新的测试用例或框架工具。

 

### Python 测试代码添加注意事项

#### 测试编码风格

- test 文件：每一个 SDK 类对应一个 test 文件，Load 和 Search 单独对应一个 test 文件

- test类：每一个 test 文件中分两个类

    - TestObjectParams ：
        - 如 TestPartitionParams 表示 Partition Interface 参数检查测试用例类
        - 检查在不同输入参数条件下，目标类/方法的表现，参数注意覆盖default，empty，none，datatype，maxsize边界值等

    - TestObjectOperations:
        - 如 TestPartitionOperations 表示 Partition Interface 针对不同 function 或操作的测试
        - 检查在合法输入参数，与其他接口有一定交互的条件下，目标类/方法的返回和表现

- testcase 命名
    - TestObjectParams 类
        - 以testcase输入参数区分命名，如 test_partition_empty_name() 表示验证空字符串作为 name 参数输入的表现
    - TestObjectOperations 类
        - 以 testcase 操作步骤区分命名，如 test_partition_drop_partition_twice() 表示验证连续 drop 两次 partition 的表现
        - 以 testcase 验证点区分命名，如 test_partition_maximum_partitions() 表示验证创建 partition 的最大数量


#### 编码注意事项

- 不能在测试用例文件中初始化 PyMilvus 对象
    - 一般情况下，不在测试用例文件中直接添加日志代码
    - 在测试用例中，应直接调用封装好的方法或者属性，如下所示：
 
        > **_NOTE:_** 如当需要创建多个 partition 对象时，可调用方法 self.init_partition_wrap()，该方法返回的结果就是新生成的 partition 对象。当无需创建多个对象时，直接使用 self.partition_wrap 即可。

        ```python
        # create partition  -Call the default initialization method
        partition_w = self.init_partition_wrap()
        assert partition_w.is_empty
        ```

        ```python
        # create partition    -Directly call the encapsulated object
        self.partition_wrap.init_partition(collection=collection_name, name=partition_name)
        assert self.partition_wrap.is_empty
        ```

- 验证接口返回错误或异常
    - check_task=CheckTasks.err_res
    - 输入期望的错误码和错误信息

        ```python
        # create partition with collection is None
        self.partition_wrap.init_partition(collection=None, name=partition_name, check_task=CheckTasks.err_res, check_items={ct.err_code: 1, ct.err_msg: "'NoneType' object has no attribute"})
        ```

- 验证接口返回正常返回值
    - check_task=CheckTasks.check_partition_property，可以在 CheckTasks 中新建校验方法，在用例中调用使用
    - 输入期望的结果，供校验方法使用

        ```python
        # create partition
        partition_w = self.init_partition_wrap(collection_w, partition_name, check_task=CheckTasks.check_partition_property, check_items={"name": partition_name, "description": description, "is_empty": True, "num_entities": 0})
        ```

#### 测试用例添加

- 在 base 文件夹的 wrapper 文件底下找到封装好的同名被测接口，各接口返回2个值的list，第一个是 PyMilvus 的接口返回结果，第二个是接口返回结果正常/异常的判断，为True/False。该返回可用于在用例中做额外的结果检查。
- 在 testcases 文件夹下找到被测接口相应的测试文件，进行用例添加。如下所示，全部测试用例可直接参考 testcases 目录下的所有 test 文件：

    ```python
    @pytest.mark.tags(CaseLabel.L1)
    @pytest.mark.parametrize("partition_name", [cf.gen_unique_str(prefix)])
    def test_partition_dropped_collection(self, partition_name):
        """
        target: verify create partition against a dropped collection
        method: 1. create collection1
                2. drop collection1
                3. create partition in collection1
        expected: 1. raise exception
        """

        # create collection
        collection_w = self.init_collection_wrap()

        # drop collection
        collection_w.drop()

        # create partition failed
        self.partition_wrap.init_partition(
                                            collection_w.collection, 
                                            partition_name, 
                                            check_task=CheckTasks.err_res, 
                                            check_items={
                                                        ct.err_code: 1, 
                                                        ct.err_msg: "can't find collection"})
    ```

- Tips

    - 用例注释分为三个部分：目标，测试方法及期望结果，依此说明相应内容
    - 在 base/client_base.py 文件 Base 类的 setup 方法中对被测试的类进行了初始化，如下图所示：

        ```python
        self.connection_wrap = ApiConnectionsWrapper()
        self.utility_wrap = ApiUtilityWrapper()
        self.collection_wrap = ApiCollectionWrapper()
        self.partition_wrap = ApiPartitionWrapper()
        self.index_wrap = ApiIndexWrapper()
        self.collection_schema_wrap = ApiCollectionSchemaWrapper()
        self.field_schema_wrap = ApiFieldSchemaWrapper()
        ```

    - 调用需要测试的接口，应按照相应封装好的方法传入参数。如下所示，除了 check_task，check_items 两个参数外，其余参数与 PyMilvus 的接口参数一致。

        ```python
        def init_partition(self, collection, name, description="", check_task=None, check_items=None, **kwargs)
        ```

    - check_task 用来选择 check/func_check.py 文件中 ResponseChecker 检查类中对应的接口检查方法，可选择的方法在 common/common_type.py 文件的 CheckTasks 类中。
    - check_items 传入检查方法所需的特定内容，具体内容由实现的检查方法所决定。
    - 默认不传这两个参数，则检查接口能正常返回请求结果。

#### 框架功能添加

- 在 utils 目录下添加需要的全局方法或者工具
- 可将相应的配置内容加入 config 目录下




