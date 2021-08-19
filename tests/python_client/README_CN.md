<h1 >测试框架使用指南</h1>
<h3 >简介</h3>
<p>基于 pytest 编写的 <strong>pymilvus-orm</strong> 的测试框架。</p>
<p><strong><em>测试代码：</em></strong><em><a href='https://github.com/milvus-io/milvus/tree/master/tests/python_client' target='_blank' class='url'>https://github.com/milvus-io/milvus/tree/master/tests/python_client</a></em></p>
<p>&nbsp;</p>
<h3 >快速开始</h3>
<h5 >部署 Milvus </h5>
<p>Milvus 支持4种部署方式，请根据需求选择部署方式，pymilvus_orm 支持任意部署下的 Milvus。</p>
<ol>
<li><a href='https://github.com/milvus-io/milvus/blob/master/DEVELOPMENT.md'>源码编译部署</a></li>
<li>Docker Compose 部署(<a href='https://milvus.io/cn/docs/v2.0.0/install_standalone-docker.md'>单机版本</a> <a href='https://milvus.io/cn/docs/v2.0.0/install_cluster-docker.md'>分布式版本</a>)</li>
<li>Kubernetes 部署(<a href='https://milvus.io/cn/docs/v2.0.0/install_standalone-helm.md'>单机版本</a> <a href='https://milvus.io/cn/docs/v2.0.0/install_cluster-helm.md'>分布式版本</a>)</li>
<li>KinD 部署</li>

</ol>
<p>KinD部署提供一键安装部署：最新的Milvus服务和测试客户端。KinD部署非常适合开发/调试测试用例，功能验证等对数据规模要求不大的场景，但并不适合性能或压力等有较大数据规模的场景。</p>
<ol>
<li>准备环境</li>

<ul>
<li><a href='https://github.com/milvus-io/milvus/blob/master/tests/README.md'>安装 Docker 、Docker Compose、jq、kubectl、helm、kind</a></li>

</ul>
<li>脚本安装</li>

</ol>
<ul>
<li>进入代码目录 */milvus/tests/scripts/</li>

</ul>
<ul>
<li>新建KinD环境，并自动执行CI Regression测试用例：</li>

</ul>
<pre><code> ./e2e-k8s.sh 
</code></pre>
<p>note：默认参数下KinD环境将在执行完测试用例后被自动清理</p>
<ul>
<li>如果需保留KinD环境，请使用--skip-cleanup参数：</li>

</ul>
<pre><code> ./e2e-k8s.sh --skip-cleanup
</code></pre>
<ul>
<li>如不需要自动执行测试用例，并保留KinD环境：</li>

</ul>
<pre><code> ./e2e-k8s.sh --skip-cleanup --skip-test --manual
</code></pre>
<p>Note：需要log in到测试客户端的container进行手动执行或调试测试用例</p>
<ul>
<li>更多脚本运行参数，请使用--help查看：</li>

</ul>
<pre><code> ./e2e-k8s.sh --help
</code></pre>
<ul>
<li>导出集群日志</li>

</ul>
<pre><code> kind export logs .
</code></pre>
<p>&nbsp;</p>
<h5 >pymilvus_orm 测试环境部署及用例执行</h5>
<p>推荐使用  <strong>Python 3(&gt;= 3.6)</strong> ，与 <strong><a href='https://pymilvus-orm.readthedocs.io/en/latest/install.html'>pymilvus_orm</a></strong> 支持的 python 版本保持一致。</p>
<p>Note: 如选择KinD部署方式，以下步骤可以自动完成。</p>
<ol>
<li>安装测试所需的 python 包，进入代码 <strong>*/milvus/tests/python_client/</strong> 目录，执行命令：</li>

<pre><code>pip install -r requirements.txt
</code></pre>
<li>在<strong>config</strong>目录下，测试的日志目录默认为：<strong>/tmp/ci_logs/</strong>，可在启动测试用例之前添加环境变量来修改log的存放路径：</li>

<pre><code>export CI_LOG_PATH=/tmp/ci_logs/test/
</code></pre>
<figure><table>
<thead>
<tr><th><strong>Log Level</strong> </th><th><strong>Log File</strong> </th></tr></thead>
<tbody><tr><td>Debug  </td><td>ci_test_log.debug </td></tr><tr><td>Info  </td><td>ci_test_log.log </td></tr><tr><td>Error  </td><td>ci_test_log.err </td></tr></tbody>
</table></figure>
<li>在主目录 <strong>pytest.ini</strong> 文件内可设置默认传递的参数，如下例中 ip 为所需要设置的 milvus 服务的ip地址，<strong>*.html</strong> 为测试生成的 <strong>report</strong>：</li>

<pre><code>addopts = --host *.*.*.*  --html=/tmp/ci_logs/report.html
</code></pre>
<li>进入 <strong>testcases</strong> 目录，命令与 <a href='https://docs.pytest.org/en/6.2.x/'>pytest</a> 框架的执行命令一致，运行如下命令执行测试用例：</li>

</ol>
<pre><code>python3 -W ignore -m pytest &lt;选择的测试文件&gt;
</code></pre>
<p>&nbsp;</p>
<h3 >模块介绍</h3>
<p><strong>模块调用关系图</strong></p>
<p><img src="https://github.com/milvus-io/milvus/blob/master/tests/python_client/graphs/module_call_diagram.jpg" referrerpolicy="no-referrer" alt="img"></p>
<h5 > 工作目录及文件介绍</h5>
<ul>
<li><strong>base</strong>：放置已封装好的 <strong>pymilvus-orm 模块文件</strong>，以及 pytest 框架的 setup 和 teardown 处理等</li>
<li><strong>check</strong>：接口返回结果的<strong>检查模块</strong></li>
<li><strong>common</strong>：测试用例<strong>通用的方法和参数</strong></li>
<li><strong>config</strong>：<strong>基础配置</strong>内容</li>
<li><strong>testcases</strong>：存放<strong>测试用例</strong>脚本</li>
<li><strong>utils</strong>：<strong>通用程序</strong>，如可用于全局的日志类、或者环境检查的方法等</li>
<li><strong>requirements</strong>：执行测试文件所<strong>依赖</strong>的 python 包</li>
<li><strong>conftest.py</strong>：编写<strong>装饰器函数</strong>，或者自己实现的<strong>本地插件</strong>，作用范围为该文件存放的目录及其子目录</li>
<li><strong>pytest.ini</strong>：pytest 的主<strong>配置</strong>文件</li>

</ul>
<p>&nbsp;</p>
<h5 >主要设计思路</h5>
<ul>
<li><strong>base/*_warpper.py</strong>: <strong>封装被测接口</strong>，统一处理接口请求，提取接口请求的返回结果，传入 <strong>check/func_check.py</strong> 模块进行结果检查。</li>
<li><strong>check/func_check.py</strong>: 模块编写各接口返回结果的检查方法，供测试用例调用。</li>
<li><strong>base/client_base.py</strong>: 模块使用pytest框架，进行相应的setup/teardown方法处理。</li>
<li><strong>testcases</strong> 目录下的测试文件，继承 <strong>base/client_base.py</strong> 里的 <strong>TestcaseBase</strong> 模块，进行相应的测试用例编写。用例里用到的通用参数和数据处理方法，写入<strong>common</strong>模块供用例调用。</li>
<li><strong>config</strong> 目录下加入一些全局配置，如日志的路径。</li>
<li><strong>utils</strong> 目录下负责实现全局的方法，如全局可用的日志模块。</li>
</ul>

<p>&nbsp;</p>
<h3 >代码添加</h3>
<p>可参考添加新的测试用例或框架工具。</p>
<p>&nbsp;</p>

<h5 >Python 测试代码添加注意事项</h5>


<ol>
<h6 >1. 测试编码风格</h6>
</ol>
<ul>
    <ul>
        <li>test 文件：每一个 SDK 类对应一个 test 文件，Load 和 Search 单独对应一个 test 文件</li>
    </ul>
    <ul>
        <li><p>test类：每一个 test 文件中分两个类</p>
            <ul>
                <li><p>TestObjectParams ：</p>
                    <ul>
                        <li>如 TestPartitionParams 表示 Partition Interface 参数检查测试用例类</li>
                        <li>检查在不同输入参数条件下，目标类/方法的表现，参数注意覆盖default，empty，none，datatype，maxsize边界值等</li>
                    </ul>
                </li>
            </ul>
            <ul>
                <li><p>TestObjectOperations: </p>
                    <ul>
                        <li>如 TestPartitionOperations 表示 Partition Interface 针对不同 function 或操作的测试</li>
                        <li>检查在合法输入参数，与其他接口有一定交互的条件下，目标类/方法的返回和表现</li>
                    </ul>
            </ul>
        </li>
    </ul>

<ul>
<li><p>testcase 命名</p>
<ul>
<li><p>TestObjectParams 类</p>
    <ul>
    <li>以testcase输入参数区分命名，如 test_partition_empty_name() 表示验证空字符串作为 name 参数输入的表现</li>
    </ul>
</li>

<li><p>TestObjectOperations 类</p>
    <ul>
    <li>以 testcase 操作步骤区分命名，如 test_partition_drop_partition_twice() 表示验证连续 drop 两次 partition 的表现</li>
    <li>以 testcase 验证点区分命名，如 test_partition_maximum_partitions() 表示验证创建  partition 的最大数量</li>
    </ul>
</li>

</ul>
</ul>
</ul>
<ul>
</ul>
<ol>

<h6 >2. 编码注意事项</h6>
</ol>
<ul>
<ul>
    <li>不能在测试用例文件中初始化 ORM 对象</li>
    <li>一般情况下，不在测试用例文件中直接添加日志代码</li>
    <li>在测试用例中，应直接调用封装好的方法或者属性，如下所示：</li>
</ul>

<p><em>如当需要创建多个 partition 对象时，可调用方法 self.init_partition_wrap()，该方法返回的结果就是新生成的 partition 对象。当无需创建多个对象时，直接使用 self.partition_wrap 即可</em></p>

<pre><code># create partition  -Call the default initialization method
partition_w = self.init_partition_wrap()
assert partition_w.is_empty
</code></pre>

<pre><code># create partition    -Directly call the encapsulated object
self.partition_wrap.init_partition(collection=collection_name, name=partition_name)
assert self.partition_wrap.is_empty
</code></pre>

<ul>
<li><p>验证接口返回错误或异常</p>
<ul>
    <li>check_task=CheckTasks.err_res</li>
    <li>输入期望的错误码和错误信息</li>
</ul>
</li>
</ul>

<pre><code># create partition with collection is None
self.partition_wrap.init_partition(collection=None, name=partition_name, check_task=CheckTasks.err_res, check_items={ct.err_code: 1, ct.err_msg: &quot;&#39;NoneType&#39; object has no attribute&quot;})
</code></pre>

<ul>
<li><p>验证接口返回正常返回值</p>
<ul>
    <li>check_task=CheckTasks.check_partition_property，可以在 CheckTasks 中新建校验方法，在用例中调用使用</li>
    <li>输入期望的结果，供校验方法使用</li>
</ul>
</li>
</ul>

<pre><code># create partition
partition_w = self.init_partition_wrap(collection_w, partition_name, check_task=CheckTasks.check_partition_property, check_items={&quot;name&quot;: partition_name, &quot;description&quot;: description, &quot;is_empty&quot;: True, &quot;num_entities&quot;: 0})
</code></pre>
</ul>
<ol>
<h6 >3. 测试用例添加</h6>
</ol>
<ul>
<ul>
    <li>在 base 文件夹的 wrapper 文件底下找到封装好的同名被测接口，各接口返回2个值的list，第一个是 pymilvus-orm 的接口返回结果，第二个是接口返回结果正常/异常的判断，为True/False。该返回可用于在用例中做额外的结果检查。</li>
    <li>在 testcases 文件夹下找到被测接口相应的测试文件，进行用例添加。如下所示，全部测试用例可直接参考 testcases 目录下的所有 test 文件：</li>
</ul>

<pre><code>    @pytest.mark.tags(CaseLabel.L1)
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
        self.partition_wrap.init_partition(collection_w.collection, partition_name, check_task=CheckTasks.err_res, check_items={ct.err_code: 1, ct.err_msg: "can't find collection"})
</code></pre>

<ul>
<li><p>Tips</p>
    <ul>
        <li>用例注释分为三个部分：目标，测试方法及期望结果，依此说明相应内容</li>
        <li>在 base/client_base.py 文件 Base 类的 setup 方法中对被测试的类进行了初始化，如下图所示：</li>
    </ul>
</li>

<pre><code>self.connection_wrap = ApiConnectionsWrapper()
self.utility_wrap = ApiUtilityWrapper()
self.collection_wrap = ApiCollectionWrapper()
self.partition_wrap = ApiPartitionWrapper()
self.index_wrap = ApiIndexWrapper()
self.collection_schema_wrap = ApiCollectionSchemaWrapper()
self.field_schema_wrap = ApiFieldSchemaWrapper()
</code></pre>

<ul>
    <li>调用需要测试的接口，应按照相应封装好的方法传入参数。如下所示，除了 check_task，check_items 两个参数外，其余参数与 pymilvus-orm 的接口参数一致。</li>
</ul>
<pre><code>def init_partition(self, collection, name, description="", check_task=None, check_items=None, **kwargs)
</code></pre>
<ul>
    <li>check_task 用来选择 check/func_check.py 文件中 ResponseChecker 检查类中对应的接口检查方法，可选择的方法在 common/common_type.py 文件的 CheckTasks 类中。</li>
    <li>check_items 传入检查方法所需的特定内容，具体内容由实现的检查方法所决定。</li>
    <li>默认不传这两个参数，则检查接口能正常返回请求结果。</li>
</ul>
</ul>
</ul>
<ol>
<h6 >4. 框架功能添加</h6>
</ol>
<ul>
<ul>
    <li>在 utils 目录下添加需要的全局方法或者工具</li>
    <li>可将相应的配置内容加入 config 目录下</li>
</ul>
</ul>

