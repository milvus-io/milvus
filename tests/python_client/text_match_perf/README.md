

使用方法：
首先cd到当前目录，然后执行以下命令：


通过bulk insert进行数据准备
```shell
python prepare_data.py
```
查看分词结果,选择词频最低的单词作为TextMatch的对象
```shell
python analyze_documents.py
```
输出
```
❯ python analyze_decuments.py
Analyze document cost time: 3.7922911643981934
('worker', 939)
```
将`test_text_match_query_perf.py`中的`TextMatch`对象替换为上面输出的单词
```python
filter = "TextMatch(word, 'worker')"
```
运行locust测试,注意修改host为自己的ip
```shell
 locust -f test_text_match_query_perf.py --processes 8 --host=http://10.104.1.10:19530 --headless
```
