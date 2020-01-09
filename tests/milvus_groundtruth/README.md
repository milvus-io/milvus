# 运行说明

- 用于计算向量数据的 L2 和 IP 结果
- 由于使用 python 多进程计算结果，下表中后五个参数是结果参数，无需修改

### 参数说明：

| 参数               | 描述                             | 默认设置             |
| ------------------ | -------------------------------- | -------------------- |
| GET_VEC            | 是否保存为npy格式的向量          | False                |
| PROCESS_NUM        | 脚本执行的进程数                 | 5                    |
| IP                 | Milvus的metric_type是否为IP      | True                 |
| L2                 | Milvus的metric_type是否为L2      | False                |
| CSV                | 查询向量文件格式是否为.csv       | False                |
| UINT8              | 查询向量是否为uint8格式          | False                |
| BASE_FOLDER_NAME   | 源向量数据集的路径               | '/data/milvus/base'  |
| NQ_FOLDER_NAME     | 查询向量集的路径                 | '/data/milvus/query' |
| GT_ALL_FOLDER_NAME | 执行多进程产生的中间文件         | 'ground_truth_all'   |
| GT_FOLDER_NAME     | gorund truth结果保存的路径       | 'ground_truth'       |
| LOC_FILE_NAME      | 该文件存储gorund truth的位置信息 | 'location.txt'       |
| FLOC_FILE_NAME     | 该文件存储gorund truth的文件信息 | 'file_location.txt'  |
| VEC_FILE_NAME      | 该文件存储gorund truth向量       | 'vectors.npy'        |

### 使用说明：

```bash
$ python3 milvus_ground_truth.py [-q <nq_num>] -k <topk_num> -l

# 执行-l生成源向量数据集的ground truth，并将结果写入GT_FOLDER_NAME目录,其中LOC_FILE_NAME存储着数字位置，如"8002005210"（首位‘8’无意义，第2-4位表示结果文件在文件夹中的位置，第5-10位表示结果向量在结果文件中的位置），FLOC_FILE_NAME中存着结果对应的结果文件名和结果向量位置，如"binary_128d_00000.npy 81759"，VEC_FILE_NAME中存有结果向量
# -q或者--nq表示从查询向量集中按序取出的向量个数，该参数可选，若没有-q表示ground truth需查询向量为查询集中的全部数据
# -k或者--topk表示ground truth需查询每个向量的前k个相似的向量
```