# Quick Start

- For calculating L2 or IP distance of feature vectors.
- At below table, the last five parameters  do not need to alter.

### Parameter Description：

| parameter          | description                                   | default setting      |
| ------------------ | --------------------------------------------- | -------------------- |
| GET_VEC            | whether to save feature vectors               | False                |
| PROCESS_NUM        | number of processes                           | 5                    |
| IP                 | whether metric_type is IP                     | True                 |
| L2                 | whether metric_type is L2                     | False                |
| CSV                | whether the query vector file format is csv   | False                |
| UINT8              | whether the query vector data format is uint8 | False                |
| BASE_FOLDER_NAME   | path to the source vector dataset             | '/data/milvus/base'  |
| NQ_FOLDER_NAME     | path to the query vector dataset              | '/data/milvus/query' |
| GT_ALL_FOLDER_NAME | intermediate filename                         | 'ground_truth_all'   |
| GT_FOLDER_NAME     | path saved the ground truth results           | 'ground_truth'       |
| LOC_FILE_NAME      | file saved the gorund truth's location info   | 'location.txt'       |
| FLOC_FILE_NAME     | file saved the gorund truth's filenames info  | 'file_location.txt'  |
| VEC_FILE_NAME      | file saved the gorund truth's feature vectors | 'vectors.npy'        |

### Usage：

```bash
$ python3 milvus_ground_truth.py [-q <nq_num>] -k <topk_num> -l

# -q or --nq points the number of vectors taken from the query vector set. This parameter is optional, Without it will take all the data in the query set.
# -k or --topk points calculate the top k similar vectors.
# -l means generate the ground truth results, it will save in GT_FOLDER_NAME.In this path, LOC_FILE_NAME saved the gorund truth's location info, such as "8002005210",the first ‘8’ is meaningless, the 2-4th position means the position of the result file in the folder, the 5-10th position means the position of the result vector in the result file. The result filename and vector location saved in FLOC_FILE_NAME, such as "binary_128d_00000.npy 81759", and the result vector is saved in VEC_FILE_NAME.
```