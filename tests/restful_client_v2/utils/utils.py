import random
import time
import random
import string
from faker import Faker
import numpy as np
from ml_dtypes import bfloat16
from sklearn import preprocessing
import base64
import requests
from loguru import logger
import datetime
from sklearn.metrics import pairwise_distances
from collections import Counter
import bm25s
import jieba


fake = Faker()
fake.seed_instance(19530)
rng = np.random.default_rng()


en_vocabularies_distribution = {
    "hello": 0.01,
    "milvus": 0.01,
    "vector": 0.01,
    "database": 0.01
}

zh_vocabularies_distribution = {
    "你好": 0.01,
    "向量": 0.01,
    "数据": 0.01,
    "库": 0.01
}


def patch_faker_text(fake_instance, vocabularies_distribution):
    """
    Monkey patch the text() method of a Faker instance to include custom vocabulary.
    Each word in vocabularies_distribution has an independent chance to be inserted.
    Args:
        fake_instance: Faker instance to patch
        vocabularies_distribution: Dictionary where:
            - key: word to insert
            - value: probability (0-1) of inserting this word into each sentence
    Example:
        vocabularies_distribution = {
            "hello": 0.1,    # 10% chance to insert "hello" in each sentence
            "milvus": 0.1,   # 10% chance to insert "milvus" in each sentence
        }
    """
    original_text = fake_instance.text

    def new_text(*args, **kwargs):
        sentences = []
        # Split original text into sentences
        original_sentences = original_text(*args,**kwargs).split('.')
        original_sentences = [s.strip() for s in original_sentences if s.strip()]

        for base_sentence in original_sentences:
            words = base_sentence.split()

            # Independently decide whether to insert each word
            for word, probability in vocabularies_distribution.items():
                if random.random() < probability:
                    # Choose random position to insert the word
                    insert_pos = random.randint(0, len(words))
                    words.insert(insert_pos, word)

            # Reconstruct the sentence
            base_sentence = ' '.join(words)

            # Ensure proper capitalization
            base_sentence = base_sentence[0].upper() + base_sentence[1:]
            sentences.append(base_sentence)

        return '. '.join(sentences) + '.'

    # Replace the original text method with our custom one
    fake_instance.text = new_text


def analyze_documents(texts, language="en"):
    stopwords = "en"
    if language in ["en", "english"]:
        stopwords = "en"
    if language in ["zh", "cn", "chinese"]:
        stopword = " "
        new_texts = []
        for doc in texts:
            seg_list = jieba.cut(doc, cut_all=True)
            new_texts.append(" ".join(seg_list))
        texts = new_texts
        stopwords = [stopword]
    # Start timing
    t0 = time.time()

    # Tokenize the corpus
    tokenized = bm25s.tokenize(texts, lower=True, stopwords=stopwords)
    # log.info(f"Tokenized: {tokenized}")
    # Create a frequency counter
    freq = Counter()

    # Count the frequency of each token
    for doc_ids in tokenized.ids:
        freq.update(doc_ids)
    # Create a reverse vocabulary mapping
    id_to_word = {id: word for word, id in tokenized.vocab.items()}

    # Convert token ids back to words
    word_freq = Counter({id_to_word[token_id]: count for token_id, count in freq.items()})

    # End timing
    tt = time.time() - t0
    logger.info(f"Analyze document cost time: {tt}")

    return word_freq


def random_string(length=8):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))


def gen_collection_name(prefix="test_collection", length=8):
    name = f'{prefix}_' + datetime.datetime.now().strftime("%Y_%m_%d_%H_%M_%S_%f") + random_string(length=length)
    return name

def admin_password():
    return "Milvus"


def gen_unique_str(prefix="test", length=8):
    return prefix + "_" + random_string(length=length)


def invalid_cluster_name():
    res = [
        "demo" * 100,
        "demo" + "!",
        "demo" + "@",
    ]
    return res


def wait_cluster_be_ready(cluster_id, client, timeout=120):
    t0 = time.time()
    while True and time.time() - t0 < timeout:
        rsp = client.cluster_describe(cluster_id)
        if rsp['code'] == 200:
            if rsp['data']['status'] == "RUNNING":
                return time.time() - t0
        time.sleep(1)
        logger.debug("wait cluster to be ready, cost time: %s" % (time.time() - t0))
    return -1





def gen_data_by_type(field):
    data_type = field["type"]
    if data_type == "bool":
        return random.choice([True, False])
    if data_type == "int8":
        return random.randint(-128, 127)
    if data_type == "int16":
        return random.randint(-32768, 32767)
    if data_type == "int32":
        return random.randint(-2147483648, 2147483647)
    if data_type == "int64":
        return random.randint(-9223372036854775808, 9223372036854775807)
    if data_type == "float32":
        return np.float64(random.random())  # Object of type float32 is not JSON serializable, so set it as float64
    if data_type == "float64":
        return np.float64(random.random())
    if "varchar" in data_type:
        length = int(data_type.split("(")[1].split(")")[0])
        return "".join([chr(random.randint(97, 122)) for _ in range(length)])
    if "floatVector" in data_type:
        dim = int(data_type.split("(")[1].split(")")[0])
        return preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
    return None


def get_data_by_fields(fields, nb):
    # logger.info(f"fields: {fields}")
    fields_not_auto_id = []
    for field in fields:
        if not field.get("autoId", False):
            fields_not_auto_id.append(field)
    # logger.info(f"fields_not_auto_id: {fields_not_auto_id}")
    data = []
    for i in range(nb):
        tmp = {}
        for field in fields_not_auto_id:
            tmp[field["name"]] = gen_data_by_type(field)
        data.append(tmp)
    return data


def get_random_json_data(uid=None):
    # gen random dict data
    if uid is None:
        uid = 0
    data = {"uid": uid,  "name": fake.name(), "address": fake.address(), "text": fake.text(), "email": fake.email(),
            "phone_number": fake.phone_number(),
            "json": {
                "name": fake.name(),
                "address": fake.address()
                }
            }
    for i in range(random.randint(1, 10)):
        data["key" + str(random.randint(1, 100_000))] = "value" + str(random.randint(1, 100_000))
    return data


def get_data_by_payload(payload, nb=100):
    dim = payload.get("dimension", 128)
    vector_field = payload.get("vectorField", "vector")
    pk_field = payload.get("primaryField", "id")
    data = []
    if nb == 1:
        data = [{
            pk_field: int(time.time()*10000),
            vector_field: preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist(),
            **get_random_json_data()

        }]
    else:
        for i in range(nb):
            data.append({
                pk_field: int(time.time()*10000),
                vector_field: preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist(),
                **get_random_json_data(uid=i)
            })
    return data


def get_common_fields_by_data(data, exclude_fields=None):
    fields = set()
    if isinstance(data, dict):
        data = [data]
    if not isinstance(data, list):
        raise Exception("data must be list or dict")
    common_fields = set(data[0].keys())
    for d in data:
        keys = set(d.keys())
        common_fields = common_fields.intersection(keys)
    if exclude_fields is not None:
        exclude_fields = set(exclude_fields)
        common_fields = common_fields.difference(exclude_fields)
    return list(common_fields)


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for _ in range(num):
        raw_vector = [random.randint(0, 1) for _ in range(dim)]
        raw_vectors.append(raw_vector)
        # packs a binary-valued array into bits in a unit8 array, and bytes array_of_ints
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


def gen_fp16_vectors(num, dim):
    """
    generate float16 vector data
    raw_vectors : the vectors
    fp16_vectors: the bytes used for insert
    return: raw_vectors and fp16_vectors
    """
    raw_vectors = []
    fp16_vectors = []
    for _ in range(num):
        raw_vector = [random.random() for _ in range(dim)]
        raw_vectors.append(raw_vector)
        fp16_vector = np.array(raw_vector, dtype=np.float16).view(np.uint8).tolist()
        fp16_vectors.append(bytes(fp16_vector))

    return raw_vectors, fp16_vectors


def gen_bf16_vectors(num, dim):
    """
    generate brain float16 vector data
    raw_vectors : the vectors
    bf16_vectors: the bytes used for insert
    return: raw_vectors and bf16_vectors
    """
    raw_vectors = []
    bf16_vectors = []
    for _ in range(num):
        raw_vector = [random.random() for _ in range(dim)]
        raw_vectors.append(raw_vector)
        bf16_vector = np.array(raw_vector, dtype=bfloat16).view(np.uint8).tolist()
        bf16_vectors.append(bytes(bf16_vector))

    return raw_vectors, bf16_vectors


def gen_vector(datatype="FloatVector", dim=128, binary_data=False, sparse_format='dok'):
    value = None
    if datatype == "FloatVector":
        return preprocessing.normalize([np.array([random.random() for i in range(dim)])])[0].tolist()
    if datatype == "SparseFloatVector":
        if sparse_format == 'dok':
            return {d: rng.random() for d in random.sample(range(dim), random.randint(20, 30))}
        elif sparse_format == 'coo':
            data = {d: rng.random() for d in random.sample(range(dim), random.randint(20, 30))}
            coo_data = {
                "indices": list(data.keys()),
                "values": list(data.values())
            }
            return coo_data
        else:
            raise Exception(f"unsupported sparse format: {sparse_format}")
    if datatype == "BinaryVector":
        value = gen_binary_vectors(1, dim)[1][0]
    if datatype == "Float16Vector":
        value = gen_fp16_vectors(1, dim)[1][0]
    if datatype == "BFloat16Vector":
        value = gen_bf16_vectors(1, dim)[1][0]
    if value is None:
        raise Exception(f"unsupported datatype: {datatype}")
    else:
        if binary_data:
            return value
        else:
            data = base64.b64encode(value).decode("utf-8")
            return data


def get_all_fields_by_data(data, exclude_fields=None):
    fields = set()
    for d in data:
        keys = list(d.keys())
        fields.union(keys)
    if exclude_fields is not None:
        exclude_fields = set(exclude_fields)
        fields = fields.difference(exclude_fields)
    return list(fields)


def ip_distance(x, y):
    return np.dot(x, y)


def cosine_distance(u, v, epsilon=1e-8):
    dot_product = np.dot(u, v)
    norm_u = np.linalg.norm(u)
    norm_v = np.linalg.norm(v)
    return dot_product / (max(norm_u * norm_v, epsilon))


def l2_distance(u, v):
    return np.sum((u - v) ** 2)


def get_sorted_distance(train_emb, test_emb, metric_type):
    milvus_sklearn_metric_map = {
        "L2": l2_distance,
        "COSINE": cosine_distance,
        "IP": ip_distance
    }
    distance = pairwise_distances(train_emb, Y=test_emb, metric=milvus_sklearn_metric_map[metric_type], n_jobs=-1)
    distance = np.array(distance.T, order='C', dtype=np.float32)
    distance_sorted = np.sort(distance, axis=1).tolist()
    return distance_sorted
