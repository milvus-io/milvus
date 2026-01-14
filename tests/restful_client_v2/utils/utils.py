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


# ============= Geometry Utils =============

def generate_wkt_by_type(wkt_type: str, bounds: tuple = (0, 100, 0, 100), count: int = 10) -> list:
    """
    Generate WKT examples dynamically based on geometry type

    Args:
        wkt_type: Type of WKT geometry to generate (POINT, LINESTRING, POLYGON, MULTIPOINT, MULTILINESTRING, MULTIPOLYGON, GEOMETRYCOLLECTION)
        bounds: Coordinate bounds as (min_x, max_x, min_y, max_y)
        count: Number of geometries to generate

    Returns:
        List of WKT strings
    """
    if wkt_type == "POINT":
        points = []
        for _ in range(count):
            wkt_string = f"POINT ({random.uniform(bounds[0], bounds[1]):.2f} {random.uniform(bounds[2], bounds[3]):.2f})"
            points.append(wkt_string)
        return points

    elif wkt_type == "LINESTRING":
        lines = []
        for _ in range(count):
            points = []
            num_points = random.randint(2, 6)
            for _ in range(num_points):
                x = random.uniform(bounds[0], bounds[1])
                y = random.uniform(bounds[2], bounds[3])
                points.append(f"{x:.2f} {y:.2f}")
            wkt_string = f"LINESTRING ({', '.join(points)})"
            lines.append(wkt_string)
        return lines

    elif wkt_type == "POLYGON":
        polygons = []
        for _ in range(count):
            if random.random() < 0.7:  # 70% rectangles
                x = random.uniform(bounds[0], bounds[1] - 50)
                y = random.uniform(bounds[2], bounds[3] - 50)
                width = random.uniform(10, 50)
                height = random.uniform(10, 50)
                polygon_wkt = f"POLYGON (({x:.2f} {y:.2f}, {x + width:.2f} {y:.2f}, {x + width:.2f} {y + height:.2f}, {x:.2f} {y + height:.2f}, {x:.2f} {y:.2f}))"
            else:  # 30% triangles
                x1, y1 = random.uniform(bounds[0], bounds[1]), random.uniform(bounds[2], bounds[3])
                x2, y2 = random.uniform(bounds[0], bounds[1]), random.uniform(bounds[2], bounds[3])
                x3, y3 = random.uniform(bounds[0], bounds[1]), random.uniform(bounds[2], bounds[3])
                polygon_wkt = f"POLYGON (({x1:.2f} {y1:.2f}, {x2:.2f} {y2:.2f}, {x3:.2f} {y3:.2f}, {x1:.2f} {y1:.2f}))"
            polygons.append(polygon_wkt)
        return polygons

    elif wkt_type == "MULTIPOINT":
        multipoints = []
        for _ in range(count):
            points = []
            num_points = random.randint(2, 8)
            for _ in range(num_points):
                x = random.uniform(bounds[0], bounds[1])
                y = random.uniform(bounds[2], bounds[3])
                points.append(f"({x:.2f} {y:.2f})")
            wkt_string = f"MULTIPOINT ({', '.join(points)})"
            multipoints.append(wkt_string)
        return multipoints

    elif wkt_type == "MULTILINESTRING":
        multilines = []
        for _ in range(count):
            lines = []
            num_lines = random.randint(2, 5)
            for _ in range(num_lines):
                line_points = []
                num_points = random.randint(2, 4)
                for _ in range(num_points):
                    x = random.uniform(bounds[0], bounds[1])
                    y = random.uniform(bounds[2], bounds[3])
                    line_points.append(f"{x:.2f} {y:.2f}")
                lines.append(f"({', '.join(line_points)})")
            wkt_string = f"MULTILINESTRING ({', '.join(lines)})"
            multilines.append(wkt_string)
        return multilines

    elif wkt_type == "MULTIPOLYGON":
        multipolygons = []
        for _ in range(count):
            polygons = []
            num_polygons = random.randint(2, 4)
            for _ in range(num_polygons):
                x = random.uniform(bounds[0], bounds[1] - 30)
                y = random.uniform(bounds[2], bounds[3] - 30)
                size = random.uniform(10, 30)
                polygon_coords = f"(({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))"
                polygons.append(polygon_coords)
            wkt_string = f"MULTIPOLYGON ({', '.join(polygons)})"
            multipolygons.append(wkt_string)
        return multipolygons

    elif wkt_type == "GEOMETRYCOLLECTION":
        collections = []
        for _ in range(count):
            collection_types = random.randint(2, 4)
            geoms = []

            for _ in range(collection_types):
                geom_type = random.choice(["POINT", "LINESTRING", "POLYGON"])
                if geom_type == "POINT":
                    x, y = random.uniform(bounds[0], bounds[1]), random.uniform(bounds[2], bounds[3])
                    geoms.append(f"POINT({x:.2f} {y:.2f})")
                elif geom_type == "LINESTRING":
                    x1, y1 = random.uniform(bounds[0], bounds[1]), random.uniform(bounds[2], bounds[3])
                    x2, y2 = random.uniform(bounds[0], bounds[1]), random.uniform(bounds[2], bounds[3])
                    geoms.append(f"LINESTRING({x1:.2f} {y1:.2f}, {x2:.2f} {y2:.2f})")
                else:  # POLYGON
                    x, y = random.uniform(bounds[0], bounds[1] - 20), random.uniform(bounds[2], bounds[3] - 20)
                    size = random.uniform(5, 20)
                    geoms.append(f"POLYGON(({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))")

            wkt_string = f"GEOMETRYCOLLECTION({', '.join(geoms)})"
            collections.append(wkt_string)
        return collections

    else:
        raise ValueError(f"Unsupported WKT type: {wkt_type}")


def generate_diverse_base_data(count=100, bounds=(0, 100, 0, 100), pk_field_name="id", geo_field_name="geo"):
    """
    Generate diverse base geometry data for testing

    Args:
        count: Number of geometries to generate (default: 100)
        bounds: Coordinate bounds as (min_x, max_x, min_y, max_y)
        pk_field_name: Name of the primary key field (default: "id")
        geo_field_name: Name of the geometry field (default: "geo")

    Returns:
        List of geometry data with format [{pk_field_name: int, geo_field_name: "WKT_STRING"}, ...]
    """
    base_data = []
    min_x, max_x, min_y, max_y = bounds

    # Generate points (30% of data)
    point_count = int(count * 0.3)
    for _ in range(point_count):
        x = random.uniform(min_x, max_x)
        y = random.uniform(min_y, max_y)
        wkt_string = f"POINT ({x:.2f} {y:.2f})"
        base_data.append({pk_field_name: len(base_data), geo_field_name: wkt_string})

    # Generate polygons (40% of data)
    polygon_count = int(count * 0.4)
    for _ in range(polygon_count):
        size = random.uniform(5, 20)
        x = random.uniform(min_x, max_x - size)
        y = random.uniform(min_y, max_y - size)
        wkt_string = f"POLYGON (({x:.2f} {y:.2f}, {x + size:.2f} {y:.2f}, {x + size:.2f} {y + size:.2f}, {x:.2f} {y + size:.2f}, {x:.2f} {y:.2f}))"
        base_data.append({pk_field_name: len(base_data), geo_field_name: wkt_string})

    # Generate linestrings (25% of data)
    line_count = int(count * 0.25)
    for _ in range(line_count):
        point_count_per_line = random.randint(2, 4)
        coords = []
        for _ in range(point_count_per_line):
            x = random.uniform(min_x, max_x)
            y = random.uniform(min_y, max_y)
            coords.append(f"{x:.2f} {y:.2f}")
        wkt_string = f"LINESTRING ({', '.join(coords)})"
        base_data.append({pk_field_name: len(base_data), geo_field_name: wkt_string})

    # Add some specific geometries for edge cases
    remaining = count - len(base_data)
    if remaining > 0:
        # Add duplicate points for ST_EQUALS testing
        if len(base_data) > 0 and "POINT" in base_data[0][geo_field_name]:
            base_data.append({pk_field_name: len(base_data), geo_field_name: base_data[0][geo_field_name]})
            remaining -= 1

        # Fill remaining with random points
        for _ in range(remaining):
            x = random.uniform(min_x, max_x)
            y = random.uniform(min_y, max_y)
            wkt_string = f"POINT ({x:.2f} {y:.2f})"
            base_data.append({pk_field_name: len(base_data), geo_field_name: wkt_string})

    return base_data


def generate_spatial_query_data_for_function(spatial_func, base_data, geo_field_name="geo"):
    """
    Generate query geometry for specific spatial function based on base data
    Ensures the query will match multiple results (>1)

    Args:
        spatial_func: The spatial function name (e.g., "ST_INTERSECTS", "ST_CONTAINS")
        base_data: List of base geometry data with format [{"id": int, geo_field_name: "WKT_STRING"}, ...]
        geo_field_name: Name of the geometry field in base_data (default: "geo")

    Returns:
        query_geom: WKT string of the query geometry that should match multiple base geometries
    """
    import re

    def parse_point(wkt):
        """Extract x, y from POINT WKT"""
        match = re.search(r"POINT \(([0-9.-]+) ([0-9.-]+)\)", wkt)
        if match:
            return float(match.group(1)), float(match.group(2))
        return None, None

    def parse_polygon_bounds(wkt):
        """Extract min/max bounds from POLYGON WKT"""
        match = re.search(r"POLYGON \(\(([^)]+)\)\)", wkt)
        if match:
            coords = match.group(1).split(", ")
            xs, ys = [], []
            for coord in coords:
                parts = coord.strip().split()
                if len(parts) >= 2:
                    xs.append(float(parts[0]))
                    ys.append(float(parts[1]))
            if xs and ys:
                return min(xs), max(xs), min(ys), max(ys)
        return None, None, None, None

    if spatial_func == "ST_INTERSECTS":
        # Create a large query polygon that will intersect with many geometries
        all_coords = []
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                x, y = parse_point(item[geo_field_name])
                if x is not None and y is not None:
                    all_coords.append((x, y))
            elif "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    all_coords.append(((min_x + max_x) / 2, (min_y + max_y) / 2))

        if all_coords and len(all_coords) >= 5:
            target_coords = all_coords[:min(10, len(all_coords))]
            center_x = sum(coord[0] for coord in target_coords) / len(target_coords)
            center_y = sum(coord[1] for coord in target_coords) / len(target_coords)
            size = 40
            query_geom = f"POLYGON (({center_x - size / 2} {center_y - size / 2}, {center_x + size / 2} {center_y - size / 2}, {center_x + size / 2} {center_y + size / 2}, {center_x - size / 2} {center_y + size / 2}, {center_x - size / 2} {center_y - size / 2}))"
        else:
            query_geom = "POLYGON ((30 30, 70 30, 70 70, 30 70, 30 30))"

    elif spatial_func == "ST_CONTAINS":
        # Create a query polygon that contains multiple points
        points = []
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                x, y = parse_point(item[geo_field_name])
                if x is not None and y is not None:
                    points.append((x, y))

        if len(points) >= 3:
            target_points = points[:min(10, len(points))]
            min_x = min(p[0] for p in target_points) - 5
            max_x = max(p[0] for p in target_points) + 5
            min_y = min(p[1] for p in target_points) - 5
            max_y = max(p[1] for p in target_points) + 5
            query_geom = f"POLYGON (({min_x} {min_y}, {max_x} {min_y}, {max_x} {max_y}, {min_x} {max_y}, {min_x} {min_y}))"
        else:
            query_geom = "POLYGON ((25 25, 75 25, 75 75, 25 75, 25 25))"

    elif spatial_func == "ST_WITHIN":
        # Create a large query polygon that contains many small geometries
        query_geom = "POLYGON ((5 5, 95 5, 95 95, 5 95, 5 5))"

    elif spatial_func == "ST_EQUALS":
        # Find a point in base data and create query with same point
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                query_geom = item[geo_field_name]
                break
        else:
            query_geom = "POINT (25 25)"

    elif spatial_func == "ST_TOUCHES":
        # Create a polygon that touches some base geometries
        points = []
        for item in base_data:
            if "POINT" in item[geo_field_name]:
                x, y = parse_point(item[geo_field_name])
                if x is not None and y is not None:
                    points.append((x, y))

        if points:
            target_point = points[0]
            x, y = target_point[0], target_point[1]
            size = 20
            query_geom = f"POLYGON (({x} {y - size}, {x + size} {y - size}, {x + size} {y}, {x} {y}, {x} {y - size}))"
        else:
            query_geom = "POLYGON ((0 0, 20 0, 20 20, 0 20, 0 0))"

    elif spatial_func == "ST_OVERLAPS":
        # Find polygons in base data and create overlapping query polygon
        polygons = []
        for item in base_data:
            if "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    polygons.append((min_x, max_x, min_y, max_y))

        if polygons:
            target_poly = polygons[0]
            min_x, max_x, min_y, max_y = target_poly[0], target_poly[1], target_poly[2], target_poly[3]
            shift = (max_x - min_x) * 0.3
            query_geom = f"POLYGON (({min_x + shift} {min_y + shift}, {max_x + shift} {min_y + shift}, {max_x + shift} {max_y + shift}, {min_x + shift} {max_y + shift}, {min_x + shift} {min_y + shift}))"
        else:
            query_geom = "POLYGON ((10 10, 30 10, 30 30, 10 30, 10 10))"

    elif spatial_func == "ST_CROSSES":
        # Create a line that crosses polygons
        polygons = []
        for item in base_data:
            if "POLYGON" in item[geo_field_name]:
                min_x, max_x, min_y, max_y = parse_polygon_bounds(item[geo_field_name])
                if min_x is not None:
                    polygons.append((min_x, max_x, min_y, max_y))

        if polygons:
            target_poly = polygons[0]
            min_x, max_x, min_y, max_y = target_poly[0], target_poly[1], target_poly[2], target_poly[3]
            center_x = (min_x + max_x) / 2
            center_y = (min_y + max_y) / 2
            query_geom = f"LINESTRING ({center_x} {min_y - 10}, {center_x} {max_y + 10})"
        else:
            query_geom = "LINESTRING (15 -5, 15 25)"

    else:
        query_geom = "POLYGON ((0 0, 50 0, 50 50, 0 50, 0 0))"

    return query_geom


def generate_gt(spatial_func, base_data, query_geom, geo_field_name="geo", pk_field_name="id"):
    """
    Generate ground truth (expected IDs) using shapely

    Args:
        spatial_func: The spatial function name (e.g., "ST_INTERSECTS", "ST_CONTAINS")
        base_data: List of base geometry data with format [{pk_field_name: int, geo_field_name: "WKT_STRING"}, ...]
        query_geom: WKT string of the query geometry
        geo_field_name: Name of the geometry field in base_data (default: "geo")
        pk_field_name: Name of the primary key field in base_data (default: "id")

    Returns:
        expected_ids: List of primary key values that should match the spatial function
    """
    try:
        from shapely import wkt
        import shapely
    except ImportError:
        logger.warning("shapely not installed, returning empty expected_ids")
        return []

    # Spatial function mapping
    spatial_function_mapping = {
        "ST_EQUALS": shapely.equals,
        "ST_TOUCHES": shapely.touches,
        "ST_OVERLAPS": shapely.overlaps,
        "ST_CROSSES": shapely.crosses,
        "ST_CONTAINS": shapely.contains,
        "ST_INTERSECTS": shapely.intersects,
        "ST_WITHIN": shapely.within,
    }

    if spatial_func not in spatial_function_mapping:
        logger.warning(f"Unsupported spatial function {spatial_func}, returning empty expected_ids")
        return []

    try:
        # Parse query geometry
        query_geometry = wkt.loads(query_geom)
        shapely_func = spatial_function_mapping[spatial_func]

        # Parse all base geometries
        base_geometries = []
        base_ids = []
        for item in base_data:
            try:
                base_geometry = wkt.loads(item[geo_field_name])
                base_geometries.append(base_geometry)
                base_ids.append(item[pk_field_name])
            except Exception as e:
                logger.warning(f"Failed to parse geometry {item[geo_field_name]}: {e}")
                continue

        if not base_geometries:
            return []

        # Convert to numpy array for vectorized operation
        base_geoms_array = np.array(base_geometries)
        base_ids_array = np.array(base_ids)

        # Apply vectorized spatial function
        results = shapely_func(base_geoms_array, query_geometry)

        # Get matching IDs
        expected_ids = base_ids_array[results].tolist()

        return expected_ids

    except Exception as e:
        logger.error(f"Failed to compute ground truth for {spatial_func}: {e}")
        return []
