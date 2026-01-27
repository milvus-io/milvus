import os
import random
import math
import string
import json
import time
import uuid
from functools import singledispatch
import numpy as np
import pandas as pd
from ml_dtypes import bfloat16
from sklearn import preprocessing
from npy_append_array import NpyAppendArray
from faker import Faker
from pathlib import Path
from minio import Minio
from base.schema_wrapper import ApiCollectionSchemaWrapper, ApiFieldSchemaWrapper
from common import common_type as ct
from common.common_params import ExprCheckParams
from utils.util_log import test_log as log
from customize.milvus_operator import MilvusOperator
import pickle
from collections import Counter
import bm25s
import jieba
import re
import inspect
from typing import Optional, Tuple
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta, timezone as tzmod
from datetime import timezone
from dateutil import parser
import pytz

from pymilvus import CollectionSchema, FieldSchema, DataType, FunctionType, Function, MilvusException, MilvusClient

from bm25s.tokenization import Tokenizer

fake = Faker()


from common.common_params import Expr
"""" Methods of processing data """


try:
    RNG = np.random.default_rng(seed=0)
except ValueError as e:
    RNG = None


@singledispatch
def to_serializable(val):
    """Used by default."""
    return str(val)


@to_serializable.register(np.float32)
def ts_float32(val):
    """Used if *val* is an instance of numpy.float32."""
    return np.float64(val)


class ParamInfo:
    def __init__(self):
        self.param_host = ""
        self.param_port = ""
        self.param_handler = ""
        self.param_user = ""
        self.param_password = ""
        self.param_secure = False
        self.param_replica_num = ct.default_replica_num
        self.param_uri = ""
        self.param_token = ""
        self.param_bucket_name = ""

    def prepare_param_info(self, host, port, handler, replica_num, user, password, secure, uri, token, bucket_name):
        self.param_host = host
        self.param_port = port
        self.param_handler = handler
        self.param_user = user
        self.param_password = password
        self.param_secure = secure
        self.param_replica_num = replica_num
        self.param_uri = uri
        self.param_token = token
        self.param_bucket_name = bucket_name


param_info = ParamInfo()

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

    def new_text(nb_sentences=100, *args, **kwargs):
        sentences = []
        # Split original text into sentences
        original_sentences = original_text(nb_sentences).split('.')
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


def get_bm25_ground_truth(corpus, queries, top_k=100, language="en"):
    """
    Get the ground truth for BM25 search.
    :param corpus: The corpus of documents
    :param queries: The query string or list of query strings
    :return: The ground truth for BM25 search
    """

    def remove_punctuation(text):
        text = text.strip()
        text = text.replace("\n", " ")
        return re.sub(r'[^\w\s]', ' ', text)

    # Tokenize the corpus
    def jieba_split(text):
        text_without_punctuation = remove_punctuation(text)
        return jieba.lcut(text_without_punctuation)

    stopwords = "english" if language in ["en", "english"] else [" "]
    stemmer = None
    if language in ["zh", "cn", "chinese"]:
        splitter = jieba_split
        tokenizer = Tokenizer(
            stemmer=stemmer, splitter=splitter, stopwords=stopwords
        )
    else:
        tokenizer = Tokenizer(
            stemmer=stemmer, stopwords=stopwords
        )
    corpus_tokens = tokenizer.tokenize(corpus, return_as="tuple")
    retriever = bm25s.BM25()
    retriever.index(corpus_tokens)
    query_tokens = tokenizer.tokenize(queries,return_as="tuple")
    results, scores = retriever.retrieve(query_tokens, corpus=corpus, k=top_k)
    return results, scores


def custom_tokenizer(language="en"):
    def remove_punctuation(text):
        text = text.strip()
        text = text.replace("\n", " ")
        return re.sub(r'[^\w\s]', ' ', text)

    # Tokenize the corpus
    def jieba_split(text):
        text_without_punctuation = remove_punctuation(text)
        return jieba.cut_for_search(text_without_punctuation)

    def blank_space_split(text):
        text_without_punctuation = remove_punctuation(text)
        return text_without_punctuation.split()

    stopwords = [" "]
    stemmer = None
    if language in ["zh", "cn", "chinese"]:
        splitter = jieba_split
        tokenizer = Tokenizer(
            stemmer=stemmer, splitter=splitter, stopwords=stopwords
        )
    else:
        splitter = blank_space_split
        tokenizer = Tokenizer(
            stemmer=stemmer, splitter= splitter, stopwords=stopwords
        )
    return tokenizer


def manual_check_text_match(df, word, col):
    id_list = []
    for i in range(len(df)):
        row = df.iloc[i]
        # log.info(f"word :{word}, row: {row[col]}")
        if word in row[col]:
            id_list.append(row["id"])
    return id_list


def get_top_english_tokens(counter, n=10):
    english_pattern = re.compile(r'^[a-zA-Z]+$')

    english_tokens = {
        word: freq
        for word, freq in counter.items()
        if english_pattern.match(str(word))
    }
    english_counter = Counter(english_tokens)
    return english_counter.most_common(n)


def analyze_documents(texts, language="en"):

    tokenizer = custom_tokenizer(language)
    new_texts = []
    for text in texts:
        if isinstance(text, str):
            new_texts.append(text)
    # Tokenize the corpus
    tokenized = tokenizer.tokenize(new_texts, return_as="tuple", show_progress=False)
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

    # if language in ["zh", "cn", "chinese"], remove the long words
    # this is a trick to make the text match test case verification simple, because the long word can be still split
    if language in ["zh", "cn", "chinese"]:
        word_freq = Counter({word: count for word, count in word_freq.items() if 1< len(word) <= 3})
    log.debug(f"word freq {word_freq.most_common(10)}")
    return word_freq


def analyze_documents_with_analyzer_params(texts, analyzer_params):
    if param_info.param_uri:
        uri = param_info.param_uri
    else:
        uri = "http://" + param_info.param_host + ":" + str(param_info.param_port)

    client = MilvusClient(
        uri=uri,
        token=param_info.param_token
    )
    freq = Counter()
    res = client.run_analyzer(texts, analyzer_params, with_detail=True, with_hash=True)
    for r in res:
        freq.update(t['token'] for t in r.tokens)
    log.info(f"word freq {freq.most_common(10)}")
    return freq


def check_token_overlap(text_a, text_b, language="en"):
    word_freq_a = analyze_documents([text_a], language)
    word_freq_b = analyze_documents([text_b], language)
    overlap = set(word_freq_a.keys()).intersection(set(word_freq_b.keys()))
    return overlap, word_freq_a, word_freq_b


def split_dataframes(df, fields, language="en"):
    df_copy = df.copy()
    for col in fields:
        tokenizer = custom_tokenizer(language)
        texts = df[col].to_list()
        tokenized = tokenizer.tokenize(texts, return_as="tuple")
        new_texts = []
        id_vocab_map = {id: word for word, id in tokenized.vocab.items()}
        for doc_ids in tokenized.ids:
            new_texts.append([id_vocab_map[token_id] for token_id in doc_ids])
        df_copy[col] = new_texts
    return df_copy


def generate_pandas_text_match_result(expr, df):
    def manual_check(expr):
        if "not" in expr:
            key = expr["not"]["field"]
            value = expr["not"]["value"]
            return lambda row: value not in row[key]
        key = expr["field"]
        value = expr["value"]
        return lambda row: value in row[key]
    if "not" in expr:
        key = expr["not"]["field"]
    else:
        key = expr["field"]
    manual_result = df[df.apply(manual_check(expr), axis=1)]
    log.info(f"pandas filter result {len(manual_result)}\n{manual_result[key]}")
    return manual_result


def generate_text_match_expr(query_dict):
    """
    Generate a TextMatch expression with multiple logical operators and field names.
    :param query_dict: A dictionary representing the query structure
    :return: A string representing the TextMatch expression
    """

    def process_node(node):
        if isinstance(node, dict) and 'field' in node and 'value' in node:
            return f"TEXT_MATCH({node['field']}, '{node['value']}')"
        elif isinstance(node, dict) and 'not' in node:
            return f"not {process_node(node['not'])}"
        elif isinstance(node, list):
            return ' '.join(process_node(item) for item in node)
        elif isinstance(node, str):
            return node
        else:
            raise ValueError(f"Invalid node type: {type(node)}")

    return f"({process_node(query_dict)})"


def generate_pandas_query_string(query):
    def process_node(node):
        if isinstance(node, dict):
            if 'field' in node and 'value' in node:
                return f"('{node['value']}' in row['{node['field']}'])"
            elif 'not' in node:
                return f"not {process_node(node['not'])}"
        elif isinstance(node, str):
            return node
        else:
            raise ValueError(f"Invalid node type: {type(node)}")

    parts = [process_node(item) for item in query]
    expression = ' '.join(parts).replace('and', 'and').replace('or', 'or')
    log.info(f"Generated pandas query: {expression}")
    return lambda row: eval(expression)


def evaluate_expression(step_by_step_results):
    # merge result of different steps to final result
    def apply_operator(operators, operands):
        operator = operators.pop()
        right = operands.pop()
        left = operands.pop()
        if operator == "and":
            operands.append(left.intersection(right))
        elif operator == "or":
            operands.append(left.union(right))

    operators = []
    operands = []

    for item in step_by_step_results:
        if isinstance(item, list):
            operands.append(set(item))
        elif item in ("and", "or"):
            while operators and operators[-1] == "and" and item == "or":
                apply_operator(operators, operands)
            operators.append(item)
    while operators:
        apply_operator(operators, operands)

    return operands[0] if operands else set()


def generate_random_query_from_freq_dict(freq_dict, min_freq=1, max_terms=3, p_not=0.2):
    """
    Generate a random query expression from a dictionary of field frequencies.
    :param freq_dict: A dictionary where keys are field names and values are word frequency dictionaries
    :param min_freq: Minimum frequency for a word to be included in the query (default: 1)
    :param max_terms: Maximum number of terms in the query (default: 3)
    :param p_not: Probability of using NOT for any term (default: 0.2)
    :return: A tuple of (query list, query expression string)
    example:
    freq_dict = {
    "title": {"The": 3, "Lord": 2, "Rings": 2, "Harry": 1, "Potter": 1},
    "author": {"Tolkien": 2, "Rowling": 1, "Orwell": 1},
    "description": {"adventure": 4, "fantasy": 3, "magic": 1, "dystopian": 2}
    }
    print("Random queries from frequency dictionary:")
    for _ in range(5):
        query_list, expr = generate_random_query_from_freq_dict(freq_dict, min_freq=1, max_terms=4, p_not=0.2)
        print(f"Query: {query_list}")
        print(f"Expression: {expr}")
        print()
    """

    def random_term(field, words):
        term = {"field": field, "value": random.choice(words)}
        if random.random() < p_not:
            return {"not": term}
        return term

    # Filter words based on min_freq
    filtered_dict = {
        field: [word for word, freq in words.items() if freq >= min_freq]
        for field, words in freq_dict.items()
    }

    # Remove empty fields
    filtered_dict = {k: v for k, v in filtered_dict.items() if v}

    if not filtered_dict:
        return [], ""

    # Randomly select fields and terms
    query = []
    for _ in range(min(max_terms, sum(len(words) for words in filtered_dict.values()))):
        if not filtered_dict:
            break
        field = random.choice(list(filtered_dict.keys()))
        if filtered_dict[field]:
            term = random_term(field, filtered_dict[field])
            query.append(term)
            # Insert random AND/OR between terms
            if query and _ < max_terms - 1:
                query.append(random.choice(["and", "or"]))
            # Remove the used word to avoid repetition
            used_word = term['value'] if isinstance(term, dict) and 'value' in term else term['not']['value']
            filtered_dict[field].remove(used_word)
            if not filtered_dict[field]:
                del filtered_dict[field]
    return query, generate_text_match_expr(query), generate_pandas_query_string(query)


def generate_array_dataset(size, array_length, hit_probabilities, target_values):
    dataset = []
    target_array_length = target_values.get('array_length_field', None)
    target_array_access = target_values.get('array_access', None)
    all_target_values = set(
        val for sublist in target_values.values() for val in (sublist if isinstance(sublist, list) else [sublist]))
    for i in range(size):
        entry = {"id": i}

        # Generate random arrays for each condition
        for condition in hit_probabilities.keys():
            available_values = [val for val in range(1, 100) if val not in all_target_values]
            array = random.sample(available_values, array_length)

            # Ensure the array meets the condition based on its probability
            if random.random() < hit_probabilities[condition]:
                if condition == 'contains':
                    if target_values[condition] not in array:
                        array[random.randint(0, array_length - 1)] = target_values[condition]
                elif condition == 'contains_any':
                    if not any(val in array for val in target_values[condition]):
                        array[random.randint(0, array_length - 1)] = random.choice(target_values[condition])
                elif condition == 'contains_all':
                    indices = random.sample(range(array_length), len(target_values[condition]))
                    for idx, val in zip(indices, target_values[condition]):
                        array[idx] = val
                elif condition == 'equals':
                    array = target_values[condition][:]
                elif condition == 'array_length_field':
                    array = [random.randint(0, 10) for _ in range(target_array_length)]
                elif condition == 'array_access':
                    array = [random.randint(0, 10) for _ in range(random.randint(10, 20))]
                    array[target_array_access[0]] = target_array_access[1]
                else:
                    raise ValueError(f"Unknown condition: {condition}")

            entry[condition] = array

        dataset.append(entry)

    return dataset


def prepare_array_test_data(data_size, hit_rate=0.005, dim=128):
    size = data_size  # Number of arrays in the dataset
    array_length = 10  # Length of each array

    # Probabilities that an array hits the target condition
    hit_probabilities = {
        'contains': hit_rate,
        'contains_any': hit_rate,
        'contains_all': hit_rate,
        'equals': hit_rate,
        'array_length_field': hit_rate,
        'array_access': hit_rate
    }

    # Target values for each condition
    target_values = {
        'contains': 42,
        'contains_any': [21, 37, 42],
        'contains_all': [15, 30],
        'equals': [1,2,3,4,5],
        'array_length_field': 5, # array length == 5
        'array_access': [0, 5] # index=0, and value == 5
    }

    # Generate dataset
    dataset = generate_array_dataset(size, array_length, hit_probabilities, target_values)
    data = {
        "id": pd.Series([x["id"] for x in dataset]),
        "contains": pd.Series([x["contains"] for x in dataset]),
        "contains_any": pd.Series([x["contains_any"] for x in dataset]),
        "contains_all": pd.Series([x["contains_all"] for x in dataset]),
        "equals": pd.Series([x["equals"] for x in dataset]),
        "array_length_field": pd.Series([x["array_length_field"] for x in dataset]),
        "array_access": pd.Series([x["array_access"] for x in dataset]),
        "emb": pd.Series([np.array([random.random() for j in range(dim)], dtype=np.dtype("float32")) for _ in
                          range(size)])
    }
    # Define testing conditions
    contains_value = target_values['contains']
    contains_any_values = target_values['contains_any']
    contains_all_values = target_values['contains_all']
    equals_array = target_values['equals']

    # Perform tests
    contains_result = [d for d in dataset if contains_value in d["contains"]]
    contains_any_result = [d for d in dataset if any(val in d["contains_any"] for val in contains_any_values)]
    contains_all_result = [d for d in dataset if all(val in d["contains_all"] for val in contains_all_values)]
    equals_result = [d for d in dataset if d["equals"] == equals_array]
    array_length_result = [d for d in dataset if len(d["array_length_field"]) == target_values['array_length_field']]
    array_access_result = [d for d in dataset if d["array_access"][0] == target_values['array_access'][1]]
    # Calculate and log.info proportions
    contains_ratio = len(contains_result) / size
    contains_any_ratio = len(contains_any_result) / size
    contains_all_ratio = len(contains_all_result) / size
    equals_ratio = len(equals_result) / size
    array_length_ratio = len(array_length_result) / size
    array_access_ratio = len(array_access_result) / size

    log.info(f"\nProportion of arrays that contain the value: {contains_ratio}")
    log.info(f"Proportion of arrays that contain any of the values: {contains_any_ratio}")
    log.info(f"Proportion of arrays that contain all of the values: {contains_all_ratio}")
    log.info(f"Proportion of arrays that equal the target array: {equals_ratio}")
    log.info(f"Proportion of arrays that have the target array length: {array_length_ratio}")
    log.info(f"Proportion of arrays that have the target array access: {array_access_ratio}")

    train_df = pd.DataFrame(data)

    target_id = {
        "contains": [r["id"] for r in contains_result],
        "contains_any": [r["id"] for r in contains_any_result],
        "contains_all": [r["id"] for r in contains_all_result],
        "equals": [r["id"] for r in equals_result],
        "array_length": [r["id"] for r in array_length_result],
        "array_access": [r["id"] for r in array_access_result]
    }
    target_id_list = [target_id[key] for key in ["contains", "contains_any", "contains_all", "equals", "array_length", "array_access"]]

    filters = [
        "array_contains(contains, 42)",
        "array_contains_any(contains_any, [21, 37, 42])",
        "array_contains_all(contains_all, [15, 30])",
        "equals == [1,2,3,4,5]",
        "array_length(array_length_field) == 5",
        "array_access[0] == 5"

    ]
    query_expr = []
    for i in range(len(filters)):
        item = {
            "expr": filters[i],
            "ground_truth": target_id_list[i],
        }
        query_expr.append(item)
    return train_df, query_expr


def gen_unique_str(str_value=None):
    prefix = "".join(random.choice(string.ascii_letters + string.digits) for _ in range(8))
    return "test_" + prefix if str_value is None else str_value + "_" + prefix


def gen_str_by_length(length=8, letters_only=False, contain_numbers=False):
    if letters_only:
        return "".join(random.choice(string.ascii_letters) for _ in range(length))
    if contain_numbers:
        return "".join(random.choice(string.ascii_letters) for _ in range(length-1)) + \
            "".join(random.choice(string.digits))
    return "".join(random.choice(string.ascii_letters + string.digits) for _ in range(length))


def generate_random_sentence(language):
    language_map = {
        "English": "en_US",
        "French": "fr_FR",
        "Spanish": "es_ES",
        "German": "de_DE",
        "Italian": "it_IT",
        "Portuguese": "pt_PT",
        "Russian": "ru_RU",
        "Chinese": "zh_CN",
        "Japanese": "ja_JP",
        "Korean": "ko_KR",
        "Arabic": "ar_SA",
        "Hindi": "hi_IN"
    }
    lang_code = language_map.get(language, "en_US")
    faker = Faker(lang_code)
    return faker.sentence()


def gen_digits_by_length(length=8):
    return "".join(random.choice(string.digits) for _ in range(length))


def gen_scalar_field(field_type, name=None, description=ct.default_desc, is_primary=False,
                     nullable=False, skip_wrapper=False, **kwargs):
    """
    Generate a field schema based on the field type.
    
    Args:
        field_type: DataType enum value (e.g., DataType.BOOL, DataType.VARCHAR, etc.)
        name: Field name (uses default if None)
        description: Field description
        is_primary: Whether this is a primary field
        nullable: Whether this field is nullable
        skip_wrapper: whether to call FieldSchemaWrapper, in gen_row_data case,
                      it logs too much if calling the wrapper
        **kwargs: Additional parameters like max_length, max_capacity, etc.
    
    Returns:
        Field schema object
    """
    # Set default names based on field type
    if name is None:
        name = ct.default_field_name_map.get(field_type, "default_field")
    
    # Set default parameters for specific field types
    if field_type == DataType.VARCHAR and 'max_length' not in kwargs:
        kwargs['max_length'] = ct.default_length
    elif field_type == DataType.ARRAY:
        if 'element_type' not in kwargs:
            kwargs['element_type'] = DataType.INT64
        if 'max_capacity' not in kwargs:
            kwargs['max_capacity'] = ct.default_max_capacity
    if is_primary is True:
        nullable = False

    if skip_wrapper is True:
        field = FieldSchema(
            name=name,
            dtype=field_type,
            description=description,
            is_primary=is_primary,
            nullable=nullable,
            **kwargs
        )
        return field
    else:
        field, _ = ApiFieldSchemaWrapper().init_field_schema(
            name=name,
            dtype=field_type,
            description=description,
            is_primary=is_primary,
            nullable=nullable,
            **kwargs
        )
        return field


# Convenience functions for backward compatibility
def gen_bool_field(name=ct.default_bool_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.BOOL, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_string_field(name=ct.default_string_field_name, description=ct.default_desc, is_primary=False,
                     max_length=ct.default_length, **kwargs):
    return gen_scalar_field(DataType.VARCHAR, name=name, description=description, is_primary=is_primary, 
                    max_length=max_length, **kwargs)


def gen_json_field(name=ct.default_json_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.JSON, name=name, description=description, is_primary=is_primary, **kwargs)

def gen_geometry_field(name=ct.default_geometry_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.GEOMETRY, name=name, description=description, is_primary=is_primary, **kwargs)

def gen_geometry_field(name="geo", description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.GEOMETRY, name=name, description=description, is_primary=is_primary, **kwargs)

def gen_timestamptz_field(name=ct.default_timestamptz_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.TIMESTAMPTZ, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_array_field(name=ct.default_array_field_name, element_type=DataType.INT64, max_capacity=ct.default_max_capacity,
                    description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.ARRAY, name=name, description=description, is_primary=is_primary,
                    element_type=element_type, max_capacity=max_capacity, **kwargs)

def gen_int8_field(name=ct.default_int8_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.INT8, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_int16_field(name=ct.default_int16_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.INT16, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_int32_field(name=ct.default_int32_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.INT32, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_int64_field(name=ct.default_int64_field_name, description=ct.default_desc, is_primary=False, **kwargs):
    return gen_scalar_field(DataType.INT64, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_float_field(name=ct.default_float_field_name, is_primary=False, description=ct.default_desc, **kwargs):
    return gen_scalar_field(DataType.FLOAT, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_double_field(name=ct.default_double_field_name, is_primary=False, description=ct.default_desc, **kwargs):
    return gen_scalar_field(DataType.DOUBLE, name=name, description=description, is_primary=is_primary, **kwargs)


def gen_float_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                        description=ct.default_desc, vector_data_type=DataType.FLOAT_VECTOR, **kwargs):

    if vector_data_type != DataType.SPARSE_FLOAT_VECTOR:
        float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=vector_data_type,
                                                                       description=description, dim=dim,
                                                                       is_primary=is_primary, **kwargs)
    else:
         # no dim for sparse vector
        float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.SPARSE_FLOAT_VECTOR,
                                                                       description=description,
                                                                       is_primary=is_primary, **kwargs)

    return float_vec_field



def gen_binary_vec_field(name=ct.default_binary_vec_field_name, is_primary=False, dim=ct.default_dim,
                         description=ct.default_desc, **kwargs):
    binary_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BINARY_VECTOR,
                                                                    description=description, dim=dim,
                                                                    is_primary=is_primary, **kwargs)
    return binary_vec_field


def gen_float16_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                          description=ct.default_desc, **kwargs):
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.FLOAT16_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return float_vec_field


def gen_bfloat16_vec_field(name=ct.default_float_vec_field_name, is_primary=False, dim=ct.default_dim,
                           description=ct.default_desc, **kwargs):
    float_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.BFLOAT16_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return float_vec_field

def gen_int8_vec_field(name=ct.default_int8_vec_field_name, is_primary=False, dim=ct.default_dim,
                           description=ct.default_desc, **kwargs):
    int8_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.INT8_VECTOR,
                                                                   description=description, dim=dim,
                                                                   is_primary=is_primary, **kwargs)
    return int8_vec_field

def gen_sparse_vec_field(name=ct.default_sparse_vec_field_name, is_primary=False, description=ct.default_desc, **kwargs):
    sparse_vec_field, _ = ApiFieldSchemaWrapper().init_field_schema(name=name, dtype=DataType.SPARSE_FLOAT_VECTOR,
                                                                    description=description,
                                                                    is_primary=is_primary, **kwargs)
    return sparse_vec_field


def gen_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                  auto_id=False, dim=ct.default_dim, enable_dynamic_field=False, with_json=True,
                                  multiple_dim_array=[], is_partition_key=None, vector_data_type=DataType.FLOAT_VECTOR,
                                  nullable_fields={}, default_value_fields={}, **kwargs):
    # gen primary key field
    if default_value_fields.get(ct.default_int64_field_name) is None:
        int64_field = gen_int64_field(is_partition_key=(is_partition_key == ct.default_int64_field_name),
                                      nullable=(ct.default_int64_field_name in nullable_fields))
    else:
        int64_field = gen_int64_field(is_partition_key=(is_partition_key == ct.default_int64_field_name),
                                      nullable=(ct.default_int64_field_name in nullable_fields),
                                      default_value=default_value_fields.get(ct.default_int64_field_name))
    if default_value_fields.get(ct.default_string_field_name) is None:
        string_field = gen_string_field(is_partition_key=(is_partition_key == ct.default_string_field_name),
                                        nullable=(ct.default_string_field_name in nullable_fields))
    else:
        string_field = gen_string_field(is_partition_key=(is_partition_key == ct.default_string_field_name),
                                        nullable=(ct.default_string_field_name in nullable_fields),
                                        default_value=default_value_fields.get(ct.default_string_field_name))
    # gen vector field
    if default_value_fields.get(ct.default_float_vec_field_name) is None:
        float_vector_field = gen_float_vec_field(dim=dim, vector_data_type=vector_data_type,
                                                 nullable=(ct.default_float_vec_field_name in nullable_fields))
    else:
        float_vector_field = gen_float_vec_field(dim=dim, vector_data_type=vector_data_type,
                                                 nullable=(ct.default_float_vec_field_name in nullable_fields),
                                                 default_value=default_value_fields.get(
                                                     ct.default_float_vec_field_name))
    if primary_field is ct.default_int64_field_name:
        fields = [int64_field]
    elif primary_field is ct.default_string_field_name:
        fields = [string_field]
    else:
        log.error("Primary key only support int or varchar")
        assert False
    if enable_dynamic_field:
        fields.append(float_vector_field)
    else:
        if default_value_fields.get(ct.default_float_field_name) is None:
            float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields))
        else:
            float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields),
                                          default_value=default_value_fields.get(ct.default_float_field_name))
        if default_value_fields.get(ct.default_json_field_name) is None:
            json_field = gen_json_field(nullable=(ct.default_json_field_name in nullable_fields))
        else:
            json_field = gen_json_field(nullable=(ct.default_json_field_name in nullable_fields),
                                        default_value=default_value_fields.get(ct.default_json_field_name))
        fields = [int64_field, float_field, string_field, json_field, float_vector_field]
        if with_json is False:
            fields.remove(json_field)

    if len(multiple_dim_array) != 0:
        for other_dim in multiple_dim_array:
            name_prefix = "multiple_vector"
            fields.append(gen_float_vec_field(gen_unique_str(name_prefix), dim=other_dim,
                                              vector_data_type=vector_data_type))

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_all_datatype_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim, enable_dynamic_field=True, nullable=True,
                                       enable_struct_array_field=True, **kwargs):
    analyzer_params = {
        "tokenizer": "standard",
    }

    # Create schema using MilvusClient
    schema = MilvusClient.create_schema(
        auto_id=auto_id,
        enable_dynamic_field=enable_dynamic_field,
        description=description,
        **kwargs
    )

    # Add all fields using schema.add_field()
    schema.add_field(primary_field, DataType.INT64, is_primary=True)
    schema.add_field(ct.default_float_field_name, DataType.FLOAT, nullable=nullable)
    schema.add_field(ct.default_string_field_name, DataType.VARCHAR, max_length=ct.default_max_length, nullable=nullable)
    schema.add_field("document", DataType.VARCHAR, max_length=2000, enable_analyzer=True, enable_match=True, nullable=nullable)
    schema.add_field("text", DataType.VARCHAR, max_length=2000, enable_analyzer=True, enable_match=True,
                    analyzer_params=analyzer_params)
    schema.add_field(ct.default_json_field_name, DataType.JSON, nullable=nullable)
    schema.add_field(ct.default_geometry_field_name, DataType.GEOMETRY, nullable=nullable)
    schema.add_field(ct.default_timestamptz_field_name, DataType.TIMESTAMPTZ, nullable=nullable)
    schema.add_field("array_int", DataType.ARRAY, element_type=DataType.INT64, max_capacity=ct.default_max_capacity)
    schema.add_field("array_float", DataType.ARRAY, element_type=DataType.FLOAT, max_capacity=ct.default_max_capacity)
    schema.add_field("array_varchar", DataType.ARRAY, element_type=DataType.VARCHAR, max_length=200, max_capacity=ct.default_max_capacity)
    schema.add_field("array_bool", DataType.ARRAY, element_type=DataType.BOOL, max_capacity=ct.default_max_capacity)
    schema.add_field(ct.default_float_vec_field_name, DataType.FLOAT_VECTOR, dim=dim)
    schema.add_field("image_emb", DataType.INT8_VECTOR, dim=dim)
    schema.add_field("text_sparse_emb", DataType.SPARSE_FLOAT_VECTOR)
    # schema.add_field("voice_emb", DataType.FLOAT_VECTOR, dim=dim)

    # Add struct array field
    if enable_struct_array_field:
        struct_schema = MilvusClient.create_struct_field_schema()
        struct_schema.add_field("name", DataType.VARCHAR, max_length=200)
        struct_schema.add_field("age", DataType.INT64)
        struct_schema.add_field("float_vector", DataType.FLOAT_VECTOR, dim=dim)
        schema.add_field("array_struct", datatype=DataType.ARRAY, element_type=DataType.STRUCT,
                        struct_schema=struct_schema, max_capacity=10)

    # Add BM25 function
    bm25_function = Function(
        name=f"text",
        function_type=FunctionType.BM25,
        input_field_names=["text"],
        output_field_names=["text_sparse_emb"],
        params={},
    )
    schema.add_function(bm25_function)

    return schema


def gen_array_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name, auto_id=False,
                                dim=ct.default_dim, enable_dynamic_field=False, max_capacity=ct.default_max_capacity,
                                max_length=100, with_json=False, **kwargs):
    if enable_dynamic_field:
        if primary_field is ct.default_int64_field_name:
            fields = [gen_int64_field(), gen_float_vec_field(dim=dim)]
        elif primary_field is ct.default_string_field_name:
            fields = [gen_string_field(), gen_float_vec_field(dim=dim)]
        else:
            log.error("Primary key only support int or varchar")
            assert False
    else:
        fields = [gen_int64_field(), gen_float_vec_field(dim=dim), gen_json_field(nullable=True),
                  gen_array_field(name=ct.default_int32_array_field_name, element_type=DataType.INT32,
                                  max_capacity=max_capacity),
                  gen_array_field(name=ct.default_float_array_field_name, element_type=DataType.FLOAT,
                                  max_capacity=max_capacity),
                  gen_array_field(name=ct.default_string_array_field_name, element_type=DataType.VARCHAR,
                                  max_capacity=max_capacity, max_length=max_length, nullable=True)]
        if with_json is False:
            fields.remove(gen_json_field(nullable=True))

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_bulk_insert_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name, with_varchar_field=True,
                                      auto_id=False, dim=ct.default_dim, enable_dynamic_field=False, with_json=False):
    if enable_dynamic_field:
        if primary_field is ct.default_int64_field_name:
            fields = [gen_int64_field(), gen_float_vec_field(dim=dim)]
        elif primary_field is ct.default_string_field_name:
            fields = [gen_string_field(), gen_float_vec_field(dim=dim)]
        else:
            log.error("Primary key only support int or varchar")
            assert False
    else:
        fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(),
                  gen_float_vec_field(dim=dim)]
        if with_json is False:
            fields.remove(gen_json_field())
        if with_varchar_field is False:
            fields.remove(gen_string_field())
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field)
    return schema


def gen_general_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                  auto_id=False, is_binary=False, dim=ct.default_dim, **kwargs):
    if is_binary:
        fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_binary_vec_field(dim=dim)]
    else:
        fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_string_pk_default_collection_schema(description=ct.default_desc, primary_field=ct.default_string_field_name,
                                            auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_json_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_multiple_json_default_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                                auto_id=False, dim=ct.default_dim, **kwargs):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_json_field(name="json1"),
              gen_json_field(name="json2"), gen_float_vec_field(dim=dim)]
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id, **kwargs)
    return schema


def gen_collection_schema_all_datatype(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                       auto_id=False, dim=ct.default_dim, enable_dynamic_field=False, with_json=True,
                                       multiple_dim_array=[], nullable_fields={}, default_value_fields={},
                                       **kwargs):
    # gen primary key field
    if default_value_fields.get(ct.default_int64_field_name) is None:
        int64_field = gen_int64_field()
    else:
        int64_field = gen_int64_field(default_value=default_value_fields.get(ct.default_int64_field_name))

    if enable_dynamic_field:
        fields = [gen_int64_field()]
    else:
        if default_value_fields.get(ct.default_int32_field_name) is None:
            int32_field = gen_int32_field(nullable=(ct.default_int32_field_name in nullable_fields))
        else:
            int32_field = gen_int32_field(nullable=(ct.default_int32_field_name in nullable_fields),
                                          default_value=default_value_fields.get(ct.default_int32_field_name))
        if default_value_fields.get(ct.default_int16_field_name) is None:
            int16_field = gen_int16_field(nullable=(ct.default_int16_field_name in nullable_fields))
        else:
            int16_field = gen_int16_field(nullable=(ct.default_int16_field_name in nullable_fields),
                                          default_value=default_value_fields.get(ct.default_int16_field_name))
        if default_value_fields.get(ct.default_int8_field_name) is None:
            int8_field = gen_int8_field(nullable=(ct.default_int8_field_name in nullable_fields))
        else:
            int8_field = gen_int8_field(nullable=(ct.default_int8_field_name in nullable_fields),
                                          default_value=default_value_fields.get(ct.default_int8_field_name))
        if default_value_fields.get(ct.default_bool_field_name) is None:
            bool_field = gen_bool_field(nullable=(ct.default_bool_field_name in nullable_fields))
        else:
            bool_field = gen_bool_field(nullable=(ct.default_bool_field_name in nullable_fields),
                                          default_value=default_value_fields.get(ct.default_bool_field_name))
        if default_value_fields.get(ct.default_float_field_name) is None:
            float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields))
        else:
            float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields),
                                          default_value=default_value_fields.get(ct.default_float_field_name))
        if default_value_fields.get(ct.default_double_field_name) is None:
            double_field = gen_double_field(nullable=(ct.default_double_field_name in nullable_fields))
        else:
            double_field = gen_double_field(nullable=(ct.default_double_field_name in nullable_fields),
                                           default_value=default_value_fields.get(ct.default_double_field_name))
        if default_value_fields.get(ct.default_string_field_name) is None:
            string_field = gen_string_field(nullable=(ct.default_string_field_name in nullable_fields))
        else:
            string_field = gen_string_field(nullable=(ct.default_string_field_name in nullable_fields),
                                            default_value=default_value_fields.get(ct.default_string_field_name))
        if default_value_fields.get(ct.default_json_field_name) is None:
            json_field = gen_json_field(nullable=(ct.default_json_field_name in nullable_fields))
        else:
            json_field = gen_json_field(nullable=(ct.default_json_field_name in nullable_fields),
                                        default_value=default_value_fields.get(ct.default_json_field_name))
        fields = [int64_field, int32_field, int16_field, int8_field, bool_field,
                  float_field, double_field, string_field, json_field]
        if with_json is False:
            fields.remove(json_field)

    if len(multiple_dim_array) == 0:
        # gen vector field
        if default_value_fields.get(ct.default_float_vec_field_name) is None:
            float_vector_field = gen_float_vec_field(dim=dim)
        else:
            float_vector_field = gen_float_vec_field(dim=dim,
                                                     default_value=default_value_fields.get(ct.default_float_vec_field_name))
        fields.append(float_vector_field)
    else:
        multiple_dim_array.insert(0, dim)
        for i in range(len(multiple_dim_array)):
            if ct.append_vector_type[i%3] != DataType.SPARSE_FLOAT_VECTOR:
                if default_value_fields.get(ct.append_vector_type[i%3]) is None:
                    vector_field = gen_float_vec_field(name=f"multiple_vector_{ct.append_vector_type[i%3].name}",
                                                       dim=multiple_dim_array[i],
                                                       vector_data_type=ct.append_vector_type[i%3])
                else:
                    vector_field = gen_float_vec_field(name=f"multiple_vector_{ct.append_vector_type[i%3].name}",
                                                       dim=multiple_dim_array[i],
                                                       vector_data_type=ct.append_vector_type[i%3],
                                                       default_value=default_value_fields.get(ct.append_vector_type[i%3].name))
                fields.append(vector_field)
            else:
                # The field of a sparse vector cannot be dimensioned
                if default_value_fields.get(ct.default_sparse_vec_field_name) is None:
                    sparse_vector_field = gen_sparse_vec_field(name=f"multiple_vector_{DataType.SPARSE_FLOAT_VECTOR.name}",
                                                              vector_data_type=DataType.SPARSE_FLOAT_VECTOR)
                else:
                    sparse_vector_field = gen_sparse_vec_field(name=f"multiple_vector_{DataType.SPARSE_FLOAT_VECTOR.name}",
                                                              vector_data_type=DataType.SPARSE_FLOAT_VECTOR,
                                                              default_value=default_value_fields.get(ct.default_sparse_vec_field_name))
                fields.append(sparse_vector_field)

    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                    primary_field=primary_field, auto_id=auto_id,
                                                                    enable_dynamic_field=enable_dynamic_field, **kwargs)
    return schema


def gen_collection_schema(fields, primary_field=None, description=ct.default_desc, auto_id=False, **kwargs):
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, primary_field=primary_field,
                                                                    description=description, auto_id=auto_id, **kwargs)
    return schema


def gen_default_binary_collection_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                                         auto_id=False, dim=ct.default_dim, nullable_fields={}, default_value_fields={},
                                         **kwargs):
    if default_value_fields.get(ct.default_int64_field_name) is None:
        int64_field = gen_int64_field(nullable=(ct.default_int64_field_name in nullable_fields))
    else:
        int64_field = gen_int64_field(nullable=(ct.default_int64_field_name in nullable_fields),
                                      default_value=default_value_fields.get(ct.default_int64_field_name))
    if default_value_fields.get(ct.default_float_field_name) is None:
        float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields))
    else:
        float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields),
                                      default_value=default_value_fields.get(ct.default_float_field_name))
    if default_value_fields.get(ct.default_string_field_name) is None:
        string_field = gen_string_field(nullable=(ct.default_string_field_name in nullable_fields))
    else:
        string_field = gen_string_field(nullable=(ct.default_string_field_name in nullable_fields),
                                        default_value=default_value_fields.get(ct.default_string_field_name))
    if default_value_fields.get(ct.default_binary_vec_field_name) is None:
        binary_vec_field = gen_binary_vec_field(dim=dim, nullable=(ct.default_binary_vec_field_name in nullable_fields))
    else:
        binary_vec_field = gen_binary_vec_field(dim=dim, nullable=(ct.default_binary_vec_field_name in nullable_fields),
                                                default_value=default_value_fields.get(ct.default_binary_vec_field_name))
    fields = [int64_field, float_field, string_field, binary_vec_field]
    binary_schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                           primary_field=primary_field,
                                                                           auto_id=auto_id, **kwargs)
    return binary_schema


def gen_default_sparse_schema(description=ct.default_desc, primary_field=ct.default_int64_field_name,
                              auto_id=False, with_json=False, multiple_dim_array=[], nullable_fields={},
                              default_value_fields={}, **kwargs):
    if default_value_fields.get(ct.default_int64_field_name) is None:
        int64_field = gen_int64_field(nullable=(ct.default_int64_field_name in nullable_fields))
    else:
        int64_field = gen_int64_field(nullable=(ct.default_int64_field_name in nullable_fields),
                                      default_value=default_value_fields.get(ct.default_int64_field_name))
    if default_value_fields.get(ct.default_float_field_name) is None:
        float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields))
    else:
        float_field = gen_float_field(nullable=(ct.default_float_field_name in nullable_fields),
                                      default_value=default_value_fields.get(ct.default_float_field_name))
    if default_value_fields.get(ct.default_string_field_name) is None:
        string_field = gen_string_field(nullable=(ct.default_string_field_name in nullable_fields))
    else:
        string_field = gen_string_field(nullable=(ct.default_string_field_name in nullable_fields),
                                        default_value=default_value_fields.get(ct.default_string_field_name))
    if default_value_fields.get(ct.default_sparse_vec_field_name) is None:
        sparse_vec_field = gen_sparse_vec_field(nullable=(ct.default_sparse_vec_field_name in nullable_fields))
    else:
        sparse_vec_field = gen_sparse_vec_field(nullable=(ct.default_sparse_vec_field_name in nullable_fields),
                                                default_value=default_value_fields.get(ct.default_sparse_vec_field_name))
    fields = [int64_field, float_field, string_field, sparse_vec_field]

    if with_json:
        if default_value_fields.get(ct.default_json_field_name) is None:
            json_field = gen_json_field(nullable=(ct.default_json_field_name in nullable_fields))
        else:
            json_field = gen_json_field(nullable=(ct.default_json_field_name in nullable_fields),
                                        default_value=default_value_fields.get(ct.default_json_field_name))
        fields.insert(-1, json_field)

    if len(multiple_dim_array) != 0:
        for i in range(len(multiple_dim_array)):
            vec_name = ct.default_sparse_vec_field_name + "_" + str(i)
            vec_field = gen_sparse_vec_field(name=vec_name)
            fields.append(vec_field)
    sparse_schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=description,
                                                                           primary_field=primary_field,
                                                                           auto_id=auto_id, **kwargs)
    return sparse_schema


def gen_schema_multi_vector_fields(vec_fields):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_float_vec_field()]
    fields.extend(vec_fields)
    primary_field = ct.default_int64_field_name
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=ct.default_desc,
                                                                    primary_field=primary_field, auto_id=False)
    return schema


def gen_schema_multi_string_fields(string_fields):
    fields = [gen_int64_field(), gen_float_field(), gen_string_field(), gen_float_vec_field()]
    fields.extend(string_fields)
    primary_field = ct.default_int64_field_name
    schema, _ = ApiCollectionSchemaWrapper().init_collection_schema(fields=fields, description=ct.default_desc,
                                                                    primary_field=primary_field, auto_id=False)
    return schema

def gen_string(nb):
    string_values = [str(random.random()) for _ in range(nb)]
    return string_values


def gen_binary_vectors(num, dim):
    raw_vectors = []
    binary_vectors = []
    for _ in range(num):
        raw_vector = [random.randint(0, 1) for _ in range(dim)]
        raw_vectors.append(raw_vector)
        # packs a binary-valued array into bits in a unit8 array, and bytes array_of_ints
        binary_vectors.append(bytes(np.packbits(raw_vector, axis=-1).tolist()))
    return raw_vectors, binary_vectors


def gen_default_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                               random_primary_key=False, multiple_dim_array=[], multiple_vector_field_name=[],
                               vector_data_type=DataType.FLOAT_VECTOR, auto_id=False,
                               primary_field=ct.default_int64_field_name, nullable_fields={}, language=None):
    if not random_primary_key:
        int_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int_values = pd.Series(data=random.sample(range(start, start + nb), nb))

    float_data = [np.float32(i) for i in range(start, start + nb)]
    float_values = pd.Series(data=float_data, dtype="float32")
    if ct.default_float_field_name in nullable_fields:
        null_number = int(nb*nullable_fields[ct.default_float_field_name])
        null_data = [None for _ in range(null_number)]
        float_data = float_data[:nb-null_number] + null_data
        log.debug(float_data)
        float_values = pd.Series(data=float_data, dtype=object)

    string_data = [str(i) for i in range(start, start + nb)]
    if language:
        string_data = [generate_random_sentence(language) for _ in range(nb)]
    string_values = pd.Series(data=string_data, dtype="string")
    if ct.default_string_field_name in nullable_fields:
        null_number = int(nb*nullable_fields[ct.default_string_field_name])
        null_data = [None for _ in range(null_number)]
        string_data = string_data[:nb-null_number] + null_data
        string_values = pd.Series(data=string_data, dtype=object)

    json_values = [{"number": i, "float": i*1.0} for i in range(start, start + nb)]
    if ct.default_json_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_json_field_name])
        null_data = [{"number": None, "float": None} for _ in range(null_number)]
        json_values = json_values[:nb-null_number] + null_data

    float_vec_values = gen_vectors(nb, dim, vector_data_type=vector_data_type)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values,
        ct.default_float_vec_field_name: float_vec_values
    })

    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)
    if auto_id is True:
        if primary_field == ct.default_int64_field_name:
            df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        elif primary_field == ct.default_string_field_name:
            df.drop(ct.default_string_field_name, axis=1, inplace=True)
    if len(multiple_dim_array) != 0:
        if len(multiple_vector_field_name) != len(multiple_dim_array):
            log.error("multiple vector feature is enabled, please input the vector field name list "
                      "not including the default vector field")
            assert len(multiple_vector_field_name) == len(multiple_dim_array)
        for i in range(len(multiple_dim_array)):
            new_float_vec_values = gen_vectors(nb, multiple_dim_array[i], vector_data_type=vector_data_type)
            df[multiple_vector_field_name[i]] = new_float_vec_values

    return df


def gen_default_list_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                          random_primary_key=False, multiple_dim_array=[], multiple_vector_field_name=[],
                          vector_data_type=DataType.FLOAT_VECTOR, auto_id=False,
                          primary_field=ct.default_int64_field_name, nullable_fields={}, language=None):
    insert_list = []
    if not random_primary_key:
        int_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    float_data = [np.float32(i) for i in range(start, start + nb)]
    float_values = pd.Series(data=float_data, dtype="float32")
    if ct.default_float_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_float_field_name])
        null_data = [None for _ in range(null_number)]
        float_data = float_data[:nb - null_number] + null_data
        float_values = pd.Series(data=float_data, dtype=object)
    string_data = [str(i) for i in range(start, start + nb)]
    if language:
        string_data = [generate_random_sentence(language) for _ in range(nb)]
    string_values = pd.Series(data=string_data, dtype="string")
    if ct.default_string_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_string_field_name])
        null_data = [None for _ in range(null_number)]
        string_data = string_data[:nb - null_number] + null_data
        string_values = pd.Series(data=string_data, dtype=object)
    json_values = [{"number": i, "float": i*1.0} for i in range(start, start + nb)]
    if ct.default_json_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_json_field_name])
        null_data = [{"number": None, "float": None} for _ in range(null_number)]
        json_values = json_values[:nb-null_number] + null_data
    float_vec_values = gen_vectors(nb, dim, vector_data_type=vector_data_type)
    insert_list = [int_values, float_values, string_values]

    if with_json is True:
        insert_list.append(json_values)
    insert_list.append(float_vec_values)

    if auto_id is True:
        if primary_field == ct.default_int64_field_name:
            index = 0
        elif primary_field == ct.default_string_field_name:
            index = 2
        del insert_list[index]
    if len(multiple_dim_array) != 0:
        # if len(multiple_vector_field_name) != len(multiple_dim_array):
        #     log.error("multiple vector feature is enabled, please input the vector field name list "
        #               "not including the default vector field")
        #     assert len(multiple_vector_field_name) == len(multiple_dim_array)
        for i in range(len(multiple_dim_array)):
            new_float_vec_values = gen_vectors(nb, multiple_dim_array[i], vector_data_type=vector_data_type)
            insert_list.append(new_float_vec_values)

    return insert_list


def gen_default_rows_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True, multiple_dim_array=[],
                          multiple_vector_field_name=[], vector_data_type=DataType.FLOAT_VECTOR, auto_id=False,
                          primary_field = ct.default_int64_field_name, nullable_fields={}, language=None):
    array = []
    for i in range(start, start + nb):
        dict = {ct.default_int64_field_name: i,
                ct.default_float_field_name: i*1.0,
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: {"number": i, "float": i*1.0},
                ct.default_float_vec_field_name: gen_vectors(1, dim, vector_data_type=vector_data_type)[0]
                }
        if with_json is False:
            dict.pop(ct.default_json_field_name, None)
        if language:
            dict[ct.default_string_field_name] = generate_random_sentence(language)
        if auto_id is True:
            if primary_field == ct.default_int64_field_name:
                dict.pop(ct.default_int64_field_name)
            elif primary_field == ct.default_string_field_name:
                dict.pop(ct.default_string_field_name)
        array.append(dict)
        if len(multiple_dim_array) != 0:
            for i in range(len(multiple_dim_array)):
                dict[multiple_vector_field_name[i]] = gen_vectors(1, multiple_dim_array[i],
                                                                  vector_data_type=vector_data_type)[0]
    if ct.default_int64_field_name in nullable_fields:
        null_number = int(nb*nullable_fields[ct.default_int64_field_name])
        for single_dict in array[-null_number:]:
            single_dict[ct.default_int64_field_name] = None
    if ct.default_float_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_float_field_name])
        for single_dict in array[-null_number:]:
            single_dict[ct.default_float_field_name] = None
    if ct.default_string_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_string_field_name])
        for single_dict in array[-null_number:]:
            single_dict[ct.default_string_field_name] = None
    if ct.default_json_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_json_field_name])
        for single_dict in array[-null_number:]:
            single_dict[ct.default_string_field_name] = {"number": None, "float": None}

    log.debug("generated default row data")

    return array


def gen_json_data_for_diff_json_types(nb=ct.default_nb, start=0, json_type="json_embedded_object"):
    """
    Method: gen json data for different json types. Refer to RFC7159
    Note: String values should be passed as json.dumps(str) to ensure they are treated as strings,
          not as serialized JSON results.
    """
    if json_type == "json_embedded_object":                 # a json object with an embedd json object
        return [{json_type: {"number": i, "level2": {"level2_number": i, "level2_float": i*1.0, "level2_str": str(i), "level2_array": [i for i in range(i, i + 10)]},
                             "float": i*1.0}, "str": str(i), "array": [i for i in range(i, i + 10)], "bool": bool(i)}
                for i in range(start, start + nb)]
    if json_type == "json_objects_array":                   # a json-objects array with 2 json objects
        return [[{"number": i, "level2": {"level2_number": i, "level2_float": i*1.0, "level2_str": str(i)}, "float": i*1.0, "str": str(i)},
                 {"number": i, "level2": {"level2_number": i, "level2_float": i*1.0, "level2_str": str(i)}, "float": i*1.0, "str": str(i)}
                 ] for i in range(start, start + nb)]
    if json_type == "json_array":                           # single array as json value
        return [[i for i in range(j, j + 10)] for j in range(start, start + nb)]
    if json_type == "json_int":                             # single int as json value
        return [i for i in range(start, start + nb)]
    if json_type == "json_float":                           # single float as json value
        return [i*1.0 for i in range(start, start + nb)]
    if json_type == "json_string":                          # single string as json value
        return [json.dumps(str(i)) for i in range(start, start + nb)]
    if json_type == "json_bool":                            # single bool as json value
        return [bool(i) for i in range(start, start + nb)]
    else:
        return []


def gen_default_data_for_upsert(nb=ct.default_nb, dim=ct.default_dim, start=0, size=10000):
    int_values = pd.Series(data=[i for i in range(start, start + nb)])
    float_values = pd.Series(data=[np.float32(i + size) for i in range(start, start + nb)], dtype="float32")
    string_values = pd.Series(data=[str(i + size) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "string": str(i)} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values,
        ct.default_float_vec_field_name: float_vec_values
    })
    return df, float_values


def gen_array_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, auto_id=False,
                             array_length=ct.default_max_capacity, with_json=False, random_primary_key=False):
    if not random_primary_key:
        int_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    float_vec_values = gen_vectors(nb, dim)
    json_values = [{"number": i, "float": i * 1.0} for i in range(start, start + nb)]

    int32_values = pd.Series(data=[[np.int32(j) for j in range(i, i + array_length)] for i in range(start, start + nb)])
    float_values = pd.Series(data=[[np.float32(j) for j in range(i, i + array_length)] for i in range(start, start + nb)])
    string_values = pd.Series(data=[[str(j) for j in range(i, i + array_length)] for i in range(start, start + nb)])

    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_vec_field_name: float_vec_values,
        ct.default_json_field_name: json_values,
        ct.default_int32_array_field_name: int32_values,
        ct.default_float_array_field_name: float_values,
        ct.default_string_array_field_name: string_values,
    })
    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)
    if auto_id:
        df.drop(ct.default_int64_field_name, axis=1, inplace=True)

    return df


def gen_dataframe_multi_vec_fields(vec_fields, nb=ct.default_nb):
    """
    gen dataframe data for fields: int64, float, float_vec and vec_fields
    :param nb: num of entities, default default_nb
    :param vec_fields: list of FieldSchema
    :return: dataframe
    """
    int_values = pd.Series(data=[i for i in range(0, nb)])
    float_values = pd.Series(data=[float(i) for i in range(nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(nb)], dtype="string")
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_float_vec_field_name: gen_vectors(nb, ct.default_dim)
    })
    for field in vec_fields:
        dim = field.params['dim']
        if field.dtype == DataType.FLOAT_VECTOR:
            vec_values = gen_vectors(nb, dim)
        elif field.dtype == DataType.BINARY_VECTOR:
            vec_values = gen_binary_vectors(nb, dim)[1]
        df[field.name] = vec_values
    return df


def gen_dataframe_multi_string_fields(string_fields, nb=ct.default_nb):
    """
    gen dataframe data for fields: int64, float, float_vec and vec_fields
    :param nb: num of entities, default default_nb
    :param vec_fields: list of FieldSchema
    :return: dataframe
    """
    int_values = pd.Series(data=[i for i in range(0, nb)])
    float_values = pd.Series(data=[float(i) for i in range(nb)], dtype="float32")
    string_values = pd.Series(data=[str(i) for i in range(nb)], dtype="string")
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_float_vec_field_name: gen_vectors(nb, ct.default_dim)
    })
    for field in string_fields:
        if field.dtype == DataType.VARCHAR:
            string_values = gen_string(nb)
        df[field.name] = string_values
    return df


def gen_dataframe_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                auto_id=False, random_primary_key=False, multiple_dim_array=[],
                                multiple_vector_field_name=[], primary_field=ct.default_int64_field_name):
    if not random_primary_key:
        int64_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int64_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    int32_values = pd.Series(data=[np.int32(i) for i in range(start, start + nb)], dtype="int32")
    int16_values = pd.Series(data=[np.int16(i) for i in range(start, start + nb)], dtype="int16")
    int8_values = pd.Series(data=[np.int8(i) for i in range(start, start + nb)], dtype="int8")
    bool_values = pd.Series(data=[np.bool_(i) for i in range(start, start + nb)], dtype="bool")
    float_values = pd.Series(data=[np.float32(i) for i in range(start, start + nb)], dtype="float32")
    double_values = pd.Series(data=[np.double(i) for i in range(start, start + nb)], dtype="double")
    string_values = pd.Series(data=[str(i) for i in range(start, start + nb)], dtype="string")
    json_values = [{"number": i, "string": str(i), "bool": bool(i),
                    "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(start, start + nb)]
    float_vec_values = gen_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int64_values,
        ct.default_int32_field_name: int32_values,
        ct.default_int16_field_name: int16_values,
        ct.default_int8_field_name: int8_values,
        ct.default_bool_field_name: bool_values,
        ct.default_float_field_name: float_values,
        ct.default_double_field_name: double_values,
        ct.default_string_field_name: string_values,
        ct.default_json_field_name: json_values
    })

    if len(multiple_dim_array) == 0:
        df[ct.default_float_vec_field_name] = float_vec_values
    else:
        for i in range(len(multiple_dim_array)):
            df[multiple_vector_field_name[i]] = gen_vectors(nb, multiple_dim_array[i], ct.append_vector_type[i%3])

    if with_json is False:
        df.drop(ct.default_json_field_name, axis=1, inplace=True)
    if auto_id:
        if primary_field == ct.default_int64_field_name:
            df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        elif primary_field == ct.default_string_field_name:
            df.drop(ct.default_string_field_name, axis=1, inplace=True)
    log.debug("generated data completed")

    return df


def gen_general_list_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                   auto_id=False, random_primary_key=False, multiple_dim_array=[],
                                   multiple_vector_field_name=[], primary_field=ct.default_int64_field_name,
                                   nullable_fields={}, language=None):
    if not random_primary_key:
        int64_values = pd.Series(data=[i for i in range(start, start + nb)])
    else:
        int64_values = pd.Series(data=random.sample(range(start, start + nb), nb))
    int32_data = [np.int32(i) for i in range(start, start + nb)]
    int32_values = pd.Series(data=int32_data, dtype="int32")
    if ct.default_int32_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_int32_field_name])
        null_data = [None for _ in range(null_number)]
        int32_data = int32_data[:nb - null_number] + null_data
        int32_values = pd.Series(data=int32_data, dtype=object)

    int16_data = [np.int16(i) for i in range(start, start + nb)]
    int16_values = pd.Series(data=int16_data, dtype="int16")
    if ct.default_int16_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_int16_field_name])
        null_data = [None for _ in range(null_number)]
        int16_data = int16_data[:nb - null_number] + null_data
        int16_values = pd.Series(data=int16_data, dtype=object)

    int8_data = [np.int8(i) for i in range(start, start + nb)]
    int8_values = pd.Series(data=int8_data, dtype="int8")
    if ct.default_int8_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_int8_field_name])
        null_data = [None for _ in range(null_number)]
        int8_data = int8_data[:nb - null_number] + null_data
        int8_values = pd.Series(data=int8_data, dtype=object)

    bool_data = [np.bool_(i) for i in range(start, start + nb)]
    bool_values = pd.Series(data=bool_data, dtype="bool")
    if ct.default_bool_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_bool_field_name])
        null_data = [None for _ in range(null_number)]
        bool_data = bool_data[:nb - null_number] + null_data
        bool_values = pd.Series(data=bool_data, dtype="bool")

    float_data = [np.float32(i) for i in range(start, start + nb)]
    float_values = pd.Series(data=float_data, dtype="float32")
    if ct.default_float_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_float_field_name])
        null_data = [None for _ in range(null_number)]
        float_data = float_data[:nb - null_number] + null_data
        float_values = pd.Series(data=float_data, dtype=object)

    double_data = [np.double(i) for i in range(start, start + nb)]
    double_values = pd.Series(data=double_data, dtype="double")
    if ct.default_double_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_double_field_name])
        null_data = [None for _ in range(null_number)]
        double_data = double_data[:nb - null_number] + null_data
        double_values = pd.Series(data=double_data, dtype=object)

    string_data = [str(i) for i in range(start, start + nb)]
    if language:
        string_data = [generate_random_sentence(language) for _ in range(nb)]
    string_values = pd.Series(data=string_data, dtype="string")
    if ct.default_string_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_string_field_name])
        null_data = [None for _ in range(null_number)]
        string_data = string_data[:nb - null_number] + null_data
        string_values = pd.Series(data=string_data, dtype=object)

    json_values = [{"number": i, "string": str(i), "bool": bool(i),
                    "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(start, start + nb)]
    if ct.default_json_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_json_field_name])
        null_data = [{"number": None, "string": None, "bool": None,
                    "list": [None for _ in range(i, i + ct.default_json_list_length)]} for i in range(null_number)]
        json_values = json_values[:nb - null_number] + null_data
    float_vec_values = gen_vectors(nb, dim)
    insert_list = [int64_values, int32_values, int16_values, int8_values, bool_values, float_values, double_values,
                   string_values, json_values]

    if len(multiple_dim_array) == 0:
        insert_list.append(float_vec_values)
    else:
        for i in range(len(multiple_dim_array)):
            insert_list.append(gen_vectors(nb, multiple_dim_array[i], ct.append_vector_type[i%3]))

    if with_json is False:
        # index = insert_list.index(json_values)
        del insert_list[8]
    if auto_id:
        if primary_field == ct.default_int64_field_name:
            index = insert_list.index(int64_values)
        elif primary_field == ct.default_string_field_name:
            index = insert_list.index(string_values)
        del insert_list[index]
    log.debug("generated data completed")

    return insert_list


def gen_default_rows_data_all_data_type(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=True,
                                        multiple_dim_array=[], multiple_vector_field_name=[], partition_id=0,
                                        auto_id=False, primary_field=ct.default_int64_field_name, language=None):
    array = []
    for i in range(start, start + nb):
        dict = {ct.default_int64_field_name: i,
                ct.default_int32_field_name: i,
                ct.default_int16_field_name: i,
                ct.default_int8_field_name: i,
                ct.default_bool_field_name: bool(i),
                ct.default_float_field_name: i*1.0,
                ct.default_double_field_name: i * 1.0,
                ct.default_string_field_name: str(i),
                ct.default_json_field_name: {"number": i, "string": str(i), "bool": bool(i),
                                             "list": [j for j in range(i, i + ct.default_json_list_length)]}
                }
        if with_json is False:
            dict.pop(ct.default_json_field_name, None)
        if language:
            dict[ct.default_string_field_name] = generate_random_sentence(language)
        if auto_id is True:
            if primary_field == ct.default_int64_field_name:
                dict.pop(ct.default_int64_field_name, None)
            elif primary_field == ct.default_string_field_name:
                dict.pop(ct.default_string_field_name, None)
        array.append(dict)
        if len(multiple_dim_array) == 0:
            dict[ct.default_float_vec_field_name] = gen_vectors(1, dim)[0]
        else:
            for i in range(len(multiple_dim_array)):
                dict[multiple_vector_field_name[i]] = gen_vectors(nb, multiple_dim_array[i],
                                                                  ct.append_vector_type[i])[0]
    if len(multiple_dim_array) != 0:
        with open(ct.rows_all_data_type_file_path + f'_{partition_id}' + f'_dim{dim}.txt', 'wb') as json_file:
            pickle.dump(array, json_file)
            log.info("generated rows data")

    return array


def gen_default_binary_dataframe_data(nb=ct.default_nb, dim=ct.default_dim, start=0, auto_id=False,
                                      primary_field=ct.default_int64_field_name, nullable_fields={}, language=None):
    int_data = [i for i in range(start, start + nb)]
    int_values = pd.Series(data=int_data)
    if ct.default_int64_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_int64_field_name])
        null_data = [None for _ in range(null_number)]
        int_data = int_data[:nb - null_number] + null_data
        int_values = pd.Series(data=int_data, dtype=object)

    float_data = [np.float32(i) for i in range(start, start + nb)]
    float_values = pd.Series(data=float_data, dtype="float32")
    if ct.default_float_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_float_field_name])
        null_data = [None for _ in range(null_number)]
        float_data = float_data[:nb - null_number] + null_data
        float_values = pd.Series(data=float_data, dtype=object)

    string_data = [str(i) for i in range(start, start + nb)]
    if language:
        string_data = [generate_random_sentence(language) for _ in range(nb)]
    string_values = pd.Series(data=string_data, dtype="string")
    if ct.default_string_field_name in nullable_fields:
        null_number = int(nb * nullable_fields[ct.default_string_field_name])
        null_data = [None for _ in range(null_number)]
        string_data = string_data[:nb - null_number] + null_data
        string_values = pd.Series(data=string_data, dtype=object)

    binary_raw_values, binary_vec_values = gen_binary_vectors(nb, dim)
    df = pd.DataFrame({
        ct.default_int64_field_name: int_values,
        ct.default_float_field_name: float_values,
        ct.default_string_field_name: string_values,
        ct.default_binary_vec_field_name: binary_vec_values
    })
    if auto_id is True:
        if primary_field == ct.default_int64_field_name:
            df.drop(ct.default_int64_field_name, axis=1, inplace=True)
        elif primary_field == ct.default_string_field_name:
            df.drop(ct.default_string_field_name, axis=1, inplace=True)

    return df, binary_raw_values


def gen_default_list_sparse_data(nb=ct.default_nb, dim=ct.default_dim, start=0, with_json=False):
    int_values = [i for i in range(start, start + nb)]
    float_values = [np.float32(i) for i in range(start, start + nb)]
    string_values = [str(i) for i in range(start, start + nb)]
    json_values = [{"number": i, "string": str(i), "bool": bool(i), "list": [j for j in range(0, i)]}
                   for i in range(start, start + nb)]
    sparse_vec_values = gen_vectors(nb, dim, vector_data_type=DataType.SPARSE_FLOAT_VECTOR)
    if with_json:
        data = [int_values, float_values, string_values, json_values, sparse_vec_values]
    else:
        data = [int_values, float_values, string_values, sparse_vec_values]
    return data


def gen_default_list_data_for_bulk_insert(nb=ct.default_nb, varchar_len=2000, with_varchar_field=True):
    str_value = gen_str_by_length(length=varchar_len)
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [f"{str(i)}_{str_value}" for i in range(nb)]
    # in case of large nb, float_vec_values will be too large in memory
    # then generate float_vec_values in each loop instead of generating all at once during generate npy or json file
    float_vec_values = []  # placeholder for float_vec
    data = [int_values, float_values, string_values, float_vec_values]
    if with_varchar_field is False:
        data = [int_values, float_values, float_vec_values]
    return data


def prepare_bulk_insert_data(schema=None,
                             nb=ct.default_nb,
                             file_type="npy",
                             minio_endpoint="127.0.0.1:9000",
                             bucket_name="milvus-bucket"):
    schema = gen_default_collection_schema() if schema is None else schema
    dim = get_dim_by_schema(schema=schema)
    log.info(f"start to generate raw data for bulk insert")
    t0 = time.time()
    data = get_column_data_by_schema(schema=schema, nb=nb, skip_vectors=True)
    log.info(f"generate raw data for bulk insert cost {time.time() - t0} s")
    data_dir = "/tmp/bulk_insert_data"
    Path(data_dir).mkdir(parents=True, exist_ok=True)
    log.info(f"schema:{schema}, nb:{nb}, file_type:{file_type}, minio_endpoint:{minio_endpoint}, bucket_name:{bucket_name}")
    files = []
    log.info(f"generate {file_type} files for bulk insert")
    if file_type == "json":
        files = gen_json_files_for_bulk_insert(data, schema, data_dir)
    if file_type == "npy":
        files = gen_npy_files_for_bulk_insert(data, schema, data_dir)
    log.info(f"generated {len(files)} {file_type} files for bulk insert, cost {time.time() - t0} s")
    log.info("upload file to minio")
    client = Minio(minio_endpoint, access_key="minioadmin", secret_key="minioadmin", secure=False)
    for file_name in files:
        file_size = os.path.getsize(os.path.join(data_dir, file_name)) / 1024 / 1024
        t0 = time.time()
        client.fput_object(bucket_name, file_name, os.path.join(data_dir, file_name))
        log.info(f"upload file {file_name} to minio, size: {file_size:.2f} MB, cost {time.time() - t0:.2f} s")
    return files


def gen_column_data_by_schema(nb=ct.default_nb, schema=None, skip_vectors=False, start=0):
    return get_column_data_by_schema(nb=nb, schema=schema, skip_vectors=skip_vectors, start=start)


def get_column_data_by_schema(nb=ct.default_nb, schema=None, skip_vectors=False, start=0, random_pk=False):
    """
    Generates column data based on the given schema.
    
    Args:
        nb (int): Number of rows to generate. Defaults to ct.default_nb.
        schema (Schema): Collection schema. If None, uses default schema.
        skip_vectors (bool): Whether to skip vector fields. Defaults to False.
        start (int): Starting value for primary key fields (default: 0)
        random_pk (bool, optional): Whether to generate random primary key values (default: False)
    
    Returns:
        list: List of column data arrays matching the schema fields (excluding auto_id fields).
    """
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    fields_to_gen = []
    for field in fields:
        if not field.auto_id and not field.is_function_output:
            fields_to_gen.append(field)
    data = []
    for field in fields_to_gen:
        if field.dtype in ct.all_vector_types and skip_vectors is True:
            tmp = []
        else:
            tmp = gen_data_by_collection_field(field, nb=nb, start=start, random_pk=random_pk)
        data.append(tmp)
    return data


def convert_orm_schema_to_dict_schema(orm_schema):
    """
    Convert ORM CollectionSchema object to dict format (same as describe_collection output).

    Args:
        orm_schema: CollectionSchema object from pymilvus.orm

    Returns:
        dict: Schema in dict format compatible with MilvusClient describe_collection output
    """
    # Use the built-in to_dict() method which already provides the right structure
    schema_dict = orm_schema.to_dict()

    # to_dict() already includes:
    # - auto_id
    # - description
    # - fields (with each field's to_dict())
    # - enable_dynamic_field
    # - functions (if present)
    # - struct_fields (if present)

    return schema_dict


def gen_row_data_by_schema(nb=ct.default_nb, schema=None, start=0, random_pk=False, 
                           skip_field_names=[], desired_field_names=[], desired_dynamic_field_names=[]):
    """
    Generates row data based on the given schema.

    Args:
        nb (int): Number of rows to generate. Defaults to ct.default_nb.
        schema (Schema): Collection schema or collection info. Can be:
                        - dict (from client.describe_collection())
                        - CollectionSchema object (from ORM)
                        - None (uses default schema)
        start (int): Starting value for primary key fields. Defaults to 0.
        random_pk (bool, optional): Whether to generate random primary key values (default: False)
        skip_field_names(list, optional): whether to skip some field to gen data manually (default: [])
        desired_field_names(list, optional): only generate data for specified field names (default: [])
        desired_dynamic_field_names(list, optional): generate additional data with random types for specified dynamic fields (default: [])

    Returns:
        list[dict]: List of dictionaries where each dictionary represents a row,
                    with field names as keys and generated data as values.

    Notes:
        - Skips auto_id fields and function output fields.
        - For primary key fields, generates sequential values starting from 'start'.
        - For non-primary fields, generates random data based on field type.
        - Supports struct array fields in both dict and ORM schema formats.
    """
    # if both skip_field_names and desired_field_names are specified, raise an exception
    if skip_field_names and desired_field_names:
        raise Exception(f"Cannot specify both skip_field_names and desired_field_names")

    if schema is None:
        schema = gen_default_collection_schema()

    # Convert ORM schema to dict schema for unified processing
    if not isinstance(schema, dict):
        schema = convert_orm_schema_to_dict_schema(schema)

    # Now schema is always a dict after conversion, process it uniformly
    enable_dynamic = schema.get('enable_dynamic_field', False)
    # Get all fields from schema
    all_fields = schema.get('fields', [])
    fields = []
    for field in all_fields:
        # if desired_field_names is specified, only generate the fields in desired_field_names
        if field.get('name', None) in desired_field_names:
            fields.append(field)
        # elif desired_field_names is not specified, generate all fields
        elif not desired_field_names:
            fields.append(field)

    # Get struct_fields from schema
    struct_fields = schema.get('struct_fields', [])
    # log.debug(f"[gen_row_data_by_schema] struct_fields from schema: {len(struct_fields)} items")
    if struct_fields:
        pass
        # log.debug(f"[gen_row_data_by_schema] First struct_field: {struct_fields[0]}")

    # If struct_fields is not present, extract struct array fields from fields list
    # This happens when using client.describe_collection()
    if not struct_fields:
        struct_fields = []
        for field in fields:
            if field.get('type') == DataType.ARRAY and field.get('element_type') == DataType.STRUCT:
                # Convert field format to struct_field format
                struct_field_dict = {
                    'name': field.get('name'),
                    'max_capacity': field.get('params', {}).get('max_capacity', 100),
                    'fields': []
                }
                # Get struct fields from field - key can be 'struct_fields' or 'struct_schema'
                struct_field_list = field.get('struct_fields') or field.get('struct_schema')
                if struct_field_list:
                    # If it's a dict with 'fields' key, get the fields
                    if isinstance(struct_field_list, dict) and 'fields' in struct_field_list:
                        struct_field_dict['fields'] = struct_field_list['fields']
                    # If it's already a list, use it directly
                    elif isinstance(struct_field_list, list):
                        struct_field_dict['fields'] = struct_field_list
                struct_fields.append(struct_field_dict)

    # Get function output fields to skip
    func_output_fields = []
    functions = schema.get('functions', [])
    for func in functions:
        output_field_names = func.get('output_field_names', [])
        func_output_fields.extend(output_field_names)
    func_output_fields = list(set(func_output_fields))

    # Filter fields that need data generation
    fields_needs_data = []
    for field in fields:
        field_name = field.get('name', None)
        if field.get('auto_id', False):
            continue
        if field_name in func_output_fields or field_name in skip_field_names:
            continue
        # Skip struct array fields as they are handled separately via struct_fields
        if field.get('type') == DataType.ARRAY and field.get('element_type') == DataType.STRUCT:
            continue
        fields_needs_data.append(field)

    # Generate data for each row
    data = []
    for i in range(nb):
        tmp = {}
        # Generate data for regular fields
        for field in fields_needs_data:
            tmp[field.get('name', None)] = gen_data_by_collection_field(field, random_pk=random_pk)
            # Handle primary key fields specially
            if field.get('is_primary', False) is True and field.get('type', None) == DataType.INT64:
                tmp[field.get('name', None)] = start
                start += 1
            if field.get('is_primary', False) is True and field.get('type', None) == DataType.VARCHAR:
                tmp[field.get('name', None)] = str(start)
                start += 1

        # Generate data for struct array fields
        for struct_field in struct_fields:
            field_name = struct_field.get('name', None)
            struct_data = gen_struct_array_data(struct_field, start=start, random_pk=random_pk)
            tmp[field_name] = struct_data
        
        # generate additional data for dynamic fields
        if enable_dynamic:
            for name in desired_dynamic_field_names:
                data_types = [DataType.JSON, DataType.INT64, DataType.FLOAT, DataType.VARCHAR, DataType.BOOL, DataType.ARRAY]
                data_type = data_types[random.randint(0, len(data_types) - 1)]
                dynamic_field = gen_scalar_field(data_type, nullable=True, skip_wrapper=True)
                tmp[name] = gen_data_by_collection_field(dynamic_field)

        data.append(tmp)

    # log.debug(f"[gen_row_data_by_schema] Generated {len(data)} rows, first row keys: {list(data[0].keys()) if data else []}")
    return data


def get_fields_map(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    fields_map = {}
    for field in fields:
        fields_map[field.name] = field.dtype
    return fields_map


def get_int64_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.INT64:
            return field.name
    return None


def get_varchar_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.VARCHAR:
            return field.name
    return None


def get_text_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    if not hasattr(schema, "functions"):
        return []
    functions = schema.functions
    bm25_func = [func for func in functions if func.type == FunctionType.BM25]
    bm25_inputs = []
    for func in bm25_func:
        bm25_inputs.extend(func.input_field_names)
    bm25_inputs = list(set(bm25_inputs))

    return bm25_inputs


def get_text_match_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    text_match_field_list = []
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.VARCHAR and field.params.get("enable_match", False):
            text_match_field_list.append(field.name)
    return text_match_field_list


def get_float_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.FLOAT or field.dtype == DataType.DOUBLE:
            return field.name
    return None


def get_float_vec_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.FLOAT_VECTOR:
            return field.name
    return None


def get_float_vec_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR, DataType.BFLOAT16_VECTOR]:
            vec_fields.append(field.name)
    return vec_fields


def get_scalar_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.BOOL, DataType.INT8, DataType.INT16, DataType.INT32, DataType.INT64, DataType.FLOAT,
                           DataType.DOUBLE, DataType.VARCHAR]:
            vec_fields.append(field.name)
    return vec_fields

def get_json_field_name_list(schema=None):
    json_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.JSON:
            json_fields.append(field.name)
    return json_fields

def get_geometry_field_name_list(schema=None):
    geometry_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.GEOMETRY:
            geometry_fields.append(field.name)
    return geometry_fields

def get_binary_vec_field_name(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.BINARY_VECTOR:
            return field.name
    return None


def get_binary_vec_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.BINARY_VECTOR]:
            vec_fields.append(field.name)
    return vec_fields

def get_int8_vec_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype in [DataType.INT8_VECTOR]:
            vec_fields.append(field.name)
    return vec_fields

def get_emb_list_field_name_list(schema=None):
    vec_fields = []
    if schema is None:
        schema = gen_default_collection_schema()
    struct_fields = schema.struct_fields
    for struct_field in struct_fields:
        for field in struct_field.fields:
            if field.dtype in [DataType.FLOAT_VECTOR]:
                vec_fields.append(f"{struct_field.name}[{field.name}]")
    return vec_fields

def get_bm25_vec_field_name_list(schema=None):
    if not hasattr(schema, "functions"):
        return []
    functions = schema.functions
    bm25_func = [func for func in functions if func.type == FunctionType.BM25]
    bm25_outputs = []
    for func in bm25_func:
        bm25_outputs.extend(func.output_field_names)
    bm25_outputs = list(set(bm25_outputs))

    return bm25_outputs

def get_dim_by_schema(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    for field in fields:
        if field.dtype == DataType.FLOAT_VECTOR or field.dtype == DataType.BINARY_VECTOR:
            dim = field.params['dim']
            return dim
    return None

def get_dense_anns_field_name_list(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()
    fields = schema.fields
    anns_fields = []
    for field in fields:
        if field.dtype in [DataType.FLOAT_VECTOR,DataType.FLOAT16_VECTOR,DataType.BFLOAT16_VECTOR, DataType.INT8_VECTOR, DataType.BINARY_VECTOR]:
            item = {
                "name": field.name,
                "dtype": field.dtype,
                "dim": field.params['dim']
            }
            anns_fields.append(item)
    return anns_fields

def get_struct_array_vector_field_list(schema=None):
    if schema is None:
        schema = gen_default_collection_schema()

    struct_fields = schema.struct_fields
    struct_vector_fields = []

    for struct_field in struct_fields:
            struct_field_name = struct_field.name
            # Check each sub-field for vector types
            for sub_field in struct_field.fields:
                sub_field_name = sub_field.name if hasattr(sub_field, 'name') else sub_field.get('name')
                sub_field_dtype = sub_field.dtype if hasattr(sub_field, 'dtype') else sub_field.get('type')

                if sub_field_dtype in [DataType.FLOAT_VECTOR, DataType.FLOAT16_VECTOR,
                                      DataType.BFLOAT16_VECTOR, DataType.INT8_VECTOR,
                                      DataType.BINARY_VECTOR]:
                    # Get dimension
                    if hasattr(sub_field, 'params'):
                        dim = sub_field.params.get('dim')
                    else:
                        dim = sub_field.get('params', {}).get('dim')

                    item = {
                        "struct_field": struct_field_name,
                        "vector_field": sub_field_name,
                        "anns_field": f"{struct_field_name}[{sub_field_name}]",
                        "dtype": sub_field_dtype,
                        "dim": dim
                    }
                    struct_vector_fields.append(item)

    return struct_vector_fields


def gen_varchar_data(length: int, nb: int, text_mode=False):
    if text_mode:
        return [fake.text() for _ in range(nb)]
    else:
        return ["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(nb)]


def gen_struct_array_data(struct_field, start=0, random_pk=False):
    """
    Generates struct array data based on the struct field schema.

    Args:
        struct_field: Either a dict (from dict schema) or StructFieldSchema object (from ORM schema)
        start: Starting value for primary key fields
        random_pk: Whether to generate random primary key values

    Returns:
        List of struct data dictionaries
    """
    struct_array_data = []

    # Handle both dict and object formats
    if isinstance(struct_field, dict):
        max_capacity = struct_field.get('max_capacity', 100)
        fields = struct_field.get('fields', [])
    else:
        # StructFieldSchema object
        max_capacity = getattr(struct_field, 'max_capacity', 100) or 100
        fields = struct_field.fields

    arr_len = random.randint(1, max_capacity)
    for _ in range(arr_len):
        struct_data = {}
        for field in fields:
            field_name = field.get('name') if isinstance(field, dict) else field.name
            struct_data[field_name] = gen_data_by_collection_field(field, nb=None, start=start, random_pk=random_pk)
        struct_array_data.append(struct_data)
    return struct_array_data

def gen_data_by_collection_field(field, nb=None, start=0, random_pk=False):
    """
    Generates test data for a given collection field based on its data type and properties.
    
    Args:
        field (dict or Field): Field information, either as a dictionary (v2 client) or Field object (ORM client)
        nb (int, optional): Bumber of data batch to generate. If None, returns a single value which usually used by row data generation
        start (int, optional): Starting value for primary key fields (default: 0)
        random_pk (bool, optional): Whether to generate random primary key values (default: False)
    Returns:
        Single value if nb is None, otherwise returns a list of generated values
    
    Notes:
        - Handles various data types including primitive types, vectors, arrays and JSON
        - For nullable fields, generates None values approximately 20% of the time
        - Special handling for primary key fields (sequential values)
        - For varchar field, use min(20, max_length) to gen data
        - For vector fields, generates random vectors of specified dimension
        - For array fields, generates arrays filled with random values of element type
    """
    
    if isinstance(field, dict):
        # for v2 client, it accepts a dict of field info
        nullable = field.get('nullable', False)
        data_type = field.get('type', None)
        params = field.get('params', {}) or {}
        enable_analyzer = params.get("enable_analyzer", False)
        is_primary = field.get('is_primary', False)
    else:
        # for ORM client, it accepts a field object
        nullable = field.nullable
        data_type = field.dtype
        enable_analyzer = field.params.get("enable_analyzer", False)
        is_primary = field.is_primary

    # generate data according to the data type
    if data_type == DataType.BOOL:
        if nb is None:
            return random.choice([True, False]) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [random.choice([True, False]) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0  and random.random() < 0.4 else random.choice([True, False]) for i in range(nb)]
    elif data_type == DataType.INT8:
        if nb is None:
            return random.randint(-128, 127) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [random.randint(-128, 127) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-128, 127) for i in range(nb)]
    elif data_type == DataType.INT16:
        if nb is None:
            return random.randint(-32768, 32767) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [random.randint(-32768, 32767) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-32768, 32767) for i in range(nb)]
    elif data_type == DataType.INT32:
        if nb is None:
            return random.randint(-2147483648, 2147483647) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [random.randint(-2147483648, 2147483647) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-2147483648, 2147483647) for i in range(nb)]
    elif data_type == DataType.INT64:
        if nb is None:
            return random.randint(-9223372036854775808, 9223372036854775807) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            if is_primary is True and random_pk is False: 
                return [i for i in range(start, start+nb)]
            else:
                return [random.randint(-9223372036854775808, 9223372036854775807) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-9223372036854775808, 9223372036854775807) for i in range(nb)]
    elif data_type == DataType.FLOAT:
        if nb is None:
            return np.float32(random.random()) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [np.float32(random.random()) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else np.float32(random.random()) for i in range(nb)]
    elif data_type == DataType.DOUBLE:
        if nb is None:
            return np.float64(random.random()) if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [np.float64(random.random()) for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else np.float64(random.random()) for i in range(nb)]
    elif data_type == DataType.VARCHAR:
        if isinstance(field, dict):
            max_length = field.get('params')['max_length']
        else:
            max_length = field.params['max_length']
        max_length = min(20, max_length-1)
        length = random.randint(0, max_length)
        if nb is None:
            return gen_varchar_data(length=length, nb=1, text_mode=enable_analyzer)[0] if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            if is_primary is True and random_pk is False:
                return [str(i) for i in range(start, start+nb)]
            else:
                return gen_varchar_data(length=length, nb=nb, text_mode=enable_analyzer)
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else gen_varchar_data(length=length, nb=1, text_mode=enable_analyzer)[0] for i in range(nb)]
    elif data_type == DataType.JSON:
        if nb is None:
            return {"name": fake.name(), "address": fake.address(), "count": random.randint(0, 100)} if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [{"name": str(i), "address": i, "count": random.randint(0, 100)} for i in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else {"name": str(i), "address": i, "count": random.randint(0, 100)} for i in range(nb)]
    elif data_type == DataType.GEOMETRY:
        if nb is None:
            lon = random.uniform(-180, 180)
            lat = random.uniform(-90, 90)
            return f"POINT({lon} {lat})" if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [f"POINT({random.uniform(-180, 180)} {random.uniform(-90, 90)})" for _ in range(nb)]
        else:
            # gen 20% none data for nullable field
            return [None if i % 2 == 0 and random.random() < 0.4 else f"POINT({random.uniform(-180, 180)} {random.uniform(-90, 90)})" for i in range(nb)]

    elif data_type in ct.all_vector_types:
        if isinstance(field, dict):
            dim = ct.default_dim if data_type == DataType.SPARSE_FLOAT_VECTOR else field.get('params')['dim']
        else:
            dim = ct.default_dim if data_type == DataType.SPARSE_FLOAT_VECTOR else field.params['dim']
        if nb is None:
            return gen_vectors(1, dim, vector_data_type=data_type)[0]
        if nullable is False:
            return gen_vectors(nb, dim, vector_data_type=data_type)
        else:
            # gen 20% none data for nullable vector field
            vectors = gen_vectors(nb, dim, vector_data_type=data_type)
            return [None if i % 2 == 0 and random.random() < 0.4 else vectors[i] for i in range(nb)]
    elif data_type == DataType.ARRAY:
        if isinstance(field, dict):
            max_capacity = field.get('params')['max_capacity']
            element_type = field.get('element_type')
        else:
            max_capacity = field.params['max_capacity']
            element_type = field.element_type

        # Struct array fields are handled separately in gen_row_data_by_schema
        # by processing struct_fields, so skip here
        if element_type == DataType.STRUCT:
            return None

        if element_type == DataType.INT8:
            if nb is None:
                return [random.randint(-128, 127) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[random.randint(-128, 127) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-128, 127) for i in range(nb)]
        if element_type == DataType.INT16:
            if nb is None:
                return [random.randint(-32768, 32767) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[random.randint(-32768, 32767) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-32768, 32767) for i in range(nb)]
        if element_type == DataType.INT32:
            if nb is None:
                return [random.randint(-2147483648, 2147483647) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[random.randint(-2147483648, 2147483647) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-2147483648, 2147483647) for i in range(nb)]
        if element_type == DataType.INT64:
            if nb is None:
                return [random.randint(-9223372036854775808, 9223372036854775807) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[random.randint(-9223372036854775808, 9223372036854775807) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else random.randint(-9223372036854775808, 9223372036854775807) for i in range(nb)]
        if element_type == DataType.BOOL:
            if nb is None:
                return [random.choice([True, False]) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[random.choice([True, False]) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else random.choice([True, False]) for i in range(nb)]
        if element_type == DataType.FLOAT:
            if nb is None:
                return [np.float32(random.random()) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[np.float32(random.random()) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else np.float32(random.random()) for i in range(nb)]
        if element_type == DataType.DOUBLE:
            if nb is None:
                return [np.float64(random.random()) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [[np.float64(random.random()) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else np.float64(random.random()) for i in range(nb)]
        if element_type == DataType.VARCHAR:
            if isinstance(field, dict):
                max_length = field.get('params')['max_length']
            else:
                max_length = field.params['max_length']
            max_length = min(20, max_length - 1)
            length = random.randint(0, max_length)
            if nb is None:
                return ["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(max_capacity)] if random.random() < 0.8 or nullable is False else None
            if nullable is False:
                return [["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(max_capacity)] for _ in range(nb)]
            else:
                # gen 20% none data for nullable field
                return [None if i % 2 == 0 and random.random() < 0.4 else "".join([chr(random.randint(97, 122)) for _ in range(length)]) for i in range(nb)]
    
    elif data_type == DataType.TIMESTAMPTZ:
        if nb is None:
            return gen_timestamptz_str() if random.random() < 0.8 or nullable is False else None
        if nullable is False:
            return [gen_timestamptz_str() for _ in range(nb)]
        # gen 20% none data for nullable field
        return [None if i % 2 == 0 and random.random() < 0.4 else gen_timestamptz_str() for i in range(nb)]
    
    else:
        raise MilvusException(message=f"gen data failed, data type {data_type} not implemented")
    return None

def gen_timestamptz_str():
    """
    Generate a timestamptz string
    Example:
        "2024-12-31 22:00:00"
        "2024-12-31T22:00:00"
        "2024-12-31T22:00:00+08:00"
        "2024-12-31T22:00:00-08:00"
        "2024-12-31T22:00:00Z"
    """
    base = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(
        days=random.randint(0, 365 * 3), seconds=random.randint(0, 86399)
    )
    # 2/3 chance to generate timezone-aware string, otherwise naive
    if random.random() < 2 / 3:
        # 20% chance to use 'Z' (UTC), always RFC3339 with 'T'
        if random.random() < 0.2:
            return base.strftime("%Y-%m-%dT%H:%M:%S") + "Z"
        # otherwise use explicit offset
        offset_hours = random.randint(-12, 14)
        if offset_hours == -12 or offset_hours == 14:
            offset_minutes = 0
        else:
            offset_minutes = random.choice([0, 30])
        tz = timezone(timedelta(hours=offset_hours, minutes=offset_minutes))
        local_dt = base.astimezone(tz)
        tz_str = local_dt.strftime("%z")  # "+0800"
        tz_str = tz_str[:3] + ":" + tz_str[3:]  # "+08:00"
        dt_str = local_dt.strftime("%Y-%m-%dT%H:%M:%S")
        return dt_str + tz_str
    else:
        # naive time string (no timezone), e.g. "2024-12-31 22:00:00"
        return base.strftime("%Y-%m-%d %H:%M:%S")

def gen_varchar_values(nb: int, length: int = 0):
    return ["".join([chr(random.randint(97, 122)) for _ in range(length)]) for _ in range(nb)]


def gen_values(schema: CollectionSchema, nb, start_id=0, default_values: dict = {}, random_pk=False):
    """
    generate default value according to the collection fields,
    which can replace the value of the specified field
    """
    data = []
    for field in schema.fields:
        default_value = default_values.get(field.name, None)
        if default_value is not None:
            data.append(default_value)
        elif field.auto_id is False:
            data.append(gen_data_by_collection_field(field, nb, start_id, random_pk=random_pk))
    return data


def gen_field_values(schema: CollectionSchema, nb, start_id=0, default_values: dict = {}, random_pk=False) -> dict:
    """
    generate default value according to the collection fields,
    which can replace the value of the specified field

    return: <dict>
        <field name>: <value list>
    """
    data = {}
    for field in schema.fields:
        default_value = default_values.get(field.name, None)
        if default_value is not None:
            data[field.name] = default_value
        elif field.auto_id is False:
            data[field.name] = gen_data_by_collection_field(field, nb, start_id * nb, random_pk=random_pk)
    return data


def gen_json_files_for_bulk_insert(data, schema, data_dir):
    for d in data:
        if len(d) > 0:
            nb = len(d)
    dim = get_dim_by_schema(schema)
    vec_field_name = get_float_vec_field_name(schema)
    fields_name = [field.name for field in schema.fields]
    # get vec field index
    vec_field_index = fields_name.index(vec_field_name)
    uuid_str = str(uuid.uuid4())
    log.info(f"file dir name: {uuid_str}")
    file_name = f"{uuid_str}/bulk_insert_data_source_dim_{dim}_nb_{nb}.json"
    files = [file_name]
    data_source = os.path.join(data_dir, file_name)
    Path(data_source).parent.mkdir(parents=True, exist_ok=True)
    log.info(f"file name: {data_source}")
    with open(data_source, "w") as f:
        f.write("{")
        f.write("\n")
        f.write('"rows":[')
        f.write("\n")
        for i in range(nb):
            entity_value = [None for _ in range(len(fields_name))]
            for j in range(len(data)):
                if j == vec_field_index:
                    entity_value[j] = [random.random() for _ in range(dim)]
                else:
                    entity_value[j] = data[j][i]
            entity = dict(zip(fields_name, entity_value))
            f.write(json.dumps(entity, indent=4, default=to_serializable))
            if i != nb - 1:
                f.write(",")
            f.write("\n")
        f.write("]")
        f.write("\n")
        f.write("}")
    return files


def gen_npy_files_for_bulk_insert(data, schema, data_dir):
    for d in data:
        if len(d) > 0:
            nb = len(d)
    dim = get_dim_by_schema(schema)
    vec_field_name = get_float_vec_field_name(schema)
    fields_name = [field.name for field in schema.fields]
    files = []
    uuid_str = uuid.uuid4()
    for field in fields_name:
        files.append(f"{uuid_str}/{field}.npy")
    for i, file in enumerate(files):
        data_source = os.path.join(data_dir, file)
        #  mkdir for npy file
        Path(data_source).parent.mkdir(parents=True, exist_ok=True)
        log.info(f"save file {data_source}")
        if vec_field_name in file:
            log.info(f"generate {nb} vectors with dim {dim} for {data_source}")
            with NpyAppendArray(data_source, "wb") as npaa:
                for j in range(nb):
                    vector = np.array([[random.random() for _ in range(dim)]])
                    npaa.append(vector)

        elif isinstance(data[i][0], dict):
            tmp = []
            for d in data[i]:
                tmp.append(json.dumps(d))
            data[i] = tmp
            np.save(data_source, np.array(data[i]))
        else:
            np.save(data_source, np.array(data[i]))
    return files


def gen_default_tuple_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [str(i) for i in range(nb)]
    float_vec_values = gen_vectors(nb, dim)
    data = (int_values, float_values, string_values, float_vec_values)
    return data


def gen_numpy_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = np.arange(nb, dtype='int64')
    float_values = np.arange(nb, dtype='float32')
    string_values = [np.str_(i) for i in range(nb)]
    json_values = [{"number": i, "string": str(i), "bool": bool(i),
                    "list": [j for j in range(i, i + ct.default_json_list_length)]} for i in range(nb)]
    float_vec_values = gen_vectors(nb, dim)
    data = [int_values, float_values, string_values, json_values, float_vec_values]
    return data


def gen_default_binary_list_data(nb=ct.default_nb, dim=ct.default_dim):
    int_values = [i for i in range(nb)]
    float_values = [np.float32(i) for i in range(nb)]
    string_values = [str(i) for i in range(nb)]
    binary_raw_values, binary_vec_values = gen_binary_vectors(nb, dim)
    data = [int_values, float_values, string_values, binary_vec_values]
    return data, binary_raw_values


def gen_autoindex_params():
    index_params = [
        {},
        {"metric_type": "IP"},
        {"metric_type": "L2"},
        {"metric_type": "COSINE"},
        {"index_type": "AUTOINDEX"},
        {"index_type": "AUTOINDEX", "metric_type": "L2"},
        {"index_type": "AUTOINDEX", "metric_type": "COSINE"},
        {"index_type": "IVF_FLAT", "metric_type": "L2", "nlist": "1024", "m": "100"},
        {"index_type": "DISKANN", "metric_type": "L2"},
        {"index_type": "IVF_PQ", "nlist": "128", "m": "16", "nbits": "8", "metric_type": "IP"},
        {"index_type": "IVF_SQ8", "nlist": "128", "metric_type": "COSINE"}
    ]
    return index_params


def gen_invalid_field_types():
    field_types = [
        6,
        1.0,
        [[]],
        {},
        (),
        "",
        "a"
    ]
    return field_types


def gen_invalid_search_params_type():
    invalid_search_key = 100
    search_params = []
    for index_type in ct.all_index_types:
        if index_type == "FLAT":
            continue
        # search_params.append({"index_type": index_type, "search_params": {"invalid_key": invalid_search_key}})
        if index_type in ["IVF_FLAT", "IVF_SQ8", "IVF_PQ", "BIN_FLAT", "BIN_IVF_FLAT"]:
            for nprobe in ct.get_invalid_ints:
                ivf_search_params = {"index_type": index_type, "search_params": {"nprobe": nprobe}}
                search_params.append(ivf_search_params)
        elif index_type in ["HNSW"]:
            for ef in ct.get_invalid_ints:
                hnsw_search_param = {"index_type": index_type, "search_params": {"ef": ef}}
                search_params.append(hnsw_search_param)
        elif index_type == "ANNOY":
            for search_k in ct.get_invalid_ints:
                if isinstance(search_k, int):
                    continue
                annoy_search_param = {"index_type": index_type, "search_params": {"search_k": search_k}}
                search_params.append(annoy_search_param)
        elif index_type == "SCANN":
            for reorder_k in ct.get_invalid_ints:
                if isinstance(reorder_k, int):
                    continue
                scann_search_param = {"index_type": index_type, "search_params": {"nprobe": 8, "reorder_k": reorder_k}}
                search_params.append(scann_search_param)
        elif index_type == "DISKANN":
            for search_list in ct.get_invalid_ints[1:]:
                diskann_search_param = {"index_type": index_type, "search_params": {"search_list": search_list}}
                search_params.append(diskann_search_param)
    return search_params


# def gen_search_param(index_type, metric_type="L2"):
#     search_params = []
#     if index_type in ["FLAT", "IVF_FLAT", "IVF_SQ8", "IVF_PQ", "GPU_IVF_FLAT", "GPU_IVF_PQ"]:
#         if index_type in ["GPU_FLAT"]:
#             ivf_search_params = {"metric_type": metric_type, "params": {}}
#             search_params.append(ivf_search_params)
#         else:
#             search_params.append({"metric_type": index_type, "params": {"nprobe": 100}})
#             search_params.append({"metric_type": index_type, "nprobe": 100})
#             search_params.append({"metric_type": index_type})
#             search_params.append({"params": {"nprobe": 100}})
#             search_params.append({"nprobe": 100})
#             search_params.append({})
#     elif index_type in ["BIN_FLAT", "BIN_IVF_FLAT"]:
#         if metric_type not in ct.binary_metrics:
#             log.error("Metric type error: binary index only supports distance type in (%s)" % ct.binary_metrics)
#             # default metric type for binary index
#             metric_type = "JACCARD"
#         for nprobe in [64, 128]:
#             binary_search_params = {"metric_type": metric_type, "params": {"nprobe": nprobe}}
#             search_params.append(binary_search_params)
#     elif index_type in ["HNSW"]:
#         for ef in [64, 1500, 32768]:
#             hnsw_search_param = {"metric_type": metric_type, "params": {"ef": ef}}
#             search_params.append(hnsw_search_param)
#     elif index_type == "ANNOY":
#         for search_k in [1000, 5000]:
#             annoy_search_param = {"metric_type": metric_type, "params": {"search_k": search_k}}
#             search_params.append(annoy_search_param)
#     elif index_type == "SCANN":
#         for reorder_k in [1200, 3000]:
#             scann_search_param = {"metric_type": metric_type, "params": {"nprobe": 64, "reorder_k": reorder_k}}
#             search_params.append(scann_search_param)
#     elif index_type == "DISKANN":
#         for search_list in [20, 300, 1500]:
#             diskann_search_param = {"metric_type": metric_type, "params": {"search_list": search_list}}
#             search_params.append(diskann_search_param)
#     elif index_type == "IVF_RABITQ":
#         for rbq_bits_query in [7]:
#             ivf_rabitq_search_param = {"metric_type": metric_type,
#                                        "params": {"rbq_bits_query": rbq_bits_query, "nprobe": 8, "refine_k": 10.0}}
#             search_params.append(ivf_rabitq_search_param)
#     else:
#         log.error("Invalid index_type.")
#         raise Exception("Invalid index_type.")
#     log.debug(search_params)
#
#     return search_params
#

def gen_autoindex_search_params():
    search_params = [
        {},
        {"metric_type": "IP"},
        {"nlist": "1024"},
        {"efSearch": "100"},
        {"search_k": "1000"}
    ]
    return search_params


def gen_all_type_fields():
    fields = []
    for k, v in DataType.__members__.items():
        if v != DataType.UNKNOWN:
            field, _ = ApiFieldSchemaWrapper().init_field_schema(name=k.lower(), dtype=v)
            fields.append(field)
    return fields


def gen_normal_expressions_and_templates():
    """
    Gen a list of filter in expression-format(as a string) and template-format(as a dict)
    The two formats equals to each other.
    """
    expressions = [
        ["", {"expr": "", "expr_params": {}}],
        ["int64 > 0", {"expr": "int64 > {value_0}", "expr_params": {"value_0": 0}}],
        ["(int64 > 0 && int64 < 400) or (int64 > 500 && int64 < 1000)",
         {"expr": "(int64 > {value_0} && int64 < {value_1}) or (int64 > {value_2} && int64 < {value_3})",
          "expr_params": {"value_0": 0, "value_1": 400, "value_2": 500, "value_3": 1000}}],
        ["int64 not in [1, 2, 3]", {"expr": "int64 not in {value_0}", "expr_params": {"value_0": [1, 2, 3]}}],
        ["int64 in [1, 2, 3] and float != 2", {"expr": "int64 in {value_0} and float != {value_1}",
                                               "expr_params": {"value_0": [1, 2, 3], "value_1": 2}}],
        ["int64 == 0 || float == 10**2 || (int64 + 1) == 3",
         {"expr": "int64 == {value_0} || float == {value_1} || (int64 + {value_2}) == {value_3}",
          "expr_params": {"value_0": 0, "value_1": 10**2, "value_2": 1, "value_3": 3}}],
        ["0 <= int64 < 400 and int64 % 100 == 0",
         {"expr": "{value_0} <= int64 < {value_1} and int64 % {value_2} == {value_0}",
          "expr_params": {"value_0": 0, "value_1": 400, "value_2": 100}}],
        ["200+300 < int64 <= 500+500", {"expr": "{value_0} < int64 <= {value_1}",
                                        "expr_params": {"value_1": 500+500, "value_0": 200+300}}],
        ["int64 > 400 && int64 < 200", {"expr": "int64 > {value_0} && int64 < {value_1}",
                                        "expr_params": {"value_0": 400, "value_1": 200}}],
        ["int64 in [300/2, 900%40, -10*30+800, (100+200)*2] or float in [+3**6, 2**10/2]",
         {"expr": "int64 in {value_0} or float in {value_1}",
          "expr_params": {"value_0": [int(300/2), 900%40, -10*30+800, (100+200)*2], "value_1": [+3**6*1.0, 2**10/2*1.0]}}],
        ["float <= -4**5/2 && float > 500-1 && float != 500/2+260",
         {"expr": "float <= {value_0} && float > {value_1} && float != {value_2}",
          "expr_params": {"value_0": -4**5/2, "value_1": 500-1, "value_2": 500/2+260}}],
    ]
    return expressions


def gen_json_field_expressions_and_templates():
    """
    Gen a list of filter in expression-format(as a string) and template-format(as a dict)
    The two formats equals to each other.
    """
    expressions = [
        ["json_field['number'] > 0", {"expr": "json_field['number'] > {value_0}", "expr_params": {"value_0": 0}}],
        ["0 <= json_field['number'] < 400 or 1000 > json_field['number'] >= 500",
         {"expr": "{value_0} <= json_field['number'] < {value_1} or {value_2} > json_field['number'] >= {value_3}",
          "expr_params": {"value_0": 0, "value_1": 400, "value_2": 1000, "value_3": 500}}],
        ["json_field['number'] not in [1, 2, 3]", {"expr": "json_field['number'] not in {value_0}",
                                                   "expr_params": {"value_0": [1, 2, 3]}}],
        ["json_field['number'] in [1, 2, 3] and json_field['float'] != 2",
         {"expr": "json_field['number'] in {value_0} and json_field['float'] != {value_1}",
          "expr_params": {"value_0": [1, 2, 3], "value_1": 2}}],
        ["json_field['number'] == 0 || json_field['float'] == 10**2 || json_field['number'] + 1 == 3",
         {"expr": "json_field['number'] == {value_0} || json_field['float'] == {value_1} || json_field['number'] + {value_2} == {value_3}",
          "expr_params": {"value_0": 0, "value_1": 10**2, "value_2": 1, "value_3": 3}}],
        ["json_field['number'] < 400 and json_field['number'] >= 100 and json_field['number'] % 100 == 0",
         {"expr": "json_field['number'] < {value_0} and json_field['number'] >= {value_1} and json_field['number'] % {value_1} == 0",
          "expr_params": {"value_0": 400, "value_1": 100}}],
        ["json_field['float'] > 400 && json_field['float'] < 200", {"expr": "json_field['float'] > {value_0} && json_field['float'] < {value_1}",
                                                                    "expr_params": {"value_0": 400, "value_1": 200}}],
        ["json_field['number'] in [300/2, -10*30+800, (100+200)*2] or json_field['float'] in [+3**6, 2**10/2]",
         {"expr": "json_field['number'] in {value_0} or json_field['float'] in {value_1}",
          "expr_params": {"value_0": [int(300/2), -10*30+800, (100+200)*2], "value_1": [+3**6*1.0, 2**10/2*1.0]}}],
        ["json_field['float'] <= -4**5/2 && json_field['float'] > 500-1 && json_field['float'] != 500/2+260",
         {"expr": "json_field['float'] <= {value_0} && json_field['float'] > {value_1} && json_field['float'] != {value_2}",
          "expr_params": {"value_0": -4**5/2, "value_1": 500-1, "value_2": 500/2+260}}],
    ]

    return expressions


def gen_json_field_expressions_all_single_operator(json_cast_type=None):
    """
    Gen a list of filter in expression-format(as a string)
    :param json_cast_type: Optional parameter to specify the JSON cast type (e.g., "ARRAY_DOUBLE")
    """
    if json_cast_type == "ARRAY_DOUBLE":
        # For ARRAY_DOUBLE type, use array-specific expressions
        expressions = [
            "json_contains(json_field['a'], 1)", "JSON_CONTAINS(json_field['a'], 1)",
            "json_contains(json_field['a'], 1.0)", "json_contains(json_field['a'], 2)",
            "json_contains_all(json_field['a'], [1, 2])", "JSON_CONTAINS_ALL(json_field['a'], [1, 2])",
            "json_contains_all(json_field['a'], [1.0, 2.0])", "json_contains_all(json_field['a'], [2, 4])",
            "json_contains_any(json_field['a'], [1, 2])", "JSON_CONTAINS_ANY(json_field['a'], [1, 2])",
            "json_contains_any(json_field['a'], [1.0, 2.0])", "json_contains_any(json_field['a'], [2, 4])",
            "array_contains(json_field['a'], 1)", "ARRAY_CONTAINS(json_field['a'], 1)",
            "array_contains(json_field['a'], 1.0)", "array_contains(json_field['a'], 2)",
            "array_contains_all(json_field['a'], [1, 2])", "ARRAY_CONTAINS_ALL(json_field['a'], [1, 2])",
            "array_contains_all(json_field['a'], [1.0, 2.0])", "array_contains_all(json_field['a'], [2, 4])",
            "array_contains_any(json_field['a'], [1, 2])", "ARRAY_CONTAINS_ANY(json_field['a'], [1, 2])",
            "array_contains_any(json_field['a'], [1.0, 2.0])", "array_contains_any(json_field['a'], [2, 4])",
            "array_length(json_field['a']) < 10", "ARRAY_LENGTH(json_field['a']) < 10"
        ]
    else:
        expressions = ["json_field['a'] <= 1", "json_field['a'] <= 1.0", "json_field['a'] >= 1", "json_field['a'] >= 1.0",
                       "json_field['a'] < 2", "json_field['a'] < 2.0", "json_field['a'] > 0", "json_field['a'] > 0.0",
                       "json_field['a'] <= '1'", "json_field['a'] >= '1'", "json_field['a'] < '2'", "json_field['a'] > '0'",
                       "json_field['a'] == 1", "json_field['a'] == 1.0", "json_field['a'] == True",
                       "json_field['a'] == 9707199254740993.0", "json_field['a'] == 9707199254740992",
                       "json_field['a'] == '1'",
                       "json_field['a'] != '1'", "json_field['a'] like '1%'", "json_field['a'] like '%1'",
                       "json_field['a'] like '%1%'", "json_field['a'] LIKE '1%'", "json_field['a'] LIKE '%1'",
                       "json_field['a'] LIKE '%1%'", "EXISTS json_field['a']", "exists json_field['a']",
                       "EXISTS json_field['a']['b']", "exists json_field['a']['b']", "json_field['a'] + 1 >= 2",
                       "json_field['a'] - 1 <= 0", "json_field['a'] + 1.0 >= 2", "json_field['a'] - 1.0 <= 0",
                       "json_field['a'] * 2 == 2", "json_field['a'] * 1.0 == 1.0", "json_field / 1 == 1",
                       "json_field['a'] / 1.0 == 1", "json_field['a'] % 10 == 1", "json_field['a'] == 1**2",
                       "json_field['a'][0] == 1 && json_field['a'][1] == 2",
                       "json_field['a'][0] == 1 and json_field['a'][1] == 2",
                       "json_field['a'][0]['b'] >=1 && json_field['a'][2] == 3",
                       "json_field['a'][0]['b'] >=1 and json_field['a'][2] == 3",
                       "json_field['a'] == 1 || json_field['a'] == '1'", "json_field['a'] == 1 or json_field['a'] == '1'",
                       "json_field['a'][0]['b'] >=1  || json_field['a']['b'] >=1",
                       "json_field['a'][0]['b'] >=1 or json_field['a']['b'] >=1",
                       "json_field['a'] in [1]",  "json_field is null", "json_field IS NULL", "json_field is not null", "json_field IS NOT NULL",
                       "json_field['a'] is null", "json_field['a'] IS NULL", "json_field['a'] is not null", "json_field['a'] IS NOT NULL"
                       ]

    return expressions


def gen_field_expressions_all_single_operator_each_field(field = ct.default_int64_field_name):
    """
    Gen a list of filter in expression-format(as a string)
    """
    if field in [ct.default_int8_field_name, ct.default_int16_field_name, ct.default_int32_field_name,
                 ct.default_int64_field_name]:
        expressions = [f"{field} <= 1", f"{field} >= 1",
                       f"{field} < 2",  f"{field} > 0",
                       f"{field} == 1", f"{field} != 1",
                       f"{field} == 9707199254740992", f"{field} != 9707199254740992",
                       f"{field} + 1 >= 2", f"{field} - 1 <= 0",
                       f"{field} * 2 == 2", f"{field} / 1 == 1",
                       f"{field} % 10 == 1", f"{field} == 1 || {field} == 2",
                       f"{field} == 1 or {field} == 2",
                       f"{field} in [1]", f"{field} not in [1]",
                       f"{field} is null", f"{field} IS NULL",
                       f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    elif field in [ct.default_bool_field_name]:
        expressions = [f"{field} == True", f"{field} == False",
                       f"{field} != True", f"{field} != False",
                       f"{field} <= True", f"{field} >= True",
                       f"{field} <= False", f"{field} >= False",
                       f"{field} < True", f"{field} > True",
                       f"{field} < False", f"{field} > False",
                       f"{field} == True && {field} == False",
                       f"{field} == True and {field} == False ",
                       f"{field} == True || {field} == False",
                       f"{field} == True or {field} == False",
                       f"{field} in [True]", f"{field} in [False]", f"{field} in [True, False]",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"]
    elif field in [ct.default_float_field_name, ct.default_double_field_name]:
        expressions = [f"{field} <= 1", f"{field} >= 1",
                       f"{field} < 2", f"{field} > 0",
                       f"{field} == 1", f"{field} != 1",
                       f"{field} == 9707199254740992", f"{field} != 9707199254740992",
                       f"{field} <= 1.0", f"{field} >= 1.0",
                       f"{field} < 2.0", f"{field} > 0.0",
                       f"{field} == 1.0", f"{field} != 1.0",
                       f"{field} == 9707199254740992.0", f"{field} != 9707199254740992.0",
                       f"{field} - 1 <= 0", f"{field} + 1.0 >= 2",
                       f"{field} - 1.0 <= 0", f"{field} * 2 == 2",
                       f"{field} * 1.0 == 1.0", f"{field} / 1 == 1",
                       f"{field} / 1.0 == 1.0", f"{field} == 1**2",
                       f"{field} == 1 && {field} == 2",
                       f"{field} == 1 and {field} == 2.0",
                       f"{field} >=1 && {field} == 3.0",
                       f"{field} >=1 and {field} == 3",
                       f"{field} == 1 || {field} == 2.0",
                       f"{field} == 1 or {field} == 2.0",
                       f"{field} >= 1  || {field} <=2.0",
                       f"{field} >= 1.0 or {field} <= 2.0",
                       f"{field} in [1]", f"{field} in [1, 2]",
                       f"{field} in [1.0]", f"{field} in [1.0, 2.0]",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    elif field in [ct.default_string_field_name]:
        expressions = [f"{field} <= '1'", f"{field} >= '1'", f"{field} < '2'", f"{field} > '0'",
                       f"{field} == '1'", f"{field} != '1'", f"{field} like '1%'", f"{field} like '%1'",
                       f"{field} like '%1%'", f"{field} LIKE '1%'", f"{field} LIKE '%1'",
                       f"{field} LIKE '%1%'",
                       f"{field} == '1' && {field} == '2'",
                       f"{field} == '1' and {field} == '2'",
                       f"{field} == '1' || {field} == '2'",
                       f"{field} == '1' or {field} == '2'",
                       f"{field} >= '1' || {field} <= '2'",
                       f"{field} >= '1' or {field} <= '2'",
                       f"{field} in ['1']", f"{field} in ['1', '2']",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    elif field in [ct.default_int8_array_field_name, ct.default_int16_array_field_name,
                   ct.default_int32_array_field_name, ct.default_int64_array_field_name]:
        expressions = [f"{field}[0] <= 1", f"{field}[0] >= 1",
                       f"{field}[0] < 2", f"{field}[0] > 0",
                       f"{field}[1] == 1", f"{field}[1] != 1",
                       f"{field}[0] == 9707199254740992", f"{field}[0] != 9707199254740992",
                       f"{field}[0] + 1 >= 2", f"{field}[0] - 1 <= 0",
                       f"{field}[0] + 1.0 >= 2", f"{field}[0] - 1.0 <= 0",
                       f"{field}[0] * 2 == 2", f"{field}[1] * 1.0 == 1.0",
                       f"{field}[1] / 1 == 1", f"{field}[0] / 1.0 == 1", f"{field}[1] % 10 == 1",
                       f"{field}[0] == 1 && {field}[1] == 2", f"{field}[0] == 1 and {field}[1] == 2",
                       f"{field}[0] >=1 && {field}[2] <= 3", f"{field}[0] >=1 and {field}[1] == 2",
                       f"{field}[0] >=1  || {field}[1] <=2", f"{field}[0] >=1 or {field}[1] <=2",
                       f"{field}[0] in [1]", f"json_contains({field}, 1)", f"JSON_CONTAINS({field}, 1)",
                       f"json_contains_all({field}, [1, 2])", f"JSON_CONTAINS_ALL({field}, [1, 2])",
                       f"json_contains_any({field}, [1, 2])", f"JSON_CONTAINS_ANY({field}, [1, 2])",
                       f"array_contains({field}, 2)", f"ARRAY_CONTAINS({field}, 2)",
                       f"array_contains_all({field}, [1, 2])", f"ARRAY_CONTAINS_ALL({field}, [1, 2])",
                       f"array_contains_any({field}, [1, 2])", f"ARRAY_CONTAINS_ANY({field}, [1, 2])",
                       f"array_length({field}) < 10", f"ARRAY_LENGTH({field}) < 10",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    elif field in [ct.default_float_array_field_name, ct.default_double_array_field_name]:
        expressions = [f"{field}[0] <= 1", f"{field}[0] >= 1",
                       f"{field}[0] < 2", f"{field}[0] > 0",
                       f"{field}[1] == 1", f"{field}[1] != 1",
                       f"{field}[0] == 9707199254740992", f"{field}[0] != 9707199254740992",
                       f"{field}[0] <= 1.0", f"{field}[0] >= 1.0",
                       f"{field}[0] < 2.0", f"{field}[0] > 0.0",
                       f"{field}[1] == 1.0", f"{field}[1] != 1.0",
                       f"{field}[0] == 9707199254740992.0",
                       f"{field}[0] - 1 <= 0", f"{field}[0] + 1.0 >= 2",
                       f"{field}[0] - 1.0 <= 0", f"{field}[0] * 2 == 2",
                       f"{field}[0] * 1.0 == 1.0", f"{field}[0] / 1 == 1",
                       f"{field}[0] / 1.0 == 1.0", f"{field}[0] == 1**2",
                       f"{field}[0] == 1 && {field}[1] == 2",
                       f"{field}[0] == 1 and {field}[1] == 2.0",
                       f"{field}[0] >=1 && {field}[2] == 3.0",
                       f"{field}[0] >=1 and {field}[2] == 3",
                       f"{field}[0] == 1 || {field}[1] == 2.0",
                       f"{field}[0] == 1 or {field}[1] == 2.0",
                       f"{field}[0] >= 1  || {field}[1] <=2.0",
                       f"{field}[0] >= 1.0 or {field}[1] <= 2.0",
                       f"{field}[0] in [1]", f"{field}[0] in [1.0]", f"json_contains({field}, 1.0)",
                       f"JSON_CONTAINS({field}, 1.0)", f"json_contains({field}, 1.0)", f"JSON_CONTAINS({field}, 1.0)",
                       f"json_contains_all({field}, [2.0, 4.0])", f"JSON_CONTAINS_ALL({field}, [2.0, 4.0])",
                       f"json_contains_any({field}, [2.0, 4.0])", f"JSON_CONTAINS_ANY({field}, [2.0, 4.0])",
                       f"array_contains({field}, 2.0)", f"ARRAY_CONTAINS({field}, 2.0)",
                       f"array_contains({field}, 2.0)", f"ARRAY_CONTAINS({field}, 2.0)",
                       f"array_contains_all({field}, [1.0, 2.0])", f"ARRAY_CONTAINS_ALL({field}, [1.0, 2.0])",
                       f"array_contains_any({field}, [1.0, 2.0])", f"ARRAY_CONTAINS_ANY({field}, [1.0, 2.0])",
                       f"array_length({field}) < 10", f"ARRAY_LENGTH({field}) < 10",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    elif field in [ct.default_bool_array_field_name]:
        expressions = [f"{field}[0] == True", f"{field}[0] == False",
                       f"{field}[0] != True", f"{field}[0] != False",
                       f"{field}[0] <= True", f"{field}[0] >= True",
                       f"{field}[1] <= False", f"{field}[1] >= False",
                       f"{field}[0] < True", f"{field}[1] > True",
                       f"{field}[0] < False", f"{field}[0] > False",
                       f"{field}[0] == True && {field}[1] == False",
                       f"{field}[0] == True and {field}[1] == False ",
                       f"{field}[0] == True || {field}[1] == False",
                       f"{field}[0] == True or {field}[1] == False",
                       f"{field}[0] in [True]", f"{field}[1] in [False]", f"{field}[0] in [True, False]",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    elif field in [ct.default_string_array_field_name]:
        expressions = [f"{field}[0] <= '1'", f"{field}[0] >= '1'",
                       f"{field}[0] < '2'", f"{field}[0] > '0'",
                       f"{field}[1] == '1'", f"{field}[1] != '1'",
                       f"{field}[1] like '1%'", f"{field}[1] like '%1'",
                       f"{field}[1] like '%1%'", f"{field}[1] LIKE '1%'",
                       f"{field}[1] LIKE '%1'", f"{field}[1] LIKE '%1%'",
                       f"{field}[1] == '1' && {field}[2] == '2'",
                       f"{field}[1] == '1' and {field}[2] == '2'",
                       f"{field}[0] == '1' || {field}[2] == '2'",
                       f"{field}[0] == '1' or {field}[2] == '2'",
                       f"{field}[1] >= '1' || {field}[2] <= '2'",
                       f"{field}[1] >= '1' or {field}[2] <= '2'",
                       f"{field}[0] in ['0']", f"{field}[1] in ['1', '2']",
                       f"{field} is null", f"{field} IS NULL", f"{field} is not null", f"{field} IS NOT NULL"
                       ]
    else:
        raise Exception("Invalid field name")

    return expressions


def concatenate_uneven_arrays(arr1, arr2):
    """
    concatenate the element in two arrays with different length
    """
    max_len = max(len(arr1), len(arr2))
    result = []
    op_list = ["and", "or", "&&", "||"]
    for i in range(max_len):
        a = arr1[i] if i < len(arr1) else ""
        b = arr2[i] if i < len(arr2) else ""
        if a == "" or b == "":
            result.append(a + b)
        else:
            random_op = op_list[random.randint(0, len(op_list)-1)]
            result.append( a + " " + random_op + " " + b)

    return result

def gen_multiple_field_expressions(field_name_list=[], random_field_number=0, expr_number=1):
    """
    Gen an expression including multiple fields
    parameters:
       field_name_list: the field names to be filtered. And the names should be in the following field name list if this
                        parameter is specified: (both repeated or non-repeated field name are supported)
                        all_fields = [ct.default_int8_field_name, ct.default_int16_field_name,
                                          ct.default_int32_field_name, ct.default_int64_field_name,
                                          ct.default_float_field_name, ct.default_double_field_name,
                                          ct.default_string_field_name, ct.default_bool_field_name,
                                          ct.default_int8_array_field_name, ct.default_int16_array_field_name,
                                          ct.default_int32_array_field_name,ct.default_int64_array_field_name,
                                          ct.default_bool_array_field_name, ct.default_float_array_field_name,
                                          ct.default_double_array_field_name, ct.default_string_array_field_name]
       random_field_number: the random field numbers to be filtered. The filtered fields will be randomly selected in
                            the above field name list (all_fields) if this parameter is specified.
                            And if random_field_number <= len(all_fields), the fields will be randomly selected without
                            repeat. If random_field_number > len(all_fields), there will be repeated fields
                            for (random_field_number - len(all_fields)) part.
       expr_number: the number of expressions for each field
    return:
       expressions_fields: all the expressions for multiple fields
       field_name_list: the field name list used for the filtered expressions
    """
    if not isinstance(field_name_list, list):
        raise Exception("parameter field_name_list should be a list of all the fields to be filtered")
    if random_field_number < 0:
        raise Exception(f"random_field_number should be greater than or equal with 0]")
    if not isinstance(expr_number, int):
        raise Exception("parameter parameter should be an interger")
    log.info(field_name_list)
    log.info(random_field_number)
    if len(field_name_list) != 0 and random_field_number != 0:
        raise Exception("Not support both field_name_list and random_field_number are specified")

    field_name_list_cp = field_name_list.copy()

    all_fields = [ct.default_int8_field_name, ct.default_int16_field_name,
                  ct.default_int32_field_name, ct.default_int64_field_name,
                  ct.default_float_field_name, ct.default_double_field_name,
                  ct.default_string_field_name, ct.default_bool_field_name,
                  ct.default_int8_array_field_name, ct.default_int16_array_field_name,
                  ct.default_int32_array_field_name,ct.default_int64_array_field_name,
                  ct.default_bool_array_field_name, ct.default_float_array_field_name,
                  ct.default_double_array_field_name, ct.default_string_array_field_name]

    if len(field_name_list) == 0 and random_field_number != 0:
        if random_field_number <= len(all_fields):
            random_array = random.sample(range(len(all_fields)), random_field_number)
        else:
            random_array = random.sample(range(len(all_fields)), len(all_fields))
            for _ in range(random_field_number - len(all_fields)):
                random_array.append(random.randint(0, len(all_fields)-1))
        for i in random_array:
            field_name_list_cp.append(all_fields[i])
    if len(field_name_list) == 0 and random_field_number == 0:
        field_name_list_cp = all_fields
    expressions_fields = gen_field_expressions_all_single_operator_each_field(field_name_list_cp[0])
    if len(field_name_list_cp) > 1:
        for field in field_name_list[1:]:
            expressions = gen_field_expressions_all_single_operator_each_field(field)
            expressions_fields = concatenate_uneven_arrays(expressions_fields, expressions)

    return expressions_fields, field_name_list_cp


def gen_array_field_expressions_and_templates():
    """
    Gen a list of filter in expression-format(as a string) and template-format(as a dict) for a field.
    The two formats equals to each other.
    """
    expressions = [
        ["int32_array[0] > 0", {"expr": "int32_array[0] > {value_0}", "expr_params": {"value_0": 0}}],
        ["0 <= int32_array[0] < 400 or 1000 > float_array[1] >= 500",
         {"expr": "{value_0} <= int32_array[0] < {value_1} or {value_2} > float_array[1] >= {value_3}",
          "expr_params": {"value_0": 0, "value_1": 400, "value_2": 1000, "value_3": 500}}],
        ["int32_array[1] not in [1, 2, 3]", {"expr": "int32_array[1] not in {value_0}", "expr_params": {"value_0": [1, 2, 3]}}],
        ["int32_array[1] in [1, 2, 3] and string_array[1] != '2'",
         {"expr": "int32_array[1] in {value_0} and string_array[1] != {value_2}",
          "expr_params": {"value_0": [1, 2, 3], "value_2": "2"}}],
        ["int32_array == [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]", {"expr": "int32_array == {value_0}",
                                                            "expr_params": {"value_0": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]}}],
        ["int32_array[1] + 1 == 3 && int32_array[0] - 1 != 1",
         {"expr": "int32_array[1] + {value_0} == {value_2} && int32_array[0] - {value_0} != {value_0}",
          "expr_params": {"value_0": 1, "value_2": 3}}],
        ["int32_array[1] % 100 == 0 && string_array[1] in ['1', '2']",
         {"expr": "int32_array[1] % {value_0} == {value_1} && string_array[1] in {value_2}",
          "expr_params": {"value_0": 100, "value_1": 0, "value_2": ["1", "2"]}}],
        ["int32_array[1] in [300/2, -10*30+800, (200-100)*2] or (float_array[1] <= -4**5/2 || 100 <= int32_array[1] < 200)",
         {"expr": "int32_array[1] in {value_0} or (float_array[1] <= {value_1} || {value_2} <= int32_array[1] < {value_3})",
          "expr_params": {"value_0": [int(300/2), -10*30+800, (200-100)*2], "value_1": -4**5/2, "value_2": 100, "value_3": 200}}]
    ]
    return expressions


def gen_field_compare_expressions(fields1=None, fields2=None):
    if fields1 is None:
        fields1 = ["int64_1"]
        fields2 = ["int64_2"]
    expressions = []
    for field1, field2 in zip(fields1, fields2):
        expression = [
            f"{field1} | {field2} == 1",
            f"{field1} + {field2} <= 10 || {field1} - {field2} == 2",
            f"{field1} * {field2} >= 8 && {field1} / {field2} < 2",
            f"{field1} ** {field2} != 4 and {field1} + {field2} > 5",
            f"{field1} not in {field2}",
            f"{field1} in {field2}",
        ]
        expressions.extend(expression)
    return expressions


def gen_normal_string_expressions(fields=None):
    if fields is None:
        fields = [ct.default_string_field_name]
    expressions = []
    for field in fields:
        expression = [
            f"\"0\"< {field} < \"3\"",
            f"{field} >= \"0\"",
            f"({field} > \"0\" && {field} < \"100\") or ({field} > \"200\" && {field} < \"300\")",
            f"\"0\" <= {field} <= \"100\"",
            f"{field} == \"0\"|| {field} == \"1\"|| {field} ==\"2\"",
            f"{field} != \"0\"",
            f"{field} not in [\"0\", \"1\", \"2\"]",
            f"{field} in [\"0\", \"1\", \"2\"]"
        ]
        expressions.extend(expression)
    return expressions


def gen_invalid_string_expressions():
    expressions = [
        "varchar in [0, \"1\"]",
        "varchar not in [\"0\", 1, 2]"
    ]
    return expressions


def gen_normal_expressions_and_templates_field(field):
    """
    Gen a list of filter in expression-format(as a string) and template-format(as a dict) for a field.
    The two formats equals to each other.
    """
    expressions_and_templates = [
        ["", {"expr": "", "expr_params": {}}],
        [f"{field} > 0", {"expr": f"{field} > {{value_0}}", "expr_params": {"value_0": 0}}],
        [f"({field} > 0 && {field} < 400) or ({field} > 500 && {field} < 1000)",
         {"expr": f"({field} > {{value_0}} && {field} < {{value_1}}) or ({field} > {{value_2}} && {field} < {{value_3}})",
          "expr_params": {"value_0": 0, "value_1": 400, "value_2": 500, "value_3": 1000}}],
        [f"{field} not in [1, 2, 3]", {"expr": f"{field} not in {{value_0}}", "expr_params": {"value_0": [1, 2, 3]}}],
        [f"{field} in [1, 2, 3] and {field} != 2", {"expr": f"{field} in {{value_0}} and {field} != {{value_1}}", "expr_params": {"value_0": [1, 2, 3], "value_1": 2}}],
        [f"{field} == 0 || {field} == 1 || {field} == 2", {"expr": f"{field} == {{value_0}} || {field} == {{value_1}} || {field} == {{value_2}}",
         "expr_params": {"value_0": 0, "value_1": 1, "value_2": 2}}],
        [f"0 < {field} < 400", {"expr": f"{{value_0}} < {field} < {{value_1}}", "expr_params": {"value_0": 0, "value_1": 400}}],
        [f"500 <= {field} <= 1000", {"expr": f"{{value_0}} <= {field} <= {{value_1}}", "expr_params": {"value_0": 500, "value_1": 1000}}],
        [f"200+300 <= {field} <= 500+500", {"expr": f"{{value_0}} <= {field} <= {{value_1}}", "expr_params": {"value_0": 200+300, "value_1": 500+500}}],
        [f"{field} in [300/2, 900%40, -10*30+800, 2048/2%200, (100+200)*2]", {"expr": f"{field} in {{value_0}}", "expr_params": {"value_0": [300*1.0/2, 900*1.0%40, -10*30*1.0+800, 2048*1.0/2%200, (100+200)*1.0*2]}}],
        [f"{field} in [+3**6, 2**10/2]", {"expr": f"{field} in {{value_0}}", "expr_params": {"value_0": [+3**6*1.0, 2**10*1.0/2]}}],
        [f"{field} <= 4**5/2 && {field} > 500-1 && {field} != 500/2+260", {"expr": f"{field} <= {{value_0}} && {field} > {{value_1}} && {field} != {{value_2}}",
         "expr_params": {"value_0": 4**5/2, "value_1": 500-1, "value_2": 500/2+260}}],
        [f"{field} > 400 && {field} < 200", {"expr": f"{field} > {{value_0}} && {field} < {{value_1}}", "expr_params": {"value_0": 400, "value_1": 200}}],
        [f"{field} < -2**8", {"expr": f"{field} < {{value_0}}", "expr_params": {"value_0": -2**8}}],
        [f"({field} + 1) == 3 || {field} * 2 == 64 || {field} == 10**2", {"expr": f"({field} + {{value_0}}) == {{value_1}} || {field} * {{value_2}} == {{value_3}} || {field} == {{value_4}}",
         "expr_params": {"value_0": 1, "value_1": 3, "value_2": 2, "value_3": 64, "value_4": 10**2}}]
    ]
    return expressions_and_templates


def get_expr_from_template(template={}):
    return template.get("expr", None)


def get_expr_params_from_template(template={}):
    return template.get("expr_params", None)


def gen_integer_overflow_expressions():
    expressions = [
        "int8 < - 128",
        "int8 > 127",
        "int8 > -129 && int8 < 128",
        "int16 < -32768",
        "int16 >= 32768",
        "int16 > -32769 && int16 <32768",
        "int32 < -2147483648",
        "int32 == 2147483648",
        "int32 < 2147483648 || int32 == -2147483648",
        "int8 in  [-129, 1] || int16 in [32769] || int32 in [2147483650, 0]"
    ]
    return expressions


def gen_modulo_expression(expr_fields):
    exprs = []
    for field in expr_fields:
        exprs.extend([
            (Expr.EQ(Expr.MOD(field, 10).subset, 1).value, field),
            (Expr.LT(Expr.MOD(field, 17).subset, 9).value, field),
            (Expr.LE(Expr.MOD(field, 100).subset, 50).value, field),
            (Expr.GT(Expr.MOD(field, 50).subset, 40).value, field),
            (Expr.GE(Expr.MOD(field, 29).subset, 15).value, field),
            (Expr.NE(Expr.MOD(field, 29).subset, 10).value, field),
        ])
    return exprs


def count_match_expr(values_l: list, rex_l: str, op: str, values_r: list, rex_r: str) -> list:
    if len(values_l) != len(values_r):
        raise ValueError(f"[count_match_expr] values not equal: {len(values_l)} != {len(values_r)}")

    res = []
    if op in ['and', '&&']:
        for i in range(len(values_l)):
            if re.search(rex_l, values_l[i]) and re.search(rex_r, values_r[i]):
                res.append(i)

    elif op in ['or', '||']:
        for i in range(len(values_l)):
            if re.search(rex_l, values_l[i]) or re.search(rex_r, values_r[i]):
                res.append(i)

    else:
        raise ValueError(f"[count_match_expr] Not support op: {op}")
    return res


def gen_varchar_expression(expr_fields):
    exprs = []
    for field in expr_fields:
        exprs.extend([
            (Expr.like(field, "a%").value, field, r'^a.*'),
            (Expr.LIKE(field, "%b").value, field, r'.*b$'),
            (Expr.AND(Expr.like(field, "%b").subset, Expr.LIKE(field, "z%").subset).value, field, r'^z.*b$'),
            (Expr.And(Expr.like(field, "i%").subset, Expr.LIKE(field, "%j").subset).value, field, r'^i.*j$'),
            (Expr.OR(Expr.like(field, "%h%").subset, Expr.LIKE(field, "%jo").subset).value, field, fr'(?:h.*|.*jo$)'),
            (Expr.Or(Expr.like(field, "ip%").subset, Expr.LIKE(field, "%yu%").subset).value, field, fr'(?:^ip.*|.*yu)'),
        ])
    return exprs


def gen_varchar_operation(expr_fields):
    exprs = []
    for field in expr_fields:
        exprs.extend([
            (Expr.EQ(field, '"a"').value, field, r'a'),
            (Expr.GT(field, '"a"').value, field, r'[^a]'),
            (Expr.GE(field, '"a"').value, field, r'.*'),
            (Expr.LT(field, '"z"').value, field, r'[^z]'),
            (Expr.LE(field, '"z"').value, field, r'.*')
        ])
    return exprs


def gen_varchar_unicode_expression(expr_fields):
    exprs = []
    for field in expr_fields:
        exprs.extend([
            (Expr.like(field, "国%").value, field, r'^国.*'),
            (Expr.LIKE(field, "%中").value, field, r'.*中$'),
            (Expr.AND(Expr.like(field, "%江").subset, Expr.LIKE(field, "麚%").subset).value, field, r'^麚.*江$'),
            (Expr.And(Expr.like(field, "鄷%").subset, Expr.LIKE(field, "%薞").subset).value, field, r'^鄷.*薞$'),
            (Expr.OR(Expr.like(field, "%核%").subset, Expr.LIKE(field, "%臥蜜").subset).value, field, fr'(?:核.*|.*臥蜜$)'),
            (Expr.Or(Expr.like(field, "咴矷%").subset, Expr.LIKE(field, "%濉蠬%").subset).value, field, fr'(?:^咴矷.*|.*濉蠬)'),
        ])
    return exprs


def gen_varchar_unicode_expression_array(expr_fields):
    exprs = []
    for field in expr_fields:
        exprs.extend([
            ExprCheckParams(field, Expr.ARRAY_CONTAINS(field, '"中"').value, 'set(["中"]).issubset({0})'),
            ExprCheckParams(field, Expr.array_contains(field, '"国"').value, 'set(["国"]).issubset({0})'),
            ExprCheckParams(field, Expr.ARRAY_CONTAINS_ALL(field, ["华"]).value, 'set(["华"]).issubset({0})'),
            ExprCheckParams(field, Expr.array_contains_all(field, ["中", "国"]).value, 'set(["中", "国"]).issubset({0})'),
            ExprCheckParams(field, Expr.ARRAY_CONTAINS_ANY(field, ["紅"]).value, 'not set(["紅"]).isdisjoint({0})'),
            ExprCheckParams(field, Expr.array_contains_any(field, ["紅", "父", "环", "稵"]).value,
                            'not set(["紅", "父", "环", "稵"]).isdisjoint({0})'),
            ExprCheckParams(field, Expr.AND(Expr.ARRAY_CONTAINS(field, '"噜"').value,
                                            Expr.ARRAY_CONTAINS_ANY(field, ["浮", "沮", "茫"]).value).value,
                            'set(["噜"]).issubset({0}) and not set(["浮", "沮", "茫"]).isdisjoint({0})'),
            ExprCheckParams(field, Expr.And(Expr.ARRAY_CONTAINS_ALL(field, ["爤"]).value,
                                            Expr.array_contains_any(field, ["暁", "非", "鸳", "丹"]).value).value,
                            'set(["爤"]).issubset({0}) and not set(["暁", "非", "鸳", "丹"]).isdisjoint({0})'),
            ExprCheckParams(field, Expr.OR(Expr.array_contains(field, '"草"').value,
                                           Expr.array_contains_all(field, ["昩", "苴"]).value).value,
                            'set(["草"]).issubset({0}) or set(["昩", "苴"]).issubset({0})'),
            ExprCheckParams(field, Expr.Or(Expr.ARRAY_CONTAINS_ANY(field, ["魡", "展", "隶", "韀", "脠", "噩"]).value,
                                           Expr.array_contains_any(field, ["备", "嘎", "蝐", "秦", "万"]).value).value,
                            'not set(["魡", "展", "隶", "韀", "脠", "噩"]).isdisjoint({0}) or ' +
                            'not set(["备", "嘎", "蝐", "秦", "万"]).isdisjoint({0})')
        ])
    return exprs


def gen_number_operation(expr_fields):
    exprs = []
    for field in expr_fields:
        exprs.extend([
            (Expr.LT(Expr.ADD(field, 23), 100).value, field),
            (Expr.LT(Expr.ADD(-23, field), 121).value, field),
            (Expr.LE(Expr.SUB(field, 123), 99).value, field),
            (Expr.GT(Expr.MUL(field, 2), 88).value, field),
            (Expr.GT(Expr.MUL(3, field), 137).value, field),
            (Expr.GE(Expr.DIV(field, 30), 20).value, field),
        ])
    return exprs


def l2(x, y):
    return np.linalg.norm(np.array(x) - np.array(y))


def ip(x, y):
    return np.inner(np.array(x), np.array(y))


def cosine(x, y):
    return np.dot(x, y)/(np.linalg.norm(x)*np.linalg.norm(y))


def jaccard(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum())


def hamming(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return np.bitwise_xor(x, y).sum()


def tanimoto(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    res = np.double(np.bitwise_and(x, y).sum()) / np.double(np.bitwise_or(x, y).sum())
    if res == 0:
        value = float("inf")
    else:
        value = -np.log2(res)
    return value


def tanimoto_calc(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return np.double((len(x) - np.bitwise_xor(x, y).sum())) / (len(y) + np.bitwise_xor(x, y).sum())


def substructure(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(y)


def superstructure(x, y):
    x = np.asarray(x, np.bool_)
    y = np.asarray(y, np.bool_)
    return 1 - np.double(np.bitwise_and(x, y).sum()) / np.count_nonzero(x)


def compare_distance_2d_vector(x, y, distance, metric, sqrt):
    for i in range(len(x)):
        for j in range(len(y)):
            if metric == "L2":
                distance_i = l2(x[i], y[j])
                if not sqrt:
                    distance_i = math.pow(distance_i, 2)
            elif metric == "IP":
                distance_i = ip(x[i], y[j])
            elif metric == "HAMMING":
                distance_i = hamming(x[i], y[j])
            elif metric == "TANIMOTO":
                distance_i = tanimoto_calc(x[i], y[j])
            elif metric == "JACCARD":
                distance_i = jaccard(x[i], y[j])
            else:
                raise Exception("metric type is invalid")
            assert abs(distance_i - distance[i][j]) < ct.epsilon

    return True


def compare_distance_vector_and_vector_list(x, y, metric, distance):
    """
    target: compare the distance between x and y[i] with the expected distance array
    method: compare the distance between x and y[i] with the expected distance array
    expected: return true if all distances are matched
    """
    if not isinstance(y, list):
        log.error("%s is not a list." % str(y))
        assert False
    for i in range(len(y)):
        if metric == "L2":
            distance_i = (l2(x, y[i]))**2
        elif metric == "IP":
            distance_i = ip(x, y[i])
        elif metric == "COSINE":
            distance_i = cosine(x, y[i])
        else:
            raise Exception("metric type is invalid")
        if abs(distance_i - distance[i]) > ct.epsilon:
            log.error(f"The distance between {x} and {y[i]} does not equal {distance[i]}, expected: {distance_i}")
            assert abs(distance_i - distance[i]) < ct.epsilon

    return True


def modify_file(file_path_list, is_modify=False, input_content=""):
    """
    file_path_list : file list -> list[<file_path>]
    is_modify : does the file need to be reset
    input_content ：the content that need to insert to the file
    """
    if not isinstance(file_path_list, list):
        log.error("[modify_file] file is not a list.")

    for file_path in file_path_list:
        folder_path, file_name = os.path.split(file_path)
        if not os.path.isdir(folder_path):
            os.makedirs(folder_path)

        if not os.path.isfile(file_path):
            log.error("[modify_file] file(%s) is not exist." % file_path)
        else:
            if is_modify is True:
                with open(file_path, "r+") as f:
                    f.seek(0)
                    f.truncate()
                    f.write(input_content)
                    f.close()


def index_to_dict(index):
    return {
        "collection_name": index.collection_name,
        "field_name": index.field_name,
        # "name": index.name,
        "params": index.params
    }


def get_index_params_params(index_type):
    """get default params of index params  by index type"""
    params = ct.default_all_indexes_params[ct.all_index_types.index(index_type)].copy()
    return params


def get_search_params_params(index_type):
    """get default params of search params by index type"""
    params = ct.default_all_search_params_params[ct.all_index_types.index(index_type)].copy()
    return params


def get_default_metric_for_vector_type(vector_type=DataType.FLOAT_VECTOR):
    """get default metric for vector type"""
    return ct.default_metric_for_vector_type[vector_type]


def assert_json_contains(expr, list_data):
    opposite = False
    if expr.startswith("not"):
        opposite = True
        expr = expr.split("not ", 1)[1]
    result_ids = []
    expr_prefix = expr.split('(', 1)[0]
    exp_ids = eval(expr.split(', ', 1)[1].split(')', 1)[0])
    if expr_prefix in ["json_contains", "JSON_CONTAINS", "array_contains", "ARRAY_CONTAINS"]:
        for i in range(len(list_data)):
            if exp_ids in list_data[i]:
                result_ids.append(i)
    elif expr_prefix in ["json_contains_all", "JSON_CONTAINS_ALL", "array_contains_all", "ARRAY_CONTAINS_ALL"]:
        for i in range(len(list_data)):
            set_list_data = set(tuple(element) if isinstance(element, list) else element for element in list_data[i])
            if set(exp_ids).issubset(set_list_data):
                result_ids.append(i)
    elif expr_prefix in ["json_contains_any", "JSON_CONTAINS_ANY", "array_contains_any", "ARRAY_CONTAINS_ANY"]:
        for i in range(len(list_data)):
            set_list_data = set(tuple(element) if isinstance(element, list) else element for element in list_data[i])
            if set(exp_ids) & set_list_data:
                result_ids.append(i)
    else:
        log.warning("unknown expr: %s" % expr)
    if opposite:
        result_ids = [i for i in range(len(list_data)) if i not in result_ids]
    return result_ids


def assert_equal_index(index_1, index_2):
    return index_to_dict(index_1) == index_to_dict(index_2)


def gen_partitions(collection_w, partition_num=1):
    """
    target: create extra partitions except for _default
    method: create more than one partitions
    expected: return collection and raw data
    """
    log.info("gen_partitions: creating partitions")
    for i in range(partition_num):
        partition_name = "search_partition_" + str(i)
        collection_w.create_partition(partition_name=partition_name,
                                      description="search partition")
    par = collection_w.partitions
    assert len(par) == (partition_num + 1)
    log.info("gen_partitions: created partitions %s" % par)


def insert_data(collection_w, nb=ct.default_nb, is_binary=False, is_all_data_type=False,
                auto_id=False, dim=ct.default_dim, insert_offset=0, enable_dynamic_field=False, with_json=True,
                random_primary_key=False, multiple_dim_array=[], primary_field=ct.default_int64_field_name,
                vector_data_type=DataType.FLOAT_VECTOR, nullable_fields={}, language=None):
    """
    target: insert non-binary/binary data
    method: insert non-binary/binary data into partitions if any
    expected: return collection and raw data
    """
    par = collection_w.partitions
    num = len(par)
    vectors = []
    binary_raw_vectors = []
    insert_ids = []
    start = insert_offset
    log.info(f"inserting {nb} data into collection {collection_w.name}")
    # extract the vector field name list
    vector_name_list = extract_vector_field_name_list(collection_w)
    # prepare data
    for i in range(num):
        log.debug("Dynamic field is enabled: %s" % enable_dynamic_field)
        if not is_binary:
            if not is_all_data_type:
                if not enable_dynamic_field:
                    if vector_data_type == DataType.FLOAT_VECTOR:
                        default_data = gen_default_dataframe_data(nb // num, dim=dim, start=start, with_json=with_json,
                                                                  random_primary_key=random_primary_key,
                                                                  multiple_dim_array=multiple_dim_array,
                                                                  multiple_vector_field_name=vector_name_list,
                                                                  vector_data_type=vector_data_type,
                                                                  auto_id=auto_id, primary_field=primary_field,
                                                                  nullable_fields=nullable_fields, language=language)
                    elif vector_data_type in ct.append_vector_type:
                        default_data = gen_default_list_data(nb // num, dim=dim, start=start, with_json=with_json,
                                                             random_primary_key=random_primary_key,
                                                             multiple_dim_array=multiple_dim_array,
                                                             multiple_vector_field_name=vector_name_list,
                                                             vector_data_type=vector_data_type,
                                                             auto_id=auto_id, primary_field=primary_field,
                                                             nullable_fields=nullable_fields, language=language)

                else:
                    default_data = gen_default_rows_data(nb // num, dim=dim, start=start, with_json=with_json,
                                                         multiple_dim_array=multiple_dim_array,
                                                         multiple_vector_field_name=vector_name_list,
                                                         vector_data_type=vector_data_type,
                                                         auto_id=auto_id, primary_field=primary_field,
                                                         nullable_fields=nullable_fields, language=language)

            else:
                if not enable_dynamic_field:
                    if vector_data_type == DataType.FLOAT_VECTOR:
                        default_data = gen_general_list_all_data_type(nb // num, dim=dim, start=start, with_json=with_json,
                                                                      random_primary_key=random_primary_key,
                                                                      multiple_dim_array=multiple_dim_array,
                                                                      multiple_vector_field_name=vector_name_list,
                                                                      auto_id=auto_id, primary_field=primary_field,
                                                                      nullable_fields=nullable_fields, language=language)
                    elif vector_data_type == DataType.FLOAT16_VECTOR or vector_data_type == DataType.BFLOAT16_VECTOR:
                        default_data = gen_general_list_all_data_type(nb // num, dim=dim, start=start, with_json=with_json,
                                                                      random_primary_key=random_primary_key,
                                                                      multiple_dim_array=multiple_dim_array,
                                                                      multiple_vector_field_name=vector_name_list,
                                                                      auto_id=auto_id, primary_field=primary_field,
                                                                      nullable_fields=nullable_fields, language=language)
                else:
                    if os.path.exists(ct.rows_all_data_type_file_path + f'_{i}' + f'_dim{dim}.txt'):
                        with open(ct.rows_all_data_type_file_path + f'_{i}' + f'_dim{dim}.txt', 'rb') as f:
                            default_data = pickle.load(f)
                    else:
                        default_data = gen_default_rows_data_all_data_type(nb // num, dim=dim, start=start,
                                                                           with_json=with_json,
                                                                           multiple_dim_array=multiple_dim_array,
                                                                           multiple_vector_field_name=vector_name_list,
                                                                           partition_id=i, auto_id=auto_id,
                                                                           primary_field=primary_field,
                                                                           language=language)
        else:
            default_data, binary_raw_data = gen_default_binary_dataframe_data(nb // num, dim=dim, start=start,
                                                                              auto_id=auto_id,
                                                                              primary_field=primary_field,
                                                                              nullable_fields=nullable_fields,
                                                                              language=language)
            binary_raw_vectors.extend(binary_raw_data)
        insert_res = collection_w.insert(default_data, par[i].name)[0]
        log.info(f"inserted {nb // num} data into collection {collection_w.name}")
        time_stamp = insert_res.timestamp
        insert_ids.extend(insert_res.primary_keys)
        vectors.append(default_data)
        start += nb // num
    return collection_w, vectors, binary_raw_vectors, insert_ids, time_stamp


def _check_primary_keys(primary_keys, nb):
    if primary_keys is None:
        raise Exception("The primary_keys is None")
    assert len(primary_keys) == nb
    for i in range(nb - 1):
        if primary_keys[i] >= primary_keys[i + 1]:
            return False
    return True


def get_segment_distribution(res):
    """
    Get segment distribution
    """
    from collections import defaultdict
    segment_distribution = defaultdict(lambda: {"sealed": []})
    for r in res:
        for node_id in r.nodeIds:
            if r.state == 3:
                segment_distribution[node_id]["sealed"].append(r.segmentID)

    return segment_distribution


def percent_to_int(string):
    """
    transform percent(0%--100%) to int
    """

    new_int = -1
    if not isinstance(string, str):
        log.error("%s is not a string" % string)
        return new_int
    if "%" not in string:
        log.error("%s is not a percent" % string)
    else:
        new_int = int(string.strip("%"))

    return new_int


def gen_grant_list(collection_name):
    grant_list = [{"object": "Collection", "object_name": collection_name, "privilege": "Load"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Release"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Compaction"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Delete"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "GetStatistics"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "CreateIndex"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "IndexDetail"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "DropIndex"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Search"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Flush"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Query"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "LoadBalance"},
                  {"object": "Collection", "object_name": collection_name, "privilege": "Import"},
                  {"object": "Global", "object_name": "*", "privilege": "All"},
                  {"object": "Global", "object_name": "*", "privilege": "CreateCollection"},
                  {"object": "Global", "object_name": "*", "privilege": "DropCollection"},
                  {"object": "Global", "object_name": "*", "privilege": "DescribeCollection"},
                  {"object": "Global", "object_name": "*", "privilege": "ShowCollections"},
                  {"object": "Global", "object_name": "*", "privilege": "CreateOwnership"},
                  {"object": "Global", "object_name": "*", "privilege": "DropOwnership"},
                  {"object": "Global", "object_name": "*", "privilege": "SelectOwnership"},
                  {"object": "Global", "object_name": "*", "privilege": "ManageOwnership"},
                  {"object": "User", "object_name": "*", "privilege": "UpdateUser"},
                  {"object": "User", "object_name": "*", "privilege": "SelectUser"}]
    return grant_list


def install_milvus_operator_specific_config(namespace, milvus_mode, release_name, image,
                                            rate_limit_enable, collection_rate_limit):
    """
    namespace : str
    milvus_mode : str -> standalone or cluster
    release_name : str
    image: str -> image tag including repository
    rate_limit_enable: str -> true or false, switch for rate limit
    collection_rate_limit: int -> collection rate limit numbers
    input_content ：the content that need to insert to the file
    return: milvus host name
    """

    if not isinstance(namespace, str):
        log.error("[namespace] is not a string.")

    if not isinstance(milvus_mode, str):
        log.error("[milvus_mode] is not a string.")

    if not isinstance(release_name, str):
        log.error("[release_name] is not a string.")

    if not isinstance(image, str):
        log.error("[image] is not a string.")

    if not isinstance(rate_limit_enable, str):
        log.error("[rate_limit_enable] is not a string.")

    if not isinstance(collection_rate_limit, int):
        log.error("[collection_rate_limit] is not an integer.")

    if milvus_mode not in ["standalone", "cluster"]:
        log.error("[milvus_mode] is not 'standalone' or 'cluster'")

    if rate_limit_enable not in ["true", "false"]:
        log.error("[rate_limit_enable] is not 'true' or 'false'")

    data_config = {
        'metadata.namespace': namespace,
        'spec.mode': milvus_mode,
        'metadata.name': release_name,
        'spec.components.image': image,
        'spec.components.proxy.serviceType': 'LoadBalancer',
        'spec.components.dataNode.replicas': 2,
        'spec.config.common.retentionDuration': 60,
        'spec.config.quotaAndLimits.enable': rate_limit_enable,
        'spec.config.quotaAndLimits.ddl.collectionRate': collection_rate_limit,
    }
    mil = MilvusOperator()
    mil.install(data_config)
    if mil.wait_for_healthy(release_name, namespace, timeout=1800):
        host = mil.endpoint(release_name, namespace).split(':')[0]
    else:
        raise MilvusException(message=f'Milvus healthy timeout 1800s')

    return host


def get_wildcard_output_field_names(collection_w, output_fields):
    """
    Processes output fields with wildcard ('*') expansion for collection queries.
    
    Args:
        collection_w (Union[dict, CollectionWrapper]): Collection information, 
        either as a dict (v2 client) or ORM wrapper.
        output_fields (List[str]): List of requested output fields, may contain '*' wildcard.
    
    Returns:
        List[str]: Expanded list of output fields with wildcard replaced by all available field names.
    """
    if not isinstance(collection_w, dict):
        # in orm, it accepts a collection wrapper
        field_names = [field.name for field in collection_w.schema.fields]
    else:
        # in client v2, it accepts a dict of collection info
        fields = collection_w.get('fields', None)
        field_names = [field.get('name') for field in fields]

    output_fields = output_fields.copy()
    if "*" in output_fields:
        output_fields.remove("*")
        output_fields.extend(field_names)

    return output_fields


def extract_vector_field_name_list(collection_w):
    """
    extract the vector field name list
    collection_w : the collection object to be extracted thea name of all the vector fields
    return: the vector field name list without the default float vector field name
    """
    schema_dict = collection_w.schema.to_dict()
    fields = schema_dict.get('fields')
    vector_name_list = []
    for field in fields:
        if field['type'] == DataType.FLOAT_VECTOR \
                or field['type'] == DataType.FLOAT16_VECTOR \
                or field['type'] == DataType.BFLOAT16_VECTOR \
                or field['type'] == DataType.SPARSE_FLOAT_VECTOR\
                or field['type'] == DataType.INT8_VECTOR:
            if field['name'] != ct.default_float_vec_field_name:
                vector_name_list.append(field['name'])

    return vector_name_list


def get_field_dtype_by_field_name(schema, field_name):
    """
    get the vector field data type by field name
    collection_w : the collection object to be extracted
    return: the field data type of the field name
    """
    # Convert ORM schema to dict schema for unified processing
    if not isinstance(schema, dict):
        schema = convert_orm_schema_to_dict_schema(schema)

    fields = schema.get('fields')
    for field in fields:
        if field['name'] == field_name:
            return field['type']
    return None


def get_activate_func_from_metric_type(metric_type):
    activate_function = lambda x: x
    if metric_type == "COSINE":
        activate_function = lambda x: (1 + x) * 0.5
    elif metric_type == "IP":
        activate_function = lambda x: 0.5 + math.atan(x)/ math.pi
    elif metric_type == "BM25":
        activate_function = lambda x: 2 * math.atan(x) / math.pi
    else:
        activate_function  = lambda x: 1.0 - 2*math.atan(x) / math.pi
    return activate_function


def get_hybrid_search_base_results_rrf(search_res_dict_array, round_decimal=-1):
    """
    merge the element in the dicts array
    search_res_dict_array : the dict array in which the elements to be merged
    return: the sorted id and score answer
    """
    # calculate hybrid search base line

    search_res_dict_merge = {}
    ids_answer = []
    score_answer = []

    for i, result in enumerate(search_res_dict_array, 0):
        for key, distance in result.items():
            search_res_dict_merge[key] = search_res_dict_merge.get(key, 0) + distance

    if round_decimal != -1 :
        for k, v in search_res_dict_merge.items():
            multiplier = math.pow(10.0, round_decimal)
            v = math.floor(v*multiplier+0.5) / multiplier
            search_res_dict_merge[k] = v

    sorted_list = sorted(search_res_dict_merge.items(), key=lambda x: x[1], reverse=True)

    for sort in sorted_list:
        ids_answer.append(int(sort[0]))
        score_answer.append(float(sort[1]))

    return ids_answer, score_answer


def get_hybrid_search_base_results(search_res_dict_array, weights, metric_types, round_decimal=-1):
    """
    merge the element in the dicts array
    search_res_dict_array : the dict array in which the elements to be merged
    return: the sorted id and score answer
    """
    # calculate hybrid search base line

    search_res_dict_merge = {}
    ids_answer = []
    score_answer = []

    for i, result in enumerate(search_res_dict_array, 0):
        activate_function = get_activate_func_from_metric_type(metric_types[i])
        for key, distance in result.items():
            activate_distance = activate_function(distance)
            weight = weights[i]
            search_res_dict_merge[key] = search_res_dict_merge.get(key, 0) + activate_function(distance) * weights[i]

    if round_decimal != -1 :
        for k, v in search_res_dict_merge.items():
            multiplier = math.pow(10.0, round_decimal)
            v = math.floor(v*multiplier+0.5) / multiplier
            search_res_dict_merge[k] = v

    sorted_list = sorted(search_res_dict_merge.items(), key=lambda x: x[1], reverse=True)

    for sort in sorted_list:
        ids_answer.append(int(sort[0]))
        score_answer.append(float(sort[1]))

    return ids_answer, score_answer


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
        bf16_vector = np.array(raw_vector, dtype=bfloat16)
        bf16_vectors.append(bf16_vector)

    return raw_vectors, bf16_vectors


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
        fp16_vector = np.array(raw_vector, dtype=np.float16)
        fp16_vectors.append(fp16_vector)

    return raw_vectors, fp16_vectors


def gen_sparse_vectors(nb, dim=1000, sparse_format="dok", empty_percentage=0):
    # default sparse format is dok, dict of keys
    # another option is coo, coordinate List

    rng = np.random.default_rng()
    vectors = [{
        d: rng.random() for d in list(set(random.sample(range(dim), random.randint(20, 30)) + [0, 1]))
    } for _ in range(nb)]
    if empty_percentage > 0:
        empty_nb = int(nb * empty_percentage / 100)
        empty_ids = random.sample(range(nb), empty_nb)
        for i in empty_ids:
            vectors[i] = {}
    if sparse_format == "coo":
        vectors = [
            {"indices": list(x.keys()), "values": list(x.values())} for x in vectors
        ]
    return vectors


def gen_vectors(nb, dim, vector_data_type=DataType.FLOAT_VECTOR):
    vectors = []
    if vector_data_type == DataType.FLOAT_VECTOR:
        vectors = [[random.uniform(-1, 1) for _ in range(dim)] for _ in range(nb)]
    elif vector_data_type == DataType.FLOAT16_VECTOR:
        vectors = gen_fp16_vectors(nb, dim)[1]
    elif vector_data_type == DataType.BFLOAT16_VECTOR:
        vectors = gen_bf16_vectors(nb, dim)[1]
    elif vector_data_type == DataType.SPARSE_FLOAT_VECTOR:
        vectors = gen_sparse_vectors(nb, dim)
    elif vector_data_type == ct.text_sparse_vector:
        vectors = gen_text_vectors(nb)    # for Full Text Search
    elif vector_data_type == DataType.BINARY_VECTOR:
        vectors = gen_binary_vectors(nb, dim)[1]
    elif vector_data_type == DataType.INT8_VECTOR:
        vectors = gen_int8_vectors(nb, dim)[1]
    else:
        log.error(f"Invalid vector data type: {vector_data_type}")
        raise Exception(f"Invalid vector data type: {vector_data_type}")
    if dim > 1:
        if vector_data_type == DataType.FLOAT_VECTOR:
            vectors = preprocessing.normalize(vectors, axis=1, norm='l2')
            vectors = vectors.tolist()
    return vectors


def gen_int8_vectors(num, dim):
    raw_vectors = []
    int8_vectors = []
    for _ in range(num):
        raw_vector = [random.randint(-128, 127) for _ in range(dim)]
        raw_vectors.append(raw_vector)
        int8_vector = np.array(raw_vector, dtype=np.int8)
        int8_vectors.append(int8_vector)
    return raw_vectors, int8_vectors


def gen_text_vectors(nb, language="en"):

    fake = Faker("en_US")
    if language == "zh":
        fake = Faker("zh_CN")
    vectors = [" milvus " + fake.text() for _ in range(nb)]
    return vectors


def field_types() -> dict:
    return dict(sorted(dict(DataType.__members__).items(), key=lambda item: item[0], reverse=True))


def get_array_element_type(data_type: str):
    if hasattr(DataType, "ARRAY") and data_type.startswith(DataType.ARRAY.name):
        element_type = data_type.lstrip(DataType.ARRAY.name).lstrip("_")
        for _field in field_types().keys():
            if str(element_type).upper().startswith(_field):
                return _field, getattr(DataType, _field)
        raise ValueError(f"[get_array_data_type] Can't find element type:{element_type} for array:{data_type}")
    raise ValueError(f"[get_array_data_type] Data type is not start with array: {data_type}")


def set_field_schema(field: str, params: dict):
    for k, v in field_types().items():
        if str(field).upper().startswith(k):
            _kwargs = {}

            _field_element, _data_type = k, DataType.NONE
            if hasattr(DataType, "ARRAY") and _field_element == DataType.ARRAY.name:
                _field_element, _data_type = get_array_element_type(field)
                _kwargs.update({"max_capacity": ct.default_max_capacity, "element_type": _data_type})

            if _field_element in [DataType.STRING.name, DataType.VARCHAR.name]:
                _kwargs.update({"max_length": ct.default_length})

            elif _field_element in [DataType.BINARY_VECTOR.name, DataType.FLOAT_VECTOR.name,
                                    DataType.FLOAT16_VECTOR.name, DataType.BFLOAT16_VECTOR.name]:
                _kwargs.update({"dim": ct.default_dim})

            if isinstance(params, dict):
                _kwargs.update(params)
            else:
                raise ValueError(
                    f"[set_field_schema] Field `{field}` params is not a dict, type: {type(params)}, params: {params}")
            return ApiFieldSchemaWrapper().init_field_schema(name=field, dtype=v, **_kwargs)[0]
    raise ValueError(f"[set_field_schema] Can't set field:`{field}` schema: {params}")


def set_collection_schema(fields: list, field_params: dict = {}, **kwargs):
    """
    :param fields: List[str]
    :param field_params: {<field name>: dict<field params>}
            int64_1:
                is_primary: bool
                description: str
            varchar_1:
                is_primary: bool
                description: str
                max_length: int = 65535
            varchar_2:
                max_length: int = 100
                is_partition_key: bool
            array_int8_1:
                max_capacity: int = 100
            array_varchar_1:
                max_capacity: int = 100
                max_length: int = 65535
            float_vector:
                dim: int = 128
    :param kwargs: <params for collection schema>
            description: str
            primary_field: str
            auto_id: bool
            enable_dynamic_field: bool
            num_partitions: int
    """
    field_schemas = [set_field_schema(field=field, params=field_params.get(field, {})) for field in fields]
    return ApiCollectionSchemaWrapper().init_collection_schema(fields=field_schemas, **kwargs)[0]


def check_key_exist(source: dict, target: dict):
    global flag
    flag = True

    def check_keys(_source, _target):
        global flag
        for key, value in _source.items():
            if key in _target and isinstance(value, dict):
                check_keys(_source[key], _target[key])
            elif key not in _target:
                log.error("[check_key_exist] Key: '{0}' not in target: {1}".format(key, _target))
                flag = False

    check_keys(source, target)
    return flag


def gen_unicode_string():
    return chr(random.randint(0x4e00, 0x9fbf))


def gen_unicode_string_batch(nb, string_len: int = 1):
    return [''.join([gen_unicode_string() for _ in range(string_len)]) for _ in range(nb)]


def gen_unicode_string_array_batch(nb, string_len: int = 1, max_capacity: int = ct.default_max_capacity):
    return [[''.join([gen_unicode_string() for _ in range(min(random.randint(1, string_len), 50))]) for _ in
             range(random.randint(0, max_capacity))] for _ in range(nb)]


def iter_insert_list_data(data: list, batch: int, total_len: int):
    nb_list = [batch for _ in range(int(total_len / batch))]
    if total_len % batch > 0:
        nb_list.append(total_len % batch)

    data_obj = [iter(d) for d in data]
    for n in nb_list:
        yield [[next(o) for _ in range(n)] for o in data_obj]


def gen_collection_name_by_testcase_name(module_index=1):
    """
    Gen a unique collection name by testcase name
    if calling from the test base class, module_index=2
    if calling from the testcase, module_index=1
    """
    return inspect.stack()[module_index][3] + gen_unique_str("_")


def parse_fmod(x: int, y: int) -> int:
    """
    Computes the floating-point remainder of x/y with the same sign as x.
    
    This function mimics the behavior of the C fmod() function for integer inputs,
    where the result has the same sign as the dividend (x).
    
    Args:
        x (int): The dividend
        y (int): The divisor
        
    Returns:
        int: The remainder of x/y with the same sign as x
        
    Raises:
        ValueError: If y is 0 (division by zero)
        
    Examples:
        parse_fmod(5, 3) -> 2
        parse_fmod(-5, 3) -> -2
        parse_fmod(5, -3) -> 2
        parse_fmod(-5, -3) -> -2
    """
    if y == 0:
        raise ValueError(f'[parse_fmod] Math domain error, `y` can not bt `0`')

    v = abs(x) % abs(y)

    return v if x >= 0 else -v

def convert_timestamptz(rows, timestamptz_field_name, timezone="UTC"):
    """
    Convert timestamptz strings in ``rows`` into the specified IANA timezone.

    Behaviour matches PostgreSQL:
    - Inputs that already include an offset (e.g. ``Z`` or ``+08:00``) are
      converted to the target timezone.
    - Naive inputs (no offset) are treated as already expressed in the target
      timezone; we simply append the correct offset for that zone.

    Examples:
        "2024-12-31 22:00:00"       -> "2024-12-31T22:00:00+08:00"
        "2024-12-31 22:00:00Z"      -> "2025-01-01T06:00:00+08:00"
        "2024-12-31T22:00:00"       -> "2024-12-31T22:00:00+08:00"
        "2024-12-31T22:00:00+08:00" -> "2024-12-31T22:00:00+08:00"
        "2024-12-31T22:00:00-08:00" -> "2025-01-01T14:00:00+08:00"
    """

    basic_re = re.compile(
        r"^(?P<y>-?\d{1,4})-(?P<m>\d{2})-(?P<d>\d{2})[T ]"
        r"(?P<h>\d{2}):(?P<mi>\d{2}):(?P<s>\d{2})(?P<offset>Z|[+-]\d{2}:\d{2})?$"
    )

    def _days_in_month(year: int, month: int) -> int:
        if month in (1, 3, 5, 7, 8, 10, 12):
            return 31
        if month in (4, 6, 9, 11):
            return 30
        is_leap = (year % 4 == 0 and (year % 100 != 0 or year % 400 == 0))
        return 29 if is_leap else 28

    def _apply_offset_to_utc(
        year: int, month: int, day: int, hour: int, minute: int, second: int, offset: Tuple[str, int, int]
    ) -> Tuple[int, int, int, int, int, int]:
        sign, oh, om = offset
        delta_minutes = oh * 60 + om
        if sign == '+':
            delta_minutes = -delta_minutes
        else:
            delta_minutes = +delta_minutes
        total_minutes = hour * 60 + minute + delta_minutes
        carry_days = 0
        if total_minutes < 0:
            carry_days = (total_minutes - 59) // (60 * 24)
            total_minutes -= carry_days * 60 * 24
        else:
            carry_days = total_minutes // (60 * 24)
            total_minutes = total_minutes % (60 * 24)
        new_hour = total_minutes // 60
        new_minute = total_minutes % 60
        day += carry_days
        while True:
            if day <= 0:
                month -= 1
                if month == 0:
                    month = 12
                    year -= 1
                day += _days_in_month(year, month)
            else:
                dim = _days_in_month(year, month)
                if day > dim:
                    day -= dim
                    month += 1
                    if month == 13:
                        month = 1
                        year += 1
                else:
                    break
        return year, month, day, new_hour, new_minute, second

    def _format_fixed(y: int, m: int, d: int, hh: int, mi: int, ss: int, offset_minutes: int) -> str:
        if offset_minutes == 0:
            return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mi:02d}:{ss:02d}Z"
        sign = '+' if offset_minutes >= 0 else '-'
        total = abs(offset_minutes)
        oh, om = divmod(total, 60)
        return f"{y:04d}-{m:02d}-{d:02d}T{hh:02d}:{mi:02d}:{ss:02d}{sign}{oh:02d}:{om:02d}"

    def _format_dt(dt: datetime) -> str:
        s = dt.isoformat(timespec="seconds")
        return s[:-6] + "Z" if s.endswith("+00:00") else s

    def _localize_naive(dt: datetime, tz_name: str) -> Optional[datetime]:
        """Best-effort localization that handles DST gaps/ambiguities."""
        # Prefer pytz because it surfaces NonExistent/Ambiguous errors we can resolve.
        try:
            tz = pytz.timezone(tz_name)
            try:
                return tz.localize(dt, is_dst=None)
            except pytz.NonExistentTimeError:
                # For forward DST jump (gap), shift back by the jump to previous valid time.
                before = tz.localize(dt - timedelta(hours=1), is_dst=None)
                after = tz.localize(dt + timedelta(hours=1), is_dst=None)
                gap = after.utcoffset() - before.utcoffset()
                adjust = gap if gap.total_seconds() != 0 else timedelta(hours=1)
                return tz.localize(dt - adjust, is_dst=None)
            except pytz.AmbiguousTimeError:
                # Choose DST side (fold=1) to align with PostgreSQL semantics.
                return tz.localize(dt, is_dst=True)
        except Exception:
            pass

        # Fallback with zoneinfo: detect offset jump around the time.
        try:
            tzinfo = ZoneInfo(tz_name)
            before = (dt - timedelta(minutes=30)).replace(tzinfo=tzinfo)
            after = (dt + timedelta(minutes=30)).replace(tzinfo=tzinfo)
            off_before = before.utcoffset()
            off_after = after.utcoffset()
            if off_before and off_after and off_before != off_after:
                gap = off_after - off_before
                adjust = gap if gap.total_seconds() != 0 else timedelta(hours=1)
                return (dt - adjust).replace(tzinfo=tzinfo)
            return dt.replace(tzinfo=tzinfo)
        except Exception:
            return None

    def _target_offset_minutes() -> int:
        try:
            probe = datetime(2004, 1, 1, 0, 0, 0, tzinfo=ZoneInfo(timezone))
            off = probe.utcoffset()
            if off is not None:
                return int(off.total_seconds() // 60)
        except Exception:
            pass
        return 480 if timezone == "Asia/Shanghai" else 0

    def _manual_path(raw: str) -> str:
        norm = raw.replace(" ", "T", 1)
        m = basic_re.match(norm)
        if not m:
            raise ValueError(f"Invalid timestamp string: {raw}")
        y, mo, d, hh, mi, ss = map(int, (m.group("y"), m.group("m"), m.group("d"), m.group("h"), m.group("mi"), m.group("s")))
        offset_str = m.group("offset")
        target_minutes = _target_offset_minutes()

        if not offset_str:
            return _format_fixed(y, mo, d, hh, mi, ss, target_minutes)

        if offset_str == "Z":
            uy, um, ud, uh, umi, uss = y, mo, d, hh, mi, ss
        else:
            sign, oh, om = offset_str[0], int(offset_str[1:3]), int(offset_str[4:6])
            uy, um, ud, uh, umi, uss = _apply_offset_to_utc(y, mo, d, hh, mi, ss, (sign, oh, om))

        if target_minutes == 0:
            return _format_fixed(uy, um, ud, uh, umi, uss, 0)

        reverse_sign = '-' if target_minutes >= 0 else '+'
        ty, tm, td, th, tmi, ts = _apply_offset_to_utc(
            uy, um, ud, uh, umi, uss, (reverse_sign, abs(target_minutes) // 60, abs(target_minutes) % 60)
        )
        return _format_fixed(ty, tm, td, th, tmi, ts, target_minutes)

    def convert_one(ts: str) -> str:
        raw = ts.strip()
        try:
            dt = parser.isoparse(raw.replace(" ", "T", 1))
            target_tz = ZoneInfo(timezone)
            if dt.tzinfo is None:
                localized = _localize_naive(dt, timezone)
                dt = localized if localized else dt.replace(tzinfo=target_tz)
            else:
                dt = dt.astimezone(target_tz)
            return _format_dt(dt)
        except Exception:
            return _manual_path(raw)

    new_rows = []
    for row in rows:
        if isinstance(row, dict) and timestamptz_field_name in row and isinstance(row[timestamptz_field_name], str):
            row = row.copy()
            row[timestamptz_field_name] = convert_one(row[timestamptz_field_name])
        new_rows.append(row)
    return new_rows