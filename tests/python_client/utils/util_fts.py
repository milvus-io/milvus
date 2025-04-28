import random
import time
import logging
from typing import List, Dict, Optional, Tuple
import pandas as pd
from faker import Faker
from pymilvus import (
    FieldSchema,
    CollectionSchema,
    DataType,
    Function,
    FunctionType,
    Collection,
    connections,
)
from pymilvus import MilvusClient

logger = logging.getLogger(__name__)


class FTSMultiAnalyzerChecker:
    """
    Full-text search utility class providing various utility methods for full-text search testing.
    Includes schema construction, test data generation, index creation, and more.
    """

    # Constant definitions
    DEFAULT_TEXT_MAX_LENGTH = 8192
    DEFAULT_LANG_MAX_LENGTH = 16
    DEFAULT_DOC_ID_START = 100

    # Faker multilingual instances as class attributes to avoid repeated creation
    fake_en = Faker("en_US")
    fake_zh = Faker("zh_CN")
    fake_fr = Faker("fr_FR")
    fake_jp = Faker("ja_JP")

    def __init__(
        self,
        collection_name: str,
        language_field_name: str,
        text_field_name: str,
        multi_analyzer_params: Optional[Dict] = None,
        client: Optional[MilvusClient] = None,
    ):
        self.collection_name = collection_name
        self.mock_collection_name = collection_name + "_mock"
        self.language_field_name = language_field_name
        self.text_field_name = text_field_name
        self.multi_analyzer_params = (
            multi_analyzer_params
            if multi_analyzer_params is not None
            else {
                "by_field": self.language_field_name,
                "analyzers": {
                    "en": {"type": "english"},
                    "zh": {"type": "chinese"},
                    "icu": {
                        "tokenizer": "icu",
                        "filter": [{"type": "stop", "stop_words": [" "]}],
                    },
                    "default": {"tokenizer": "whitespace"},
                },
                "alias": {"chinese": "zh", "eng": "en", "fr": "icu", "jp": "icu"},
            }
        )
        self.mock_multi_analyzer_params = {
            "by_field": self.language_field_name,
            "analyzers": {"default": {"tokenizer": "whitespace"}},
        }
        self.client = client
        self.collection = None
        self.mock_collection = None

    def resolve_analyzer(self, lang: str) -> str:
        """
        Return the analyzer name according to the language.
        Args:
            lang (str): Language identifier
        Returns:
            str: Analyzer name
        """
        if lang in self.multi_analyzer_params["analyzers"]:
            return lang
        if lang in self.multi_analyzer_params.get("alias", {}):
            return self.multi_analyzer_params["alias"][lang]
        return "default"

    def build_schema(self, multi_analyzer_params: dict) -> CollectionSchema:
        """
        Build a collection schema with multi-analyzer parameters.
        Args:
            multi_analyzer_params (dict): Analyzer parameters
        Returns:
            CollectionSchema: Constructed collection schema
        """
        fields = [
            FieldSchema(name="doc_id", dtype=DataType.INT64, is_primary=True),
            FieldSchema(
                name=self.language_field_name,
                dtype=DataType.VARCHAR,
                max_length=self.DEFAULT_LANG_MAX_LENGTH,
            ),
            FieldSchema(
                name=self.text_field_name,
                dtype=DataType.VARCHAR,
                max_length=self.DEFAULT_TEXT_MAX_LENGTH,
                enable_analyzer=True,
                multi_analyzer_params=multi_analyzer_params,
            ),
            FieldSchema(name="bm25_sparse_vector", dtype=DataType.SPARSE_FLOAT_VECTOR),
        ]
        schema = CollectionSchema(
            fields=fields, description="Multi-analyzer BM25 schema test"
        )
        bm25_func = Function(
            name="bm25",
            function_type=FunctionType.BM25,
            input_field_names=[self.text_field_name],
            output_field_names=["bm25_sparse_vector"],
        )
        schema.add_function(bm25_func)
        return schema

    def init_collection(self) -> None:
        """
        Initialize Milvus collections, delete if exists first.
        """
        try:
            if self.client.has_collection(self.collection_name):
                self.client.drop_collection(self.collection_name)
            if self.client.has_collection(self.mock_collection_name):
                self.client.drop_collection(self.mock_collection_name)
            self.collection = Collection(
                name=self.collection_name,
                schema=self.build_schema(self.multi_analyzer_params),
            )
            self.mock_collection = Collection(
                name=self.mock_collection_name,
                schema=self.build_schema(self.mock_multi_analyzer_params),
            )
        except Exception as e:
            logger.error(f"collection init failed: {e}")
            raise

    def get_tokens_by_analyzer(self, text: str, analyzer_params: dict) -> List[str]:
        """
        Tokenize text according to analyzer parameters.
        Args:
            text (str): Text to be tokenized
            analyzer_params (dict): Analyzer parameters
        Returns:
            List[str]: List of tokenized text
        """
        try:
            res = self.client.run_analyzer(text, analyzer_params)
            # Filter out tokens that are just whitespace
            return [token for token in res.tokens if token.strip()]
        except Exception as e:
            logger.error(f"Tokenization failed: {e}")
            return []

    def generate_test_data(
        self, num_rows: int = 3000, lang_list: Optional[List[str]] = None
    ) -> List[Dict]:
        """
        Generate test data according to the schema, row count and language list.
        Each row will contain language, article content and other fields.
        Args:
            num_rows (int): Number of data rows to generate
            lang_list (Optional[List[str]]): List of languages
        Returns:
            List[Dict]: Generated test data list
        """
        if lang_list is None:
            lang_list = ["en", "eng", "zh", "fr", "chinese", "jp", ""]
        data = []
        for i in range(num_rows):
            lang = random.choice(lang_list)
            # Generate article content according to language
            if lang in ("en", "eng"):
                content = self.fake_en.sentence()
            elif lang in ("zh", "chinese"):
                content = self.fake_zh.sentence()
            elif lang == "fr":
                content = self.fake_fr.sentence()
            elif lang == "jp":
                content = self.fake_jp.sentence()
            else:
                content = ""
            row = {
                "doc_id": i + self.DEFAULT_DOC_ID_START,
                self.language_field_name: lang,
                self.text_field_name: content,
            }
            data.append(row)
        return data

    def tokenize_data_by_multi_analyzer(
        self, data_list: List[Dict], verbose: bool = False
    ) -> List[Dict]:
        """
        Tokenize data according to multi-analyzer parameters.
        Args:
            data_list (List[Dict]): Data list
            verbose (bool): Whether to print detailed information
        Returns:
            List[Dict]: Tokenized data list
        """
        data_list_tokenized = []
        for row in data_list:
            lang = row.get(self.language_field_name, None)
            content = row.get(self.text_field_name, "")
            doc_analyzer = self.resolve_analyzer(lang)
            doc_analyzer_params = self.multi_analyzer_params["analyzers"][doc_analyzer]
            content_tokens = self.get_tokens_by_analyzer(content, doc_analyzer_params)
            tokenized_content = " ".join(content_tokens)
            data_list_tokenized.append(
                {
                    "doc_id": row.get("doc_id"),
                    self.language_field_name: lang,
                    self.text_field_name: tokenized_content,
                }
            )
        if verbose:
            original_data = pd.DataFrame(data_list)
            tokenized_data = pd.DataFrame(data_list_tokenized)
            logger.info(f"Original data:\n{original_data}")
            logger.info(f"Tokenized data:\n{tokenized_data}")
        return data_list_tokenized

    def insert_data(
        self, data: List[Dict], verbose: bool = False
    ) -> Tuple[List[Dict], List[Dict]]:
        """
        Insert test data and return original and tokenized data.
        Args:
            data (List[Dict]): Original data list
            verbose (bool): Whether to print detailed information
        Returns:
            Tuple[List[Dict], List[Dict]]: (original data, tokenized data)
        """
        try:
            self.collection.insert(data)
            self.collection.flush()
        except Exception as e:
            logger.error(f"Failed to insert original data: {e}")
            raise
        t0 = time.time()
        tokenized_data = self.tokenize_data_by_multi_analyzer(data, verbose=verbose)
        t1 = time.time()
        logger.info(f"Tokenization time: {t1 - t0}")
        try:
            self.mock_collection.insert(tokenized_data)
            self.mock_collection.flush()
        except Exception as e:
            logger.error(f"Failed to insert tokenized data: {e}")
            raise
        return data, tokenized_data

    def create_index(self) -> None:
        """
        Create BM25 index for sparse vector field.
        """
        for c in [self.collection, self.mock_collection]:
            try:
                c.create_index(
                    "bm25_sparse_vector",
                    {"index_type": "SPARSE_INVERTED_INDEX", "metric_type": "BM25"},
                )
                c.load()
            except Exception as e:
                logger.error(f"Failed to create index: {e}")
                raise

    def search(
        self, origin_query: str, tokenized_query: str, language: str, limit: int = 10
    ) -> Tuple[list, list]:
        """
        Search interface, perform BM25 search on main and mock collections respectively.
        Args:
            origin_query (str): Original query text
            tokenized_query (str): Tokenized query text
            language (str): Query language
            limit (int): Number of results to return
        Returns:
            Tuple[list, list]: (main collection results, mock collection results)
        """
        analyzer_name = self.resolve_analyzer(language)
        search_params = {"metric_type": "BM25", "analyzer_name": analyzer_name}
        logger.info(f"search_params: {search_params}")
        try:
            res = self.collection.search(
                data=[origin_query],
                anns_field="bm25_sparse_vector",
                param=search_params,
                output_fields=["doc_id"],
                limit=limit,
            )
            mock_res = self.mock_collection.search(
                data=[tokenized_query],
                anns_field="bm25_sparse_vector",
                param=search_params,
                output_fields=["doc_id"],
                limit=limit,
            )
            return res, mock_res
        except Exception as e:
            logger.error(f"Search failed: {e}")
            return [], []


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    connections.connect("default", host="10.104.25.52", port="19530")
    client = MilvusClient(uri="http://10.104.25.52:19530")
    ft = FTSMultiAnalyzerChecker(
        "test_collection", "language", "article_content", client=client
    )
    ft.init_collection()
    ft.create_index()
    language_list = ["jp", "en", "fr", "zh"]
    data = ft.generate_test_data(1000, language_list)
    _, tokenized_data = ft.insert_data(data)
    search_sample_data = random.sample(tokenized_data, 10)
    for row in search_sample_data:
        tokenized_query = row[ft.text_field_name]
        # Find the same doc_id in the original data and get the original query
        # Use pandas to find the item with matching doc_id
        # Convert data to DataFrame if it's not already
        if not isinstance(data, pd.DataFrame):
            data_df = pd.DataFrame(data)
        else:
            data_df = data
        # Filter by doc_id and get the text field value
        origin_query = data_df.loc[
            data_df["doc_id"] == row["doc_id"], ft.text_field_name
        ].iloc[0]
        logger.info(f"Query: {tokenized_query}")
        logger.info(f"Origin Query: {origin_query}")
        language = row[ft.language_field_name]
        logger.info(f"language: {language}")
        res, mock_res = ft.search(origin_query, tokenized_query, language)
        logger.info(f"Main collection search result: {res}")
        logger.info(f"Mock collection search result: {mock_res}")
        if res and mock_res:
            res_set = set([r["doc_id"] for r in res[0]])
            mock_res_set = set([r["doc_id"] for r in mock_res[0]])
            res_diff = res_set - mock_res_set
            mock_res_diff = mock_res_set - res_set
            logger.info(f"Diff: {res_diff}, {mock_res_diff}")
            if res_diff or mock_res_diff:
                logger.error(
                    f"Search results inconsistent: {res_diff}, {mock_res_diff}"
                )
                assert False
